# from tkinter import *
# import tkinter as tk
from idlelib.zoomheight import zoom_height
from tkinter import *
from tkinter import ttk
from PIL import ImageTk, Image # # pip install Pillow
import threading


import __main__ as main
import sys
import inspect
import log1 as log1

import time
import pandas as pd
import numpy as np
from random import *
import os
from datetime import datetime, time, date, timedelta

import conrad_server as conrad
import igloo_etrm as etrm
import gcp as gcp
import prepare_orders as po
import log1 as log1
from trade import *

# from constants import *

font_name = "Helvetica"
d_colours ={
    "bg":{"main_frm":"#ececee", "bid":"#77d49c", "ask":"#e0b290","tv_selection":"#00757b","id_gas":"#a2aaeb","btn":"#DCDCDC"},
    "fg":{"bid":"green","ask":"#e87400","id_gas":"#4553ba"}
}

n_hour_new_id_gas_thresh = 4
p_asset_grp_step_size = 10
mw_grp_max_sum = 100

d_sizes = {"main_frm":[1125,975],
           "fnt_lbl_version":9,
           "bd":2,
           "col_width":150,
           "col_min_width":50,
           "row_height":20}

s_version = '1.0.'
font_type_lbl_version = "italic"
d_tv_asset = {"cols":{"bid":['#0','mw_to_trade',"p_traded","p_srmc"],
                      "ask":["#0","mw_to_trade","p_srmc"]},
              "col_txt":{"bid":["Group id","Bid vol. (MW)","Traded @ (£/MWh)", "SRMC (£/MWh)"],
                         "ask":["Group id", "Offer vol. (MW)", "SRMC (£/MWh)"]}}

# update_without_clicking = True
p_inv = 0

# Boolean for Logging content
blc = True

def mf_front_end():
    # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
    global root
    update_without_clicking = True
    def change_ddl_bg(ddl_val):
        # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
        bgc = d_colours["bg"]["bid"] if ddl_val == "Bid" else d_colours["bg"]["ask"]
        ddl_gr.config(bg=bgc)
        e_p_power.config(bg=bgc)

    def submit_p_power():
        if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
        idx_grp_id = 0
        idx_p_model = 3

        selected = tv_mkt.focus()
        prod_gr = tv_mkt.item(selected, "text")
        p_model = float(tv_mkt.item(selected,'values')[idx_p_model])
        p_trader = e_p_power.get()
        tv_mkt_vals = tv_mkt.item(selected,'values')
        ts = pd.to_datetime("today")
        df_sql = pd.DataFrame([ts,prod_gr,p_trader,p_model],index=["timestamp","prod_gr","p_trader","p_model"]).T
        df_sql.rename(columns={"prod_gr":"prod_gr_fe"},inplace=True)
        gcp.write_to_postgresql(df_sql,"fe_price_input","m7")

        e_p_power.delete(0, "end")
        lbl_statusbar.config(text="New power price is submitted.", fg="blue")

    def change_ddl_gr_bg(event):
        # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
        lbl_statusbar.config(text=event)

    def update_fields():

        # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
        def group_assets(dfo):
            if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
            dfr = dfo.copy()

            # There was an issue with SRMC table on commercial server -> hence we exclude SRMC == NA values.
            dfr = dfr[~dfr["p_srmc"].isna()]

            # We fill p_traded==NA because we'll need to do some arithmetics on this column later.
            dfr["p_traded"].fillna(p_inv, inplace=True)

            df1 = dfr.loc[dfr.is_selling == False, :].sort_values(by=["sp", "p_traded"], ascending=False)
            df2 = dfr.loc[dfr.is_selling == True, :].sort_values(by=["sp", "p_srmc"])
            dfr = pd.concat([df1, df2], ignore_index=True)

            # Create a dataframe of extreme values (min SRMC for vol. to ask & max traded for vol. to bid).
            df_p_ext = pd.pivot_table(data=dfr,
                                      values=["p_srmc", "p_traded"],
                                      index="sp",
                                      aggfunc={'p_srmc': np.min, 'p_traded': np.max}).reset_index()

            # Rename columns with better labels
            df_p_ext.rename(columns={"p_srmc": "p_srmc_min", "p_traded": "p_traded_max"}, inplace=True)

            # Merge dfr with extreme prices on sp.
            dfr = pd.merge(dfr, df_p_ext, on="sp", how="left")

            # Pick price that is going to be used for grouping: p_srmc for ask-side, p_traded for bid-side.
            dfr["grp_price"] = np.where(dfr["is_selling"], dfr["p_srmc"], dfr["p_traded"])

            # Calculate the difference & between price & extreme price & divide it by input step size.
            dfr["grp_price"] = (np.where(dfr["is_selling"],
                                         dfr["p_srmc"] - dfr["p_srmc_min"],
                                         dfr["p_traded_max"] - dfr["p_traded"]) / p_asset_grp_step_size)

            # Then rounddown (floor) to get an integer
            dfr["grp_price"] = dfr["grp_price"].apply(np.floor)

            cn_piv = ["mw_to_trade", "is_selling", "sp", "grp_price"]
            df_mw_sum_piv = pd.pivot_table(
                data=dfr.loc[:, cn_piv],
                values=cn_piv[0],
                index=cn_piv[1:],
                aggfunc=np.sum).reset_index()
            df_mw_sum_piv.rename(columns={"mw_to_trade": "mw_grp_sum"}, inplace=True)
            dfr = pd.merge(dfr, df_mw_sum_piv, "left", cn_piv[1:])
            dfr["grp_is_too_large"] = dfr["mw_grp_sum"] > mw_grp_max_sum
            dfr["mw_incremental_sum"] = 0
            y = dfr[dfr["grp_is_too_large"]]

            for is_selling in y["is_selling"].unique():
                for sp in y["sp"].unique():
                    for grp in y["grp_price"].unique():
                        z = y.loc[(y.is_selling == is_selling) & (y.sp == sp) & (y.grp_price == grp), :].copy()
                        mw_sum = 0
                        for i in z.iterrows():
                            mw_sum += i[1]["mw_to_trade"]
                            dfr.loc[i[0], "mw_incremental_sum"] = mw_sum
            dfr["grp_volume"] = (dfr["mw_incremental_sum"] / mw_grp_max_sum).apply(np.floor)
            cn_grp = ["is_selling", "sp", "grp_price", "grp_volume"]
            df_grps = dfr.loc[:, cn_grp].groupby(by=cn_grp).count().reset_index()
            df_grps["shifted_is_selling"] = df_grps["is_selling"].shift(1)
            df_grps["shifted_sp"] = df_grps["sp"].shift(1)
            df_grps["same_selling"] = np.where(df_grps["shifted_is_selling"] == df_grps["is_selling"], True, False)
            df_grps["same_sp"] = np.where(df_grps["shifted_sp"] == df_grps["sp"], True, False)
            df_grps["grp"] = 1
            j = 0
            for i in range(0, df_grps.shape[0]):

                if i > 0:
                    if (df_grps.loc[i, "same_selling"]) & (df_grps.loc[i, "same_sp"]):
                        j += 1
                    else:
                        j = 1
                else:
                    j = 1
                df_grps.loc[i, "grp"] = j
            df_grps["grp"] = np.where(df_grps["is_selling"], "A", "B") + df_grps["grp"].apply(str)
            dfr = pd.merge(dfr, df_grps.loc[:, ["is_selling", "sp", "grp_price", "grp_volume", "grp"]], how="left")
            dfr = dfr.round(2)
            dfr["mw_traded"] = dfr["mw_traded"].abs()

            dfr.drop(
                labels=
                ["p_srmc_min", "p_traded_max", "grp_price", "mw_grp_sum", "grp_is_too_large", "mw_incremental_sum",
                 "grp_volume"],
                axis=1,
                inplace=True
            )
            return dfr

        def pvt_grps(dfo, at_grp, pvt_a):
            # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
            # column names

            # Common column names
            cn = ["sp", "grp", "mw_to_trade","p_srmc", "p_traded", "mw_traded"]

            # Column names for indexing pivot
            cn_pvt_idx = cn[:2] if at_grp else cn[0]

            # Column names for pivot values
            cn_pvt_val = ["mw_to_trade","mw_traded","p_srmc", "p_traded", "rev_a","rev_b"]

            # What pivot table do we want? Bid | Ask?
            df = pd.DataFrame(data=dfo.loc[dfo.is_selling == pvt_a, cn])

            if len(df.index) == 0:
                return df
            df["rev_b"] = df["mw_traded"]*df["p_traded"]
            df["rev_a"] = df["mw_to_trade"] * df["p_srmc"]

            df = pd.pivot_table(
                data=df,
                values=cn_pvt_val,
                index=cn_pvt_idx,
                aggfunc={cn_pvt_val[0]: [np.sum, np.min, np.max],
                         cn_pvt_val[1]: [np.sum, np.min, np.max],
                         cn_pvt_val[2]: [np.min, np.max],
                         cn_pvt_val[3]: [np.min, np.max],
                         cn_pvt_val[4]: np.sum,
                         cn_pvt_val[5]: np.sum}).reset_index()

            df.columns = ['_'.join(col).strip() for col in df.columns.values]

            df["p_srmc_wa"] = df["rev_a_sum"] / df["mw_to_trade_sum"]
            df["p_traded_wa"] = df["rev_b_sum"] / df["mw_traded_sum"]

            df["p_srmc"] = (
                    df["p_srmc_wa"].round(2).apply(str)
                    + " ["
                    + df["p_srmc_amin"].round(1).apply(str)
                    + "-"
                    + df["p_srmc_amax"].round(1).apply(str)
                    + "]"
            )

            df["p_traded"] = (
                    df["p_traded_wa"].round(1).apply(str)
                    + " ["
                    + df["p_traded_amin"].round(1).apply(str)
                    + "-"
                    + df["p_traded_amax"].round(1).apply(str)
                    + "]"
            )

            df["mw_traded"] = (
                    df["mw_traded_sum"].round(1).apply(str)
                    + " ["
                    + df["mw_traded_amin"].round(1).apply(str)
                    + "-"
                    + df["mw_traded_amax"].round(1).apply(str) + "]"
            )

            df["mw_to_trade"] = (
                    df["mw_to_trade_sum"].round(1).apply(str)
                    + " ["
                    + df["mw_to_trade_amin"].round(1).apply(str)
                    + "-"
                    + df["mw_to_trade_amax"].round(1).apply(str) + "]"
            )

            df.rename(columns={"sp_": "sp"}, inplace=True)
            if at_grp:
                df.rename(columns={"grp_": "grp"}, inplace=True)
            else:
                df["grp"] = np.where(pvt_a, "A0", "B0")

            df["gr_type"] = "grp" if at_grp else "sp"

            return df

        def get_grp_lbl(x):
            # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
            if x["gr_type"] == "asset":
                return x["sites"]

            elif x["gr_type"] == "grp":
                return ("HH" + x["s_sp"] + "-" + x["grp"])

            elif x["gr_type"] == "sp":
                return ("HH" + x["s_sp"] + "-" + "All")

        df_av, df_srmc = conrad.mf_conrad_server(True)

        dft, ts_trade_checked = etrm.mf_igloo_etrm()
        dfo = po.mf_get_orders(df_av, df_srmc, dft, ts_trade_checked, data_for_frontend_demo=True)
        dfo = group_assets(dfo)
        dfa_gr = pvt_grps(dfo, at_grp=True, pvt_a=True)
        dfa_sp = pvt_grps(dfo, at_grp=False, pvt_a=True)
        dfb_gr = pvt_grps(dfo, at_grp=True, pvt_a=False)
        dfb_sp = pvt_grps(dfo, at_grp=False, pvt_a=False)
        df = pd.concat([dfb_gr, dfa_gr, dfb_sp, dfa_sp], ignore_index=True)
        dfo["gr_type"] = "asset"
        df["sites"] = "NA"
        df = pd.concat([df, dfo.loc[:, ["sp", "grp", "mw_to_trade", "p_traded", "p_srmc", "gr_type", "sites"]]],
                       ignore_index=True)

        df["s_sp"] = df["sp"].apply(lambda x: str(x) if x >= 10 else "0" + str(x))

        df["prod_gr"] = df.loc[:, ["gr_type", "sites", "grp", "s_sp"]].apply(get_grp_lbl, axis=1)
        df["lvl"] = df["gr_type"].apply(lambda x: 2 if x == "asset" else 1 if x == "grp" else 0)
        df["is_selling"] = np.where(df["grp"].str[:1] == "A", True, False)
        df["int_grp"] = df["grp"].apply(lambda x: x[1:]).apply(int)
        df.sort_values(by=["sp", "int_grp", "lvl"], inplace=True)

        def get_parent_grp(x):
            # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
            if x["gr_type"]=="asset":
                return "HH" + str(x["sp"]) + "-" + str(x["grp"])

            elif x["gr_type"]=="grp":
                return "HH" + str(x["sp"]) + "-" + "All"
            else:
                return "NA"

        df["parent"] = df.loc[:,["sp","gr_type","grp"]].apply(get_parent_grp,axis=1)

        q = 'delete from m7.asset_groups;'
        gcp.query_postgresql(q, select_query=False)
        gcp.write_to_postgresql(df.loc[:,["prod_gr","parent", "sp", "gr_type", "grp", "sites",
                                          "mw_traded","mw_to_trade","p_traded","p_srmc",
                                          "lvl", "is_selling"]],
                                "asset_groups", "m7")

        def get_p_model_p_trader(df):

            # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])

            dfr = df.copy()

            dfr2 = (dfr.loc[dfr.gr_type=="asset",["sp","is_selling","prod_gr","parent","p_traded","p_srmc"]]).copy()

            dfr2.rename(columns={"prod_gr":"sites"},inplace=True)
            dfr2["p_srmc"]= dfr2["p_srmc"].astype("float")

            # df1 = df_fe.loc[:,["prod_gr_fe","p_trader","p_model"]]
            dfr2 = calc_p_asset(dfr2,asset_strategy, return_just_margin=True)
            dfr2["p_model"] = np.where(dfr2["is_selling"], dfr2[cl_p_asset_limit["o"]], dfr2[cl_p_asset_limit["b"]]).round(2)
            dfr2.rename(columns={"parent":"prod_gr"},inplace=True)
            dfr2.drop_duplicates(subset=["prod_gr","p_trader","p_model"], inplace=True)
            dfr=pd.merge(dfr, dfr2.loc[:,["prod_gr","p_model","p_trader"]], on="prod_gr", how="left")

            return dfr

        df= get_p_model_p_trader(df)

        for record in tv_mkt.get_children():
            tv_mkt.delete(record)

        tv_mkt.tag_configure("B", background=d_colours["bg"]["bid"])
        tv_mkt.tag_configure("A", background=d_colours["bg"]["ask"])

        # df_tv_mkt = df.sort_values(["int_grp","is_selling","sp"]).copy()
        df_tv_mkt = df.sort_values(["sp","is_selling","grp"]).copy()
        df_tv_mkt = df_tv_mkt.loc[df_tv_mkt["gr_type"]=="grp",["prod_gr","mw_to_trade","p_traded","p_srmc","p_model","p_trader"]]
        df_tv_mkt["p_trader"] = df_tv_mkt["p_trader"].astype(str)
        df_tv_mkt["p_trader"] = np.where(df_tv_mkt["p_trader"]=="nan","",df_tv_mkt["p_trader"])
        # Fill market tree-view
        for index, row in df_tv_mkt.iterrows():
            lst_cols = []
            for j, col in enumerate(row):
                (lst_cols.append(col) if j > 0 else None)
            t2 = tuple(lst_cols)
            s_tag = row["prod_gr"][5]
            tv_mkt.insert(parent='', index="end", iid=index, text=row["prod_gr"], values=t2, tags = (s_tag))

        for record in tv_asset_b.get_children():
            tv_asset_b.delete(record)
        # df.sort_values(by=["sp", "int_grp", "lvl"], inplace=True)
        df_tv_asset_b = df.loc[df["is_selling"]==False,["prod_gr","mw_to_trade","p_traded","p_srmc","lvl"]]
        for index, row in df_tv_asset_b.iterrows():

            if row["lvl"] == 0:
                str_parent = ""
                level_0_id = index

            elif row["lvl"] == 1:
                str_parent = str(level_0_id)
                level_1_id = index

            else:
                str_parent = str(level_1_id)

            t2 = tuple([row["mw_to_trade"], row["p_traded"], row["p_srmc"]])
            s_tag = f"level_{row['lvl']}"
            tv_asset_b.insert(parent=str_parent, index="end", iid=index, text=row["prod_gr"], values=t2, tags=("asset"))

        tv_asset_b.tag_configure("asset", background=d_colours["bg"]["main_frm"])


        for record in tv_asset_a.get_children():
            tv_asset_a.delete(record)

        df_tv_asset_a = df.loc[df["is_selling"]==True,["prod_gr","mw_to_trade","p_srmc","lvl"]]
        for index, row in df_tv_asset_a.iterrows():

            if row["lvl"] == 0:
                str_parent = ""
                level_0_id = index

            elif row["lvl"] == 1:
                str_parent = str(level_0_id)
                level_1_id = index

            else:
                str_parent = str(level_1_id)

            t2 = tuple([row["mw_to_trade"], row["p_srmc"]])
            s_tag = f"level_{row['lvl']}"
            tv_asset_a.insert(parent=str_parent, index="end", iid=index, text=row["prod_gr"], values=t2, tags=("asset"))

        tv_asset_a.tag_configure("asset", background=d_colours["bg"]["main_frm"])
        q = 'select * from m7.id_gas where inserted_time = (select max(inserted_time) from m7.id_gas);'
        df_gas = gcp.query_postgresql(q)
        ts =df_gas.loc[0,"inserted_time"].strftime("%Y-%m-%d %H:%M")
        p_id_gas = str(df_gas.loc[0,"p_id_gas"])

        ts_most_recent_id_gas = pd.to_datetime(df_gas.loc[0,"inserted_time"])
        new_id_gas_is_needed = pd.to_datetime("today") - timedelta(hours=n_hour_new_id_gas_thresh) > ts_most_recent_id_gas

        lbl = p_id_gas + " p/th"  + " updated @\n" + ts
        bgc="red" if new_id_gas_is_needed else "black"
        lbl_id_gas_latest_val.config(text=lbl, font = "Helvetica 12",fg=bgc)

        ts_str = pd.to_datetime("today").strftime("%H:%M:%S")
        lbl_statusbar.config(text=f"Fields are updated @ {ts_str}", fg="blue")

    def start_autotrader():

        # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
        btn_start.config(state=DISABLED)

        def clear_active_orders():

            # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
            token = json.loads((requests.post(base_url + '/oauth/token', data=cred_m7)).text)["Token"]

            k = get_orders(token)
            cl_orders = [
                "order_id", "igloo_product", "trader", "site_name", "is_selling", "p_order",
                "mw_order", "created_time", "amended_time", "is_active"
            ]
            orders = pd.DataFrame(columns=cl_orders)
            for x in k:
                v = [x["Id"], x["Product"], x["Trader"], x["Strategy"], x["Side"],
                     x["Price"], x["Volume"], x["CreateTime"], x["ModifyTime"], x["Active"]]
                orders.loc[orders.shape[0], :] = v
            autotrader_active_order_ids = orders.loc[
                (orders.trader == "smoody") & (orders.is_active == True), "order_id"]

            for id in autotrader_active_order_ids.iteritems():
                cancel_order(id[1], token)

        def mf_main():

            # if blc: log1.log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])


            autotrader_status = True
            q = f"update m7.autotrader_status set autotrader_status = '{autotrader_status}' where uid = 0;"
            _ = gcp.query_postgresql(q, select_query=False)

            df_av, df_srmc = conrad.mf_conrad_server()

            while True:
                if autotrader_status == False:
                    stop_event.set()

                if not stop_event.isSet():
                    dft, ts_trade_checked = etrm.mf_igloo_etrm()
                    dfo = po.mf_get_orders(df_av, df_srmc, dft, ts_trade_checked, data_for_frontend_demo=False)
                    autotrader_status = mf_trade(dfo)

        lbl_statusbar.config(text="Auto-trader started.", fg="blue")

        stop_event = threading.Event()
        stop_event.clear()

        start_event = threading.Thread(target= mf_main)
        start_event.daemon = True
        start_event.start()

    def stop_autotrader():

        q = "update m7.autotrader_status set autotrader_status = 'False' where uid = 0;"
        _ = gcp.query_postgresql(q,select_query=False)

        lbl_statusbar.config(text="Auto-trader stopped.", fg="blue")

        token = json.loads((requests.post(base_url + '/oauth/token', data=cred_m7)).text)["Token"]

        k = get_orders(token)
        cl_orders = [
            "order_id", "igloo_product", "trader", "site_name", "is_selling", "p_order",
            "mw_order", "created_time", "amended_time", "is_active"
        ]
        orders = pd.DataFrame(columns=cl_orders)
        for x in k:
            v = [x["Id"], x["Product"], x["Trader"], x["Strategy"], x["Side"],
                 x["Price"], x["Volume"], x["CreateTime"], x["ModifyTime"], x["Active"]]
            orders.loc[orders.shape[0], :] = v
        autotrader_active_order_ids = orders.loc[(orders.trader=="smoody")&(orders.is_active==True),"order_id"]

        for id in autotrader_active_order_ids.iteritems():
            cancel_order(id[1],token)

        root.quit()

        # python = sys.executable
        # os.execl(python, python, *sys.argv)

    def submit_id_gas():
        p_id_gas = float(e_id_gas.get())
        e_id_gas.delete(0, "end")
        now = pd.to_datetime("today")
        df = pd.DataFrame(np.array([now, p_id_gas]),index=["inserted_time","p_id_gas"]).T
        gcp.write_to_postgresql(df, tbl_name="id_gas", schema_name="m7")
        lbl_id_gas_latest_val.config(text="", font = "Helvetica 12",fg=bgc)
        lbl_statusbar.config(text="New ID gas price is submitted.", fg="blue")

    def select_tv_mkt_content(e):

        idx_grp_id = 0
        idx_p_model = 3
        idx_p_trader = 4

        e_p_power.delete(0, END)
        selected = tv_mkt.focus()
        p_model = tv_mkt.item(selected,'values')[idx_p_model]
        p_trader = tv_mkt.item(selected, 'values')[idx_p_trader]
        try:
            p_trader_is_nan = math.isnan(float(p_trader))
        except:
            p_trader_is_nan=True

        grp = tv_mkt.item(selected,"text")

        is_selling = True if grp[5]=="A" else False
        bgc = d_colours["bg"]["ask"] if is_selling else d_colours["bg"]["bid"]
        e_p_power.config(bg=bgc)
        if p_trader_is_nan:
            e_p_power.insert(0,str(p_model))
        else:
            e_p_power.insert(0, str(p_trader))
        # (parent='', index="end", iid=index, text=row["prod_gr"], values=t2, tags = (s_tag))

    b_input=True
    if b_input:
        path = r'C:\Users\elfa3-extra\PycharmProjects\igloo_comtrader\venv'
        root = Tk()
        root.title("VISION: Auto-trading Platform")
        root.configure(background=d_colours["bg"]["main_frm"])
        root.iconbitmap(path+"//conrad visuals//conrad_icon.ico")
        root.geometry(f"{d_sizes['main_frm'][0]}x{d_sizes['main_frm'][1]}")
        root.resizable(width=False, height=False)

        # img_conrad_logo = ImageTk.PhotoImage(Image.open("conrad_logo_final.png"))
        img_header = ImageTk.PhotoImage(Image.open(path+"//conrad visuals//vision_header_new.png"))

        # img_header_2 = ImageTk.PhotoImage(Image.open("vision_header.png"))
        # frm_main = LabelFrame(root, text="Main Frame", padx=0, pady=0, bg=bgc, bd = 2)
        # frm_title = LabelFrame(frm_main, text = "Title frame", padx=0, pady=0, bg=bgc, bd=2)
        # frm_treeview = LabelFrame(frm_main, text="Treeview Frame", padx=0, pady=0, bg=bgc, bd=2)
        # frm_p_submission_ddl = LabelFrame(frm_main, text="Submit Prices", padx=0, pady=0)

    b_frm = True
    if b_frm:

        bgc = d_colours["bg"]["main_frm"]
        bds = d_sizes["bd"]

        frm_main = Frame(root, bd = bds)
        frm_title = Frame(frm_main,bg=bgc, bd=bds)

        frm_tv_asset = LabelFrame(frm_main, text= "Asset treeview (positions & prices)",
                                  font = f"{font_name} 12", bg=bgc, bd=0, fg="blue")
        frm_tv_asset_b = LabelFrame(frm_tv_asset, text = "Bid-side    ", font="Helvetica 11 italic",
                                    bg=bgc, bd=0,labelanchor="ne",fg=d_colours["fg"]["bid"])
        lbl_empty_between_tv_assets = Label(frm_tv_asset,text="    ")
        frm_tv_asset_a = LabelFrame(frm_tv_asset, text = "Offer-side    ", font="Helvetica 11 italic",
                                    bg=bgc, bd=0,labelanchor="ne", fg=d_colours["fg"]["ask"])
        frm_tv_mkt = LabelFrame(frm_main, bg=bgc, bd=0, text= "Market treeview (pricing on the market) \n",
                                font = f"{font_name} 12",fg="blue")

        # frm_tv_mkt = LabelFrame(frm_tv_mkt, text="Bid-side",bd=0,labelanchor="ne",fg=d_colours["fg"]["bid"],
        #                           font="Helvetica 11 italic")
        # frm_tv_mkt_a = LabelFrame(frm_tv_mkt, text="Ask-side",bd=0,labelanchor="ne",fg=d_colours["fg"]["ask"],
        #                           font="Helvetica 11 italic")
        lbl_empty_between_tv_mkt_p_submission_form = Label(frm_tv_mkt, text=" ")

        frm_p_submission = LabelFrame(frm_main, text="Price submission",
                                      font = f"{font_name} 12",fg="blue", bg=bgc,bd=0)
        lbl_empty_between_price_submission_btns = Label(frm_main,text = "  ")
        frm_p_power_submission = LabelFrame(frm_p_submission, text="Power", font="Helvetica 11 italic",
                                            bg=bgc, bd=bds,padx=10,pady=10, fg="blue",labelanchor="ne")
        lbl_empty_between_gas_power_submission = Label(frm_p_submission,text = "   ")
        frm_p_gas_submission = LabelFrame(frm_p_submission, text="Gas", font="Helvetica 11 italic", bg=bgc,
                                          bd=bds,labelanchor="ne", padx=10, pady=10,fg="red")
        frm_lbl_p_submission = Frame(frm_main, bg =bgc, bd=bds)
        frm_e_p_submission = Frame(frm_main,bg =bgc, bd=bds)
        frm_btn_p_submission = Frame(frm_main, bg =bgc, bd=bds)
        frm_btn_main_function = Frame(frm_main, bg=bgc, bd=bds)
        frm_statusbar = Frame(frm_main, bg=bgc, bd=bds)

    b_title = True
    if b_title:
        lbl_vision = Label(frm_title, image= img_header)
        lbl_empty_below_title_form = Label(frm_title,text = " ")

    b_tvs = True
    if b_tvs:

        # Define style & theme for tree-views.
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview",
                        background=d_colours["bg"]["main_frm"],
                        foreground="blue",
                        rowheight=d_sizes["row_height"],
                        fieldbackground=d_colours["bg"]["main_frm"])

        style.map("Treeview", background=[("selected", d_colours["bg"]["tv_selection"])])

        ########################################################################################################################
        ########################################################################################################################
        ########################################################################################################################
        ########################################################################################################################

        global tv_asset_b, tv_asset_a
        # Get a scroll-bar on asset treeview
        sb_tv_asset_b = Scrollbar(frm_tv_asset_b)
        tv_asset_b = ttk.Treeview(frm_tv_asset_b, yscrollcommand=sb_tv_asset_b.set)
        sb_tv_asset_b.pack(side=RIGHT, fill=Y)
        sb_tv_asset_b.config(command=tv_asset_b.yview)

        # Read in internal column column names
        t = tuple(d_tv_asset["cols"]["bid"])

        # Read in external column names (what you'll see on the screen)
        lst = d_tv_asset["col_txt"]["bid"]

        # Skip the shadow column
        tv_asset_b["columns"] = t[1:]

        # Read in column width, and column min. width
        cw = d_sizes["col_width"]
        cmw = d_sizes["col_min_width"]

        # Iterate over all columns and its headings
        for i, v in enumerate(t):
            tv_asset_b.column(t[i], width=cw, minwidth=cmw, anchor=W)
            tv_asset_b.heading(t[i], text=lst[i], anchor=W)

        # Add Data
        path = r'C:\Users\elfa3-extra\PycharmProjects\igloo_comtrader\venv\default values\tv_asset.csv'
        df = pd.read_csv(path)
        for index, row in df.iterrows():

            if row["level"] == 0:
                str_parent = ""
                level_0_id = index

            elif row["level"] == 1:
                str_parent = str(level_0_id)
                level_1_id = index

            else:
                str_parent = str(level_1_id)

            t2 = (row[t[1]],row[t[2]],row[t[3]])
            s_tag = f"level_{row['level']}"
            tv_asset_b.insert(parent=str_parent, index="end", iid=index, text=row["prod_gr"], values=t2, tags=("asset"))

        tv_asset_b.tag_configure("asset", background=d_colours["bg"]["main_frm"])

        ########################################################################################################################
        ########################################################################################################################
        ########################################################################################################################
        ########################################################################################################################

        # Get a scroll-bar on asset treeview
        sb_tv_asset_a = Scrollbar(frm_tv_asset_a)
        tv_asset_a = ttk.Treeview(frm_tv_asset_a, yscrollcommand=sb_tv_asset_a.set)
        sb_tv_asset_a.pack(side=RIGHT, fill=Y)
        sb_tv_asset_a.config(command=tv_asset_a.yview)

        # Read in internal column column names
        t = tuple(d_tv_asset["cols"]["ask"])

        # Read in external column names (what you'll see on the screen)
        lst = d_tv_asset["col_txt"]["ask"]

        # Skip the shadow column
        tv_asset_a["columns"] = t[1:]

        # Read in column width, and column min. width
        cw = d_sizes["col_width"]
        cmw = d_sizes["col_min_width"]

        # Iterate over all columns and its headings
        for i, v in enumerate(t):
            tv_asset_a.column(t[i], width=cw, minwidth=cmw, anchor=W)
            tv_asset_a.heading(t[i], text=lst[i], anchor=W)

        df = pd.read_csv(path)
        for index, row in df.iterrows():

            if row["level"] == 0:
                str_parent = ""
                level_0_id = index

            elif row["level"] == 1:
                str_parent = str(level_0_id)
                level_1_id = index

            else:
                str_parent = str(level_1_id)

            t2 = (row[t[1]],row[t[2]])
            s_tag = f"level_{row['level']}"
            tv_asset_a.insert(parent=str_parent, index="end", iid=index, text=row["prod_gr"], values=t2, tags=("asset"))

        tv_asset_a.tag_configure("asset", background=d_colours["bg"]["main_frm"])


        ########################################################################################################################
        ########################################################################################################################
        ########################################################################################################################
        ########################################################################################################################
        lbl_empty_between_tv_asset_b_tv_mkt = Label(frm_tv_asset_a,text=" ")
        lbl_empty_between_tv_asset_a_tv_mkt = Label(frm_tv_asset_b, text=" ")
        global tv_mkt
        # Get a scroll-bar on asset treeview
        sb_tv_mkt = Scrollbar(frm_tv_mkt)
        tv_mkt = ttk.Treeview(frm_tv_mkt, height=10, yscrollcommand=sb_tv_mkt.set)
        tv_mkt.bind("<Double-1>", select_tv_mkt_content)
        sb_tv_mkt.pack(side=RIGHT, fill=Y)
        sb_tv_mkt.config(command=tv_mkt.yview)

        t = tuple(["#0","mw_to_trade","p_traded","p_srmc","p_model","p_trader"])
        lst = ["Group ID","Vol. to trade (MW)","Price traded (£/MWh)","SRMC (£/MWh)","Model margin (£/MWh)", "Trader margin (£/MWh)"]

        tv_mkt["columns"] = t[1:]

        cw = int((len(d_tv_asset["cols"]["bid"])+len(d_tv_asset["cols"]["ask"]))*d_sizes["col_width"]/len(lst))+6
        for i,v in enumerate(t):
            tv_mkt.column(t[i],width=cw,minwidth= cmw, anchor=W)
            tv_mkt.heading(t[i], text=lst[i], anchor=W)

        tv_mkt.tag_configure("B", background=d_colours["bg"]["bid"])
        tv_mkt.tag_configure("A", background=d_colours["bg"]["ask"])

        # Add Data
        path = r'C:\Users\elfa3-extra\PycharmProjects\igloo_comtrader\venv\default values\tv_mkt.csv'
        df = pd.read_csv(path)

        for index, row in df.iterrows():
            lst_cols = []
            for j, col in enumerate(row):
                (lst_cols.append(col) if j > 0 else None)
            t2 = tuple(lst_cols)
            s_tag = row["prod_gr"][5]
            tv_mkt.insert(parent='', index="end", iid=index, text=row["prod_gr"], values=t2, tags = (s_tag))

    b_p_submission = True
    if b_p_submission:
        global e_p_power
        # global var_ddl_gr
        # global ddl_gr

        # var_ddl_gr = StringVar()
        # var_ddl_gr.set("HH00-A0")
        lbl_price_entry = Label(frm_p_power_submission,text="Enter margin relative to SRMC (£/MWh): ", font= f"{font_name} 12", justify=LEFT)

        e_p_power = Entry(frm_p_power_submission, width=8, font= f"{font_name} 16")
        btn_p_submit_power = Button(frm_p_power_submission,
                                    bd=2, bg=d_colours["bg"]["btn"],
                                    text="Submit", font="Helvetica 12",
                                    command=submit_p_power)
        # btn_cancel = Button(frm_p_power_submission, bd=2, bg=d_colours["bg"]["btn"],
        #                           text="Cancel", font=f"{font_name} 12")

    b_gas=True
    if b_gas:
        global e_id_gas, lbl_id_gas_latest_val
        lbl_id_gas = Label(frm_p_gas_submission, text = "ID Gas (p/th)", font=f"{font_name} 12", justify= LEFT)
        e_id_gas = Entry(frm_p_gas_submission, width=6,font = f"{font_name} 16")
        lbl_id_gas_latest_val = Label(frm_p_gas_submission)

    btns=True
    if btns:
        btn_submit_id_gas = Button(frm_p_gas_submission, width=6, bd=2, text="Submit",height=1,
                                   bg=d_colours["bg"]["btn"], font = "Helvetica 12",
                                   command=submit_id_gas)

        # btn_check_status = Button(frm_btn_main_function,bg=d_colours["bg"]["btn"], width=20,bd=2,
        #                           text="Check status", font=f"{font_name} 12",
        #                           command=check_status)

        btn_update = Button(frm_btn_main_function,bg=d_colours["bg"]["btn"], width=20, bd=2, text="Update fields",
                            font=f"{font_name} 12",
                            command=update_fields)
        global btn_start
        btn_start = Button(frm_btn_main_function,bg=d_colours["bg"]["btn"], width=20, bd=2, text="Start autotrader",
                           font=f"{font_name} 12",
                           command=start_autotrader)


        btn_stop = Button(frm_btn_main_function,bg=d_colours["bg"]["btn"], width=20, bd=2, text="Stop autotrader",
                          font=f"{font_name} 12",
                          command=stop_autotrader)

        global lbl_statusbar
        lbl_statusbar = Label(frm_statusbar,text="Fields are updated. Start the auto-trader. ", fg="blue", bd=1, relief=SUNKEN,
                              font= f"{font_name} 14",pady=10)

    b_pack = True
    if b_pack:
        frm_main.pack()
        frm_title.pack()
        frm_tv_asset.pack()
        frm_tv_asset_b.pack(side=LEFT)
        lbl_empty_between_tv_assets.pack(side=LEFT)
        frm_tv_asset_a.pack(side=LEFT)
        frm_tv_mkt.pack()
        frm_p_submission.pack()
        frm_p_power_submission.pack(side=LEFT)
        lbl_empty_between_gas_power_submission.pack(side=LEFT)
        frm_p_gas_submission.pack(side=LEFT)

        frm_lbl_p_submission.pack()
        frm_e_p_submission.pack()
        frm_btn_p_submission.pack()

        frm_tv_mkt.pack()
        # lbl_empty_between_tv_mkt_bid_tv_mkt_ask.pack()
        # frm_tv_mkt_a.pack()

        tv_asset_b.pack()
        tv_asset_a.pack()
        lbl_empty_between_tv_asset_b_tv_mkt.pack()
        lbl_empty_between_tv_asset_a_tv_mkt.pack()
        tv_mkt.pack()
        # tv_mkt_ask.pack()
        lbl_empty_between_tv_mkt_p_submission_form.pack()
        lbl_vision.pack()
        lbl_empty_below_title_form.pack()
        # ddl_gr.pack(side=LEFT)

        lbl_price_entry.pack(side=LEFT)
        e_p_power.pack(side=LEFT,pady=5,padx=5)
        btn_p_submit_power.pack(side=LEFT)
        # btn_cancel.pack(side=LEFT, padx=10)

        lbl_id_gas.pack(side=LEFT)
        e_id_gas.pack(side=LEFT, pady=5,padx=5)
        btn_submit_id_gas.pack(side=LEFT)
        lbl_id_gas_latest_val.pack(side=LEFT)
        lbl_empty_between_price_submission_btns.pack()
        frm_btn_main_function.pack()
        # btn_check_status.pack(side=LEFT,padx=5)
        btn_update.pack(side=LEFT,padx=5)
        btn_start.pack(side=LEFT,padx=5)
        btn_stop.pack(side=LEFT,padx=5)
        # lbl_footer.pack()
        frm_statusbar.pack(side=RIGHT,pady=10)
        lbl_statusbar.pack()

    if update_without_clicking:
        update_fields()
        update_without_clicking=False

    root.mainloop()

def use_config():
    def sth():
        lbl.config(text="this is new text")
        root.config(bg="blue")

    root = Tk()
    root.title("Using .config")
    root.geometry('400x400')
    global lbl
    lbl = Label(root,text="this is text", font = "Helvetica 12 bold")
    lbl.pack(pady=10)
    btn=Button(root,text="click me",command=sth)
    btn.pack()
    root.mainloop()

def update_ddl():

    root = tk.Tk()
    choices = ('network one', 'network two', 'network three')
    var = tk.StringVar(root)

    def refresh():
        # Reset var and delete all old options
        var.set('')
        network_select['menu'].delete(0, 'end')

        # Insert list of new options (tk._setit hooks them up to var)
        new_choices = ('one', 'two', 'three')
        for choice in new_choices:
            network_select['menu'].add_command(label=choice, command=tk._setit(var, choice))

    network_select = tk.OptionMenu(root, var, *choices)
    network_select.grid()

    # I made this quick refresh button to demonstrate
    tk.Button(root, text='Refresh', command=refresh).grid()
    root.mainloop()


if __name__ == "__main__":
    mf_front_end()