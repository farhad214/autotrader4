import conrad_server as conrad
import igloo_m7 as m7
import igloo_etrm as etrm
import gcp as gcp
import itertools
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import logging

# Boolean for Logging content
blc = True
# from constants import *
#######################################################################################################################
################################        MODEL INPUT & ASSUMPTION            ###########################################
#######################################################################################################################

trade_selected_sites_only = True    # Trade only sites in tradable_sites list.
tradable_sites = ["LANGE", "THEVE","SANFT","MELAM"]

# tradable_sites = [
#     'ALBON','ALCOA','AMPLL','ARNTT','BARLL','BENET','BISAM','CAEON','CHAEY','DOWII','DOWRM','FENON','FLINT','GOONE',
#     'HAWEN','HEYGE','HUTTE','LANGE','LESAY','LETTH','LOWON','MATNG','MELAM','MIDCH','MOOVE','PLYAM','PLYCK','PLYTH',
#     'PURET','RAKNE','REDAR','REDDA','REDDB','SANFT','SITNE','SUDOW','THEVE','TRARK','TWIKS','WATNE','WREAM'
# ]

# With this, the auto-trader will only trade the most competitive asset: one with lowest p_srmc, or highest p_traded.
# trade_one_order_per_product = True

trade_shift_hours_only = False       # Trade only shift hours; specified in sp_desk.
sp_desk = {"wd": {"start": 13, "end": 45}, "we": {"start": 19, "end": 43}}

bb_out_of_must_run_only = True   # Don't buy-back within must-run; specified in sp_must_run & m_must_run.]
sp_must_run = [33, 34, 35, 36, 37, 38]
m_must_run = [1, 2, 11, 12]

trade_hh_only = True                    # Trade only HH products.
assume_all_sites_nbm = False            # Assume all sites are NBM.

bm_sites = [
    'ALCOA','BISAM', 'CHAEY', 'DOWRM','GOONE', 'LESAY', 'LETTH', 'MIDCH','MOOVE',
    'RAKNE','REDAR','REDDA', 'REDDB', 'SUDOW', 'THEVE', 'TRARK' 'WATNE'
]

sites = [
    'ALBON','ALCOA','AMPLL','ARNTT','BARLL','BENET','BISAM','CAEON','CHAEY','DOWII','DOWRM','FENON','FLINT','GOONE',
    'HAWEN','HEYGE','HUTTE','LANGE','LESAY','LETTH','LOWON','MATNG','MELAM','MIDCH','MOOVE','PLYAM','PLYCK','PLYTH',
    'PURET','RAKNE','REDAR','REDDA','REDDB','SANFT','SITNE','SUDOW','THEVE','TRARK','TWIKS','WATNE','WREAM'
]

#bm_sites = ["ALBON","ALCOA","AMPLL","BARLL","BISAM","CALNE","CHAEY","DOWL",]
# nbm_sites = []
t_gc_market = 15                        # minutes before market gate closure.
t_gc_bm = 60                            # minutes before BM gate closure.
t_headroom = 3                          # assumed headroom time for processing.
del_cols = True                         # Delete most of dfo's columns

# Day abbreviations for day of the week function.
day_abb = {0:"MON", 1:"TUE", 2:"WED", 3:"THU", 4:"FRI", 5:"SAT", 6:"SUN"}

# Product horizon: how many products ahead of the first product out of gate closure, can the auto-trader trade at?
# If you do not want to impose any such limitation n_prod_horizon = 48 will mean no restrictions.
n_prod_horizon = 3

# Apply assumption; only initial testing phase.
all_sites_are_hawen = False              # In case HAWEN is unavailable; to have some sites to trade with.
is_mw_av_min = False                     # Assume minimum volume as available MW.
is_mw_to_trade_min = False               # Convert all mw_to_sell to mw_vol_to_sell.
mw_to_trade_min = 0.1                   # Minimum MW to trade
using_temp_solution = True              # Igloo will provide a solution that we will be able differentiate between
                                        # orders. Until then, we'll use the temporary solution.
archive_orders = True
conrad_server_data_feed_is_automated = False
def get_unique_site_names(df_av, df_srmc):
    """
        This function returns the unique site names.
    """
    # Get sorted, unique site names (sn)
    sn = np.concatenate((df_av["SiteID"].unique(),df_srmc["siteid"].unique()), axis=None)
    sn = np.sort(np.unique(sn, axis=None))
    logging.info("unique sites-process")

    return sn

def init_dfo(df_av, df_srmc, site_names):
    """
        This function initiates all possible combinations of sites, sp, buy|sell in a dataframe dfo.
        Over the following functions, we're going to filter the content of this dataframe to those
        orders that we can|want to offer/bid on the market.
    """
    # log_content(inspect.stack()[0][3], (main.__file__).split("/")[-1])
    cn_index = ["date", "sites", "st", "is_selling", "timestamp", "sp", "igloo_product"]

    # Get today's normalised date
    today = pd.to_datetime("today").normalize()

    ts = pd.date_range(today, periods=48, freq="30Min")

    # Create an integer array of 0 & 1, representing buying & selling, respectively.
    is_selling = np.arange(0,2)

    # Do all combinations of sites & sp
    dfr = pd.DataFrame(itertools.product(site_names, is_selling, ts), columns=["sites", "is_selling", "timestamp"])
    dfr["st"] = dfr["sites"].apply(lambda x: "bm" if x in bm_sites else "nbm")

    # Add the date column
    dfr["date"] = pd.to_datetime("today").normalize()
    dfr["sp"] = dfr["timestamp"].dt.hour*2+dfr["timestamp"].apply(lambda x: 2 if x.minute>=30 else 1)

    # This section we add the igloo product string.
    if trade_hh_only:
        dfr["day_abb"] = (dfr["timestamp"].dt.dayofweek).apply(lambda x: day_abb[x])
        dfr["prefix"]= "UKP HH" + dfr["sp"].apply(str)
        dfr["igloo_product"] = dfr["prefix"]+" "+dfr["day_abb"]
    else:
        print("At the time being, we're only trading HH products. The methodology will need to be changed if we"
              "want to to apply strategy to 2H & 4H products as well.")

    # Rearrange columns & return.
    logging.info("initiate dfo-process")
    return dfr[cn_index]

def format_dfo(dfi, df_trades, df_av, df_srmc):
    """
        This function formats dfo.
        a. It merges key columns from df_trades, df_av, and df_srmc with the template dfo.
        b. It makes all columns follow a certain naming convention.
        c. It calculated mw_traded as mw; where it is stored as mwh in igloo etrm & fill nan values with 0.

    """
    cl_original = ["date","sites","st","is_selling","timestamp",
                   "sp","igloo_product","Volume","VWAP","MWExport","SRMC","ts_trades_checked"]
    cl_uniform = ["date","sites","st","is_selling","timestamp",
                  "sp","igloo_product","mw_traded","p_traded","mw_av","p_srmc","ts_trades_checked"]

    # Process the input dataframes so that merging columns wouldn't be a nightmare.
    dfr = dfi.copy()
    df_trades_int = df_trades.copy()
    df_av_int = df_av.copy()
    df_srmc_int = df_srmc.copy()

    # Currently, the data feeds that the auto-trader is pulling from conrad server are input manually on daily basis.
    # There are days that the input is not done which comprimises the whole auto-trader calculations. To circumvent,
    # this issue we're using a variable called n_o_dt which is the number of offsets in dates. Basically, if n_o_dt = 3,
    # then the code will check today's availability, SRMC, and gas data -> if not available it will check yesterday's,
    # if not available the day before that.
    # With this logic in place, we can not MERGE any data coming from conrad server with today's trades, if n_o_dt > 1,
    # and it happens to be the day that SRMC or AV are not input for today. Therefore, if
    # conrad_server_data_feed_is_automated == False => MERGE excluding the date column (which would assume AV/SRMC of
    # the first available historic date. If conrad_server_data_feed_is_automated == True, we can continue as business
    # as usual.

    if conrad_server_data_feed_is_automated:
        clm = ["date", "sites", "sp"]
        df_trades_int.rename(
            columns={"ScheduleDate":clm[0],"Asset":clm[1],"SettlementPeriod":clm[2]}, inplace=True)
        df_av_int.rename(
            columns={"SettlementDate":clm[0], "SiteID":clm[1], "SettlementPeriod":clm[2]},inplace=True)
        df_srmc_int.rename(
            columns={"settlementdate":clm[0], "siteid":clm[1], "settlementperiod":clm[2]}, inplace= True)

        # Merge dfr with input dataframes based on column labels of merging (clm).
        dfr = dfr.merge(df_trades_int, how="left", on=clm)
        dfr = dfr.merge(df_av_int, how="left", on =clm)
        dfr = dfr.merge(df_srmc_int, how="left",on = clm)

    else:
        clm = ["sites", "sp"]
        df_trades_int.rename(
            columns={"ScheduleDate":"date","Asset":clm[0],"SettlementPeriod":clm[1]}, inplace=True)
        df_av_int.rename(
            columns={"SettlementDate":"date","SiteID":clm[0], "SettlementPeriod":clm[1]},inplace=True)
        df_srmc_int.rename(
            columns={"settlementdate":"date","siteid":clm[0], "settlementperiod":clm[1]}, inplace= True)

        # Merge dfr with input dataframes based on column labels of merging (clm).
        dfr = dfr.merge(df_trades_int, how="left", on= ["date"]+clm)
        dfr = dfr.merge(df_av_int, how="left", on =clm)
        dfr = dfr.merge(df_srmc_int, how="left",on = clm)

    # Select the columns that we're interested in (cl_original) and change them to a uniform format (cl_uniform).
    dfr = dfr.loc[:,cl_original]
    dfr.columns = cl_uniform

    # Apply mw_av_min assumption for testing purposes
    if is_mw_av_min:
        dfr["mw_av"] = mw_to_trade_min

    # Convert mw_traded from original value (mwh_traded) to mw_traded.
    dfr["mw_traded"]=dfr["mw_traded"]*2
    dfr["mw_traded"].fillna(0,inplace=True)

    logging.info("format dfo-process")
    return dfr

def flag_can_bb(dfi, now):
    """
        This function flags if an order can be bought-back or not.
        The main input to this function is if we can bb during must-run period or not.
        Must-run period: [OCT-FEB];[16:00-19:00)
    """
    dfr = dfi.copy()

    if bb_out_of_must_run_only:
        def can_bb(x):
            """
                This function determines if a given combination of is_selling & sp is allowed to be bought back.
            """
            return False if (x["sp"] in sp_must_run) & (~x["is_selling"]) else True

        dow = now.weekday()
        day_type = "wd" if dow in [0, 1, 2, 3, 4] else "we"
        m = now.month
        if (day_type == "wd") & (m in m_must_run):
            dfr["f_can_bb"] = dfr.loc[:,["is_selling","sp"]].apply(can_bb, axis=1)
        else:
            dfr["f_can_bb"] = True
    else:
        dfr["f_can_bb"] = True

    logging.info("flg bb-process")

    return dfr["f_can_bb"]

def flag_for_shift_sp(dfi, now):
    """
        This function flags if an entry on dfo is within Conrad's desk operation time or not.
    """
    dfr = dfi.copy()

    # day of the week
    dow = now.weekday()

    # day type
    dt = "wd" if dow in [0, 1, 2, 3, 4] else "we"

    if trade_shift_hours_only:
        dfr["f_shift_sp"] = dfr["sp"].apply(lambda x:True if (x>=sp_desk[dt]["start"]) &
                                                             (x<sp_desk[dt]["end"]) else False)
    else:
        dfr["f_shift_sp"] = True
    logging.info("flg shift sp-process")
    return dfr["f_shift_sp"]

def flag_for_site_selection(dfi):
    """
        This function flags if a site is flagged to be included in the auto-trading procedures or not.
    """
    dfr = dfi.copy()

    if trade_selected_sites_only:
        dfr["f_sel_site"] = dfr["sites"].apply(lambda x: True if x in tradable_sites else False)
    else:
        dfr["f_sel_site"] = True
    logging.info("flg sites-process")
    return dfr["f_sel_site"]

def flag_for_gc(dfi,now):
    """
        This function flags if an order can
        be traded based on market and BM gate closures.
    """
    dfr = dfi.copy()
    if trade_hh_only:
        # Calc final moment before market & BM gate closures.
        dfr["ts_gc_mkt"] = dfr["timestamp"] - timedelta(minutes=t_gc_market + t_headroom)
        dfr["ts_gc_bm"] = dfr["timestamp"] - timedelta(minutes=t_gc_bm + t_headroom)

        # Mandatory flagging: Calculate the first tradable SP.
        dfr["f_mkt_open"] = dfr["ts_gc_mkt"].apply(lambda x: True if x > now else False)
        dfr["f_bm_open"] = dfr["ts_gc_bm"].apply(lambda x: True if x > now else False)


        # Calc final moment before market & BM gate closures.
        dfr["ts_gc_mkt"] = dfr["timestamp"] - timedelta(minutes=15 + 3)
        dfr["ts_gc_bm"] = dfr["timestamp"] - timedelta(minutes=60 + 3)

        # Mandatory flagging: Calculate the first tradable SP.
        dfr["f_mkt_open"] = dfr["ts_gc_mkt"].apply(lambda x: True if x > now else False)
        dfr["f_bm_open"] = dfr["ts_gc_bm"].apply(lambda x: True if x > now else False)


        if assume_all_sites_nbm:
            # f_open tells us the definitive gate closure: if NBM site => market GC, else BM GC.
            dfr["f_open"] = dfr["f_mkt_open"]
        else:
            dfr["f_open"] = np.where(dfr["st"] == "bm", dfr["f_bm_open"], dfr["f_mkt_open"])
    else:
        print("At the time being, we're trade only HH products. Later on we'll need to work on a methodology"
              "that is inclusive of all products")
    logging.info("flg gc-process")
    return dfr.loc[:, ["f_mkt_open", "f_bm_open","f_open"]]

def calc_mw_to_trade(dfi):
    """
        This function calculates the tradable volume for each entry on dfo.
    """
    dfr = dfi.copy()

    # Column names which we'll use for function below
    cn = ["is_selling", "f_can_trade", "mw_to_buy", "mw_to_sell"]

    def get_vol(x):

        """
            Get volume based on a) is_selling, b) f_can_trade
        """
        if x["is_selling"] & x["f_can_trade"]:
            r = mw_to_trade_min if is_mw_to_trade_min else x["mw_to_sell"]
        elif ~x["is_selling"] & ~x["f_can_trade"]:
            r = mw_to_trade_min if is_mw_to_trade_min else x["mw_to_buy"]
        else:
            r = "there is a problem with get_vol."
        return r


    # Calculate possible volumes to buy & sell regardless of is_selling & f_can_trade.

    # Note that if mw_traded < 0, it means it is sold; therefore, to calculate the remaining available volume
    # we need to add the available volume (mw_av) with traded volume (mw_traded).
    dfr["mw_to_sell"] = np.where(dfr["mw_traded"] <=0 , dfr["mw_av"]+dfr["mw_traded"], dfr["mw_av"])

    # If this entry is a traded entry; the buy-back possible volume == -dfr["mw_traded"]
    dfr["mw_to_buy"] = np.where(dfr["mw_traded"] < 0, -dfr["mw_traded"],0)


    # Finalse the tradable volume.
    dfr["mw_to_trade"] = dfr.loc[:,cn].apply(get_vol, axis=1)
    logging.info("MW to trade condition is checked")
    return dfr["mw_to_trade"]

def flag_can_trade(dfi):
    """
        This function returns if an order is tradable or not.
    """
    dfr = dfi.copy()

    dfr["f_can_sell"] = (dfr["is_selling"] == True) & ((dfr["mw_av"] + dfr["mw_traded"])>=mw_to_trade_min)
    dfr["f_can_buy"] = (dfr["is_selling"]==False) & (dfr["mw_traded"] < 0)

    # f_can_trade is the result of two interim columns calculated above
    dfr["f_can_trade"] = np.where(dfr["is_selling"]==False,dfr["f_can_buy"],dfr["f_can_sell"])
    logging.info("flag can trade-process")
    return dfr["f_can_trade"]

def flag_isin_horizon(dfi):
    """
        This function determines if a given entry on dfo is within the input trading horizon.
        The key input is n_prod_horizon which sets how many products onward of gate closure can we
        trade. e.g. if mkt_gc = 17:00 for an NBM site
        if n_prod_horizon = 2 => last tradable product == 18:00.
    """
    dfr = dfi.copy()

    if trade_hh_only:
        x = (dfr[dfr["f_open"] == True]).copy()
        x = x.groupby(by="st")["timestamp"].min().reset_index()
        x["last_trade_product"] = x["timestamp"] + timedelta(minutes=(n_prod_horizon-1) * 30)
        dfr = dfr.merge(x.loc[:, ["st", "last_trade_product"]], how="left", on="st")
        dfr["f_isin_horizon"] = np.where(
            (dfr["timestamp"] <= dfr["last_trade_product"]) &
            (dfr["f_open"]==True),
            True, False)
    else:
        print("For the time-being, we're only trading hh products."
              "Once we develop sth for 2h and 4h, insert the code here")
    logging.info("flg horizon-process")
    return dfr["f_isin_horizon"]

def finalise_dfo(dfi, data_for_frontend_demo):
    """
    This function takes connects all the flags that we've
    applied on potetial orders & returns only the compliant orders.
    """
    dfr = dfi.copy()

    # If HAWEN is unavailable during the testing phase, we can't continue as the content of the dfo = NULL.
    # Therefore, we add other available sites to the selected sites & to avoid confusing the desk; we trade
    # them under HAWEN id. Hence, we use this boolean to convert those names.
    if all_sites_are_hawen:
        dfr["sites"] = "HAWEN"

    # A part of the testing phase is to start from small volumes where we trade only with minimum tradable volume.
    if is_mw_to_trade_min:
        dfr["mw_to_trade"] = mw_to_trade_min

    # Mask the resulting orders dataframe to valid combinations only.
    # data_for_frontend_demo is an input prepare_orders scripts.
    # If True -> the data for all sites will be returned. We only use this when we're pulling data to show on the
    # front-end. This; however, doesn't mean that the auto-trader will trade all sites.
    # The back-end script of the (which actually puts orders on the market) is (must) input with
    # mf.prepare_orders(data_for_frontend_demo==False). This means that only selected sites will be traded.

    if data_for_frontend_demo == False:

        mask = (
                dfr["f_can_bb"] &
                dfr["f_open"] &
                dfr["f_sel_site"] &
                dfr["f_shift_sp"] &
                dfr["f_can_trade"] &
                dfr["f_isin_horizon"]
        )

    else:
        mask = (
                dfr["f_can_bb"] &
                dfr["f_open"] &
                dfr["f_shift_sp"] &
                dfr["f_can_trade"] &
                dfr["f_isin_horizon"]
        )


    # dfr.to_csv(r"C:\Users\elfa3-extra\Desktop\ss.csv")
    dfr = dfr[mask]

    # There are many columns that are created for dfo and they have served their purpose at this point.
    # Do you want to delete these columns now?
    if del_cols:
        dfr.drop(["date",
                  "f_can_bb", "f_mkt_open", "f_bm_open", "f_open",
                  "f_sel_site", "f_shift_sp", "f_can_trade", "f_isin_horizon"
                  ],
                 inplace=True, axis=1)

    if data_for_frontend_demo ==True:
        pass
    else:

        trade_one_order_per_product = True
        if trade_one_order_per_product:

            dfb = dfr.loc[dfr.is_selling==False,:].copy()

            if dfb.empty==False:
                dfb.sort_values(by=["sp","p_srmc"], ascending=[True,False], inplace=True)
                dfb.drop_duplicates(subset=["sp"],keep="first",inplace=True)

            dfa= dfr.loc[dfr.is_selling==True,:].copy()
            if dfa.empty==False:
                dfa.sort_values(by=["sp","p_srmc"], inplace=True)
                dfa.drop_duplicates(subset=["sp"], keep="first", inplace=True)

            dfr = pd.concat([dfb,dfa],ignore_index=True).reset_index()
    logging.info("finalise dfo-process")

    return dfr

def mf_get_orders(df_av, df_srmc, dft, ts_trade_checked, data_for_frontend_demo):
    """

    :param df_av: availability dataframe
    :param df_srmc: SRMC dataframe
    :param dft: trades dataframe
    :param data_for_frontend_demo: should mf_get_orders return data for front-end or is it for posting orders on mkt?
    :return: dfo: dataframes of eligable orders
    """
    logging.info("started")

    site_names = get_unique_site_names(df_av, df_srmc)
    dfo = init_dfo(df_av, df_srmc, site_names)
    dfo["ts_trades_checked"] = ts_trade_checked
    dfo = format_dfo(dfo, dft, df_av,df_srmc)

    now = pd.to_datetime("today")
    dfo["f_can_bb"] = flag_can_bb(dfo, now)
    dfo.loc[:,["f_mkt_open","f_bm_open","f_open"]] = flag_for_gc(dfo, now)
    dfo["f_sel_site"] = flag_for_site_selection(dfo)
    dfo["f_shift_sp"] = flag_for_shift_sp(dfo, now)
    dfo["f_can_trade"] = flag_can_trade(dfo)
    dfo["f_isin_horizon"] = flag_isin_horizon(dfo)
    dfo["mw_to_trade"] = calc_mw_to_trade(dfo)
    # dfo.loc[:,["mw_ordered", "p_ordered","id_ordered"]] = update_orders_on_mkt(dfo)
    dfo = finalise_dfo(dfo, data_for_frontend_demo)

    return dfo
