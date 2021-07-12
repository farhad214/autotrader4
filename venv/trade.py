import requests, json, datetime
import igloo_m7 as m7
import gcp as gcp
import json
import requests
import numpy as np
import pandas as pd
import itertools
import math
from prepare_orders import *
from dateutil import parser
import logging
import pytz

# Boolean for Logging content
blc = True
# from constants import *

base_url = 'https://conrad-api.iglootradingsolutions.com:8443/igloo/iglootrader'
cred_m7 = {'Username': 'smoody', 'Password': 'Conrad1234!'}

# MTO: Match Top Order, CTO: Cut-under Top Order.
mkt_strategies = ["mto", "cto"]

# Whether to include strategy in df_mkt or not.
inc_mkt_strategy = {"mto":True, "cto":True}

# Market strategy
strategy = "cto"

# Asset strategy
asset_strategy = "basic"

# strategy price increment
p_increment = 0.01

# Minimum margin on offering in market (for basic asset strategies)
p_o_margin_min = 10

# If this is true; then the strategy will pick the top order.
# An alternative can /be to pick the top 5 MW or some other logic that you can apply later.
pick_top_order = True

# Towards the end the orders dataframe becomes too large. By doing this, we delete many interim columns.
del_dfo_interim_cols = True

p_inv= 0
p_m7_min = -500
p_m7_max = 3000
n_epex_decimals = 2
round_prices_to_epex_decimals = True
cl_live_order= ["insert_time","order_id","is_selling","site_name",
                "has_traded","block_id","hh_id","igloo_product","volume","price"]

cl_p_asset_limit = {"b":"p_b_asset", "o":"p_o_asset","t":"p_trader"}

cl_p_mkt_ext = {"b":"p_b_mkt_max","o":"p_o_mkt_min"}

# Bid upper limit & offer lower limit
cl_ool = {"b":"p_b_ul","o":"p_o_ll"}

# column labels of final prices:
# asset sets the minimum profitability we expect from asset trades,
# mkt sets the most completitve order from the market,
# ool sets the own order limit (if an order exists on the other side),
# to_trade sets the final price that we're intend to trade with
# margin is profit we've achieved.

# final price column labels.
##### IF YOU CHANGE ANYTHING IN CL_FP; YOU WILL NEED TO CHANGE IT IN SQL_COLS AS WELL.
cl_fp = {
    "asset":"p_asset",
    "mkt":"p_mkt",
    "ool": "p_ool",
    "to_trade": "p_to_trade",
    "margin": "p_margin",
    "srmc":"p_srmc"
}

cl_sql = ["order_id", "date", "created_time", "amended_time", "order_status", "is_selling", "site_name", "block_id",
          "hh_id", "igloo_product", "mw_order", "p_order", "p_margin", "price_setter", "p_asset", "p_mkt", "p_ool", "r_ot"]

ct_sql = ["str", "datetime64", "datetime64", "datetime64", "str", "boolean", "str", "str",
          "int32", "str", "float64", "float64","float64", "str", "float64", "float64", "float64","float64"]

# Number of seconds Assumed Trading Takes Place After Final Order Amendment (n_sec_attpafoa).
n_sec_attpafoa = 1

b = "bid"
o = "offer"
side = "is_selling"
igprod = "igloo_product"

cl_ordered = ["mw_ordered", "p_ordered","id_ordered"]

record_failed_attemps = True

c_sql = {
    "order_id": "str",
    "date":"datetime64",
    "created_time": "datetime64",
    "inserted_time": "datetime64",
    "order_status": "str",
    "is_selling": "boolean",
    "site_name": "str",
    "block_id": "str",
    "hh_id": "int32",
    "igloo_product": "str",
    "mw_order": "float64",
    "p_order": "float64",
    "p_margin": "float64",
    "price_setter": "str",
    "p_asset": "float64",
    "p_mkt": "float64",
    "p_ool": "float64",
    "r_ot":"float64"
}

d_mkt_types = {
        'Id': 'object',
        'Market': 'object',
        'Product': 'object',
        'Side': 'object',
        'Price': 'float64',
        'Volume': 'float64',
        'AllOrNothing': 'int32',
        'CreateTime': 'datetime64',
        'ModifyTime': 'datetime64'
    }
n_mohh = 30
tbl_active_orders = "active_orders"
tbl_archived_orders = "archived_orders"
tbl_failed_attemps = "failed_attempts"
tbl_autotrader_status = "autotrader_status"
schema_m7 = "m7"
r_otr_thresh= 0.95


def convert_to_ukt(x, localise=True):
    ts= pytz.timezone("UTC").localize(x).astimezone(pytz.timezone("Europe/London"))
    if localise:
        ts = ts.tz_localize(None)
    return ts


def get_orders_or_trades(token, ret_type="orders"):

    cl_trades = ["order_id", "igloo_product", "trader", "site_name",
                 "is_selling", "p_trade", "mw_trade", "trade_time"]

    cl_orders = ["order_id", "igloo_product", "trader", "site_name", "is_selling", "p_order",
                 "mw_order", "created_time", "amended_time", "is_active"]

    if ret_type == "orders":
        o = get_orders(token)

        dfr = pd.DataFrame(columns=cl_orders)
        for x in o:
            # print(x)
            v = [x["Id"], x["Product"], x["Trader"], x["Strategy"], x["Side"],
                 x["Price"], x["Volume"], x["CreateTime"], x["ModifyTime"], x["Active"]]
            dfr.loc[dfr.shape[0], :] = v

        dfr["is_selling"] = np.where(dfr["is_selling"] == "Offer", True, False)

        dfr["created_time"] = pd.to_datetime(dfr["created_time"]).apply(lambda x: x.tz_localize(None))
        dfr["amended_time"] = pd.to_datetime(dfr["amended_time"]).apply(lambda x: x.tz_localize(None))
        dfr["created_time"] = dfr["created_time"].apply(convert_to_ukt)
        dfr["amended_time"] = dfr["amended_time"].apply(convert_to_ukt)

        now_str = pd.to_datetime("today").strftime("%Y%m%d%H%M%S--%Y-%M-%d-%H-%M-%S")
        # dfr.to_csv("E://igloo_orders//" + now_str + ".csv", index=False, line_terminator='\n')

        logging.info("m7 orders-process")

    elif ret_type == "trades":
        t = get_trades(token)

        dfr = pd.DataFrame(columns=cl_trades)
        for x in t:
            v = [x["OrderId"], x["Product"], x["Trader"], x["Strategy"], x["Side"],
                 x["Price"], x["Volume"], x["TradeTime"]]
            dfr.loc[dfr.shape[0], :] = v

        dfr["is_selling"] = np.where(dfr["is_selling"] == "Sell", True, False)
        dfr["trade_time"] = pd.to_datetime(dfr["trade_time"]).apply(lambda x: x.tz_localize(None))
        dfr["trade_time"] = dfr["trade_time"].apply(convert_to_ukt)

        now_str = pd.to_datetime("today").strftime("%Y%m%d%H%M%S--%Y-%M-%d-%H-%M-%S")
        # dfr.to_csv("E://igloo_trades//" + now_str + ".csv", index=False, line_terminator='\n')

        logging.info("m7 trades-process")

    elif ret_type=="merged":

        # we merge trades and orders. The reason for this is the strategy (site_name) field of trades query from igloo
        # m7 api doesn't work. So we use order id from orders and merge trades on the left with orders. Once we have the
        # merged trades and orders, we are interested to see when was the last trade that went thourough by asset,
        # by product, and by is_selling values. This is to check it with the timestamp that we checked when we last
        # evaluated the volumes available to trade from the dft dataframe.

        # Pull trades and orders from igloo m7 api
        t = get_orders_or_trades(token,"trades")

        now_str = pd.to_datetime("today").strftime("%Y%m%d%H%M%S--%Y-%M-%d-%H-%M-%S")
        # t.to_csv("E://igloo_trades//" + now_str + ".csv", index=False, line_terminator='\n')

        o = get_orders_or_trades(token,"orders")

        now_str = pd.to_datetime("today").strftime("%Y%m%d%H%M%S--%Y-%M-%d-%H-%M-%S")
        # o.to_csv("E://igloo_orders//" + now_str + ".csv", index=False, line_terminator='\n')

        # Merge them by order id
        dfr = pd.merge(o.loc[:, ["order_id", "site_name","igloo_product","is_selling"]],
                       t.loc[:, ["order_id", "p_trade", "mw_trade", "trade_time"]], on="order_id", how="left")

        # Drom trade_time == Nan => orders that did trade.
        dfr = dfr[dfr.trade_time.isna()==False]

        # Sort values in order below so we can drop duplicates and only keep the most recent one
        dfr.sort_values(by=["igloo_product", "site_name", "is_selling", "trade_time"], inplace=True)
        dfr.drop_duplicates(subset=["site_name", "igloo_product", "is_selling"], inplace=True, keep="last")

    else:
        print("ret_type is incorrect.")

    return dfr

def get_mkt_information(dfi):
    """
    :param dfi: dfo
    :return: returns:
        token: igloo-m7 token,
        dfm: dataframe with market orders, and token
    """
    # Get a token to connect to m7 Igloo.
    token = json.loads((requests.post(base_url + '/oauth/token', data=cred_m7)).text)["Token"]
    logging.info("token-m7api")

    # get a distinct list of igloo products that we will query.
    products = dfi["igloo_product"].unique()

    # get market orders
    dfm = get_igloo_depth(token, products)

    dfm = dfm.astype(d_mkt_types)

    trades = get_orders_or_trades(token, ret_type="trades")
    orders = get_orders_or_trades(token, ret_type="orders")

    return token, dfm, trades, orders

def init_mkt_prices(dfm):
    """
    :param dfm:
    :return: dfp: returns:
        p_mkt_b_max, p_mkt_o_min: maximum market bid & minimum market offer
        p_b_ul & p_o_ll: price of bid upper limit & offer lower limit
    """
    # select all bids|offers that are not own orders.
    df_b_mkt = dfm.loc[(dfm["IsOrder"] == False) & (dfm["Side"] == "Bid"), :]
    df_o_mkt = dfm.loc[(dfm["IsOrder"] == False) & (dfm["Side"] == "Offer"), :]

    # Select own orders
    df_b_own = dfm.loc[(dfm["IsOrder"] == True) & (dfm["Side"] == "Bid"), :]
    df_o_own = dfm.loc[(dfm["IsOrder"] == True) & (dfm["Side"] == "Offer"), :]

    if pick_top_order:
        def pivot_mkt_data(df, func, cl_output, cl_piv_val="Price", cl_piv_ind="Product", il_output="igloo_product"):
            """
            :param df: input dataframe,
            :param func: function that you want to apply with pivot_table,
            :param cl_output: column label of the output, pivoted table,
            :param cl_piv_val: column label for values of pd.pivot_table,
            :param cl_piv_ind: column label for index of pd.pivot_table,
            :param il_output: index label of the output, pivoted table.
            :return: Renamed, pivoted table of market prices.
            """

            dfr = pd.pivot_table(data=df, values=cl_piv_val, index=cl_piv_ind, aggfunc=func)
            dfr.rename(columns={cl_piv_val: cl_output}, inplace=True)
            dfr.index.names = [il_output]
            # dfr.rename(index={cl_piv_ind:il_output}, inplace=True)
            return dfr

        def fill_own_price_cols(dfi):
            """
            :param dfi: merged df that holds market prices
            :param d_own: a dictionary of column labels for own prices (e.g. p_bid_own_max)
            :return: It returns a column of nan if, we do not have any
            """
            dfr = dfi.copy()
            for k, v in cl_ool.items():

                p_ext = p_m7_max if k == "b" else p_m7_min
                if (v in dfr.columns) == False:
                    dfr[v] = p_ext
                else:
                    dfr[v].fillna(p_ext, inplace=True)
            return dfr

        # Get the most competitive market orders: maximum bid & minimum offer
        df_b_mkt_max = pivot_mkt_data(df_b_mkt, np.max, cl_p_mkt_ext["b"])
        df_o_mkt_min = pivot_mkt_data(df_o_mkt, np.min, cl_p_mkt_ext["o"])

        #   df_b_ul is the df that holds the upper limit for bids.
        #   This limit depends on if we have an offer for the same product:
        #       If yes, then the bid upper limit is set by minimum offer price - smallest increment (£0.01/MWh),
        #       If no,  then the bid upper limit is set by maximum offer price - £3000/MWh.
        #   Same logic for df_o_limits; but, the opposite way around which the limit is set by -£500/MWh.
        df_b_ul = pivot_mkt_data(df_o_own, np.min, cl_ool["b"]) - p_increment
        df_o_ll = pivot_mkt_data(df_b_own, np.max, cl_ool["o"]) + p_increment

        dfs = [df_b_mkt_max, df_o_mkt_min, df_b_ul, df_o_ll]

        jt = "left"
        prod = "igloo_product"
        dfr = dfs[0].merge(dfs[1],jt,prod).merge(dfs[2],jt,prod).merge(dfs[3],jt,prod)

        dfr = fill_own_price_cols(dfr)

    else:
        print("The simplest agorithms that we currently use depends only on what is going on at the top of the"
              "stack while we might be able to make better decision if we looked at it differently.")

    # dfr: keeps all the relevant market prices: market prices & strategy prices.
    cl_mkt_strategy = get_cl_strategy()
    dfr.loc[:, cl_mkt_strategy] = np.nan

    logging.info("calc p_mkt-process")

    return dfr

def calc_p_strategy(x):
        """
            This function returns all the strategy values that we want the script to return.
        """
        r = x.copy()

        def apply_strategy(strategy, mkt_side, p_b_mkt_max=None, p_o_mkt_min=None):
            """
                This is the function which applies the input strategy on market prices.
                There are two strategies in place at the time being: cut-under top order & match top order.
                Feel free to add more inputs if you have a strategy which would require inputs other than those
                specified already.
            """
            if strategy == "cto":

                if mkt_side == "b":
                    return p_b_mkt_max + p_increment
                else:
                    return p_o_mkt_min - p_increment

            elif strategy == "mto":

                if mkt_side == "b":
                    return p_b_mkt_max
                else:
                    return p_o_mkt_min

            else:
                print("If you come-up with additional strategies write it here.")

        # Get column labels for strategy content
        cls = get_cl_strategy()

        for col in x.index:

            # if the iterated col is a strategy label (e.g. p_bid_cto, p_offer_mto).
            if col in cls:

                # Split the column label to quantifier (e.g. p, v), side (bid|offer), and strategy (e.g. cto).
                quantifier, mkt_side, strategy = col.split("_")

                # Apply strategy on market prices.
                p = apply_strategy(strategy, mkt_side= mkt_side,
                                   p_b_mkt_max= x[cl_p_mkt_ext["b"]],
                                   p_o_mkt_min=x[cl_p_mkt_ext["o"]])

                # For some reason I can't get a 0.00 value and it come back as 0.9999999999999999991.
                # Therefore, a simple solution is applied here.
                r[col] = round(p, 2)
            else:

                # if not a strategy column => same value.
                r[col] = round(r[col], 2)

        return r

def get_cl_strategy():

    # column label prefix
    cl_prefix = ["p_b_", "p_o_"]

    # Make a list of strategy strings that we want to be calculated.
    cl_strategy = [s for s in mkt_strategies if inc_mkt_strategy[s]]

    # Create the resulting column labels.
    clr = [''.join(map(str, label_tuple)) for label_tuple in itertools.product(cl_prefix, cl_strategy)]

    return clr

def calc_p_asset(dfi, asset_strategy, return_just_margin=False):
    """
    :param dfi: dfo
    :param asset_strategy: strategy which sets the lowest accepted profitability from the asset.
    :return: p_asset_b and p_asset_s.
    """
    dfr = dfi.copy()

    check_for_front_end_input=True

    if asset_strategy == "basic":

        def get_p_ref_bb(x):

            p_traded_is_inv = (x["p_traded"] == p_inv)|(math.isnan(x["p_traded"]))
            if p_traded_is_inv:
                p_ref_bb = x["p_srmc"]
            else:
                p_ref_bb = x["p_traded"] if x["p_srmc"] > x["p_traded"] else x["p_srmc"]
            return p_ref_bb

        if check_for_front_end_input:
            q = 'select * from m7.fe_price_input fe left join m7.asset_groups ag on fe.prod_gr_fe = ag.parent '
            q = q + 'where fe."timestamp">=CURRENT_DATE and ag.parent IS NOT NULL '
            q = q + 'order by fe.prod_gr_fe, ag.sp, fe.timestamp;'

            df_fe = gcp.query_postgresql(q)
            logging.info("select asset groups-sql")

            if df_fe is not None:
                df_fe.drop_duplicates(subset=["prod_gr_fe","sites"], keep="last", inplace=True)
                dfr = pd.merge(dfr, df_fe.loc[:, ["sp", "is_selling", "sites", "p_trader"]],
                               on=["sp", "is_selling", "sites"], how="left")
                logging.info("merge asset groups with dfo-process")

            else:
                dfr["p_trader"] = np.nan

        if return_just_margin==False:

            dfr[cl_p_asset_limit["b"]] = dfr.apply(get_p_ref_bb, axis=1) - p_increment
            dfr[cl_p_asset_limit["o"]] = dfr["p_srmc"] + p_o_margin_min
        else:
            dfr[cl_p_asset_limit["b"]] = - p_increment
            dfr[cl_p_asset_limit["o"]] = p_o_margin_min
    else:
        print("Asset strategy governs the profitability that we're happy to sell at. At the time being, we're "
              "just using a simple minimum margin on top of p_srmc + 0.01 for selling & p_srmc - 0.01 for"
              "buying-back. Later we will develop more strategies which will further enable churning.")
    logging.info("calc p_asset-process")
    return dfr

def simplify_dfo(dfi, dfp, token):
    """

    :param dfi: dfo
    :param dfp: strategy and market prices
    :param token: igloo-m7 token
    :return: returns a simlified, trimmed dfo
    """
    dfr = dfi.copy()
    dfp_int = dfp.copy()

    # Use igloo product key to extract the HH price.
    dfp_int.reset_index(inplace=True)

    # Rename the product column
    dfp_int.rename(columns={"Product": "igloo_product"}, inplace=True)

    # Merge orders with prices
    dfr = dfr.merge(dfp_int, how="left", on="igloo_product")

    # Pick the minimum price depending on side of the order

    def get_p_to_trade(x):

        if x["is_selling"]:

            if x[cl_fp["mkt"]] >= x[cl_fp["asset"]]:

                if x[cl_fp["mkt"]] > x[cl_fp["ool"]]:
                    r = x[cl_fp["mkt"]]

                else:
                    r = x[cl_fp["ool"]]

            else:

                if x[cl_fp["asset"]] >= x[cl_fp["ool"]]:
                    r = x[cl_fp["asset"]]

                else:
                    r = x[cl_fp["ool"]]
        else:
            if x[cl_fp["mkt"]] >= x[cl_fp["asset"]]:

                if x[cl_fp["asset"]] < x[cl_fp["ool"]]:
                    r = x[cl_fp["asset"]]

                else:
                    r = x[cl_fp["ool"]]
            else:

                if x[cl_fp["mkt"]] < x[cl_fp["ool"]]:
                    r = x[cl_fp["mkt"]]

                else:
                    r = x[cl_fp["ot_limit"]]

        return round(r,n_epex_decimals)

    dfr[cl_fp["asset"]] = np.where(dfr["is_selling"]==True,dfr[cl_p_asset_limit["o"]], dfr[cl_p_asset_limit["b"]])
    dfr[cl_fp["asset"]] = np.where(dfr["p_trader"].isna(),dfr[cl_fp["asset"]],dfr["p_srmc"]+dfr["p_trader"])
    dfr[cl_fp["mkt"]] = np.where(dfr["is_selling"]==True,dfr["p_o_"+strategy],dfr["p_b_"+strategy])
    dfr[cl_fp["ool"]] = np.where(dfr["is_selling"]==True,dfr[cl_ool["o"]],dfr[cl_ool["b"]])
    dfr[cl_fp["to_trade"]] = dfr.loc[:,["is_selling", cl_fp["asset"], cl_fp["mkt"], cl_fp["ool"]]].apply(get_p_to_trade, axis=1)

    dfr[cl_fp["margin"]] = np.where(dfr["is_selling"]==True,dfr["p_to_trade"]-dfr["p_srmc"],dfr["p_traded"]-dfr["p_to_trade"])

    def exclude_unchanged_orders(i):
        r = i.copy()
        r["post_order_type"] = r.apply(get_post_order_type, axis=1)
        return r


    dfr = exclude_unchanged_orders(dfr)

    dfr = dfr.loc[(dfr.high_r_ot==False)&(dfr.post_order_type!="unchanged"),:]
    dfr.reset_index(inplace=True, drop=True)

    # We need the token in a vector form so we can use it in a functional form.
    dfr["token"] = token

    if del_dfo_interim_cols:

        del_cols = []
        for col in dfr.columns:
            if (col in cl_fp==False)|(col[:4] in ["p_b_","p_o_"]):
                del_cols.append(col)
        dfr.drop(del_cols, axis=1, inplace=True)
    logging.info("simplify dfo-process")

    return dfr

def get_earliest_tradable_product(site_type):
    """
    :param site_type: BM|NBM
    :return:
    """

    if trade_hh_only:
        now = datetime.now()
        if site_type == "nbm":
            t_tot = t_gc_market+t_headroom
            ts = now + timedelta(minutes=t_tot)
        else:
            t_tot = t_gc_bm + t_headroom+t_headroom_bm
            ts = now + timedelta(minutes=t_tot)

        sp = ts.hour*2+(2 if ts.minute >= n_mohh else 1) + 1

        return sp
    else:
        print("We're only trading HH products. for 2H and 4H think of something")

def submit_failed_attempt(dfi_row, j, order_status):
    """

    :param dfi: this is the row instance of a df_row,
    :param amended_order: boolean which specifies if an order is amended or not
    :param j: json content returned by igloo-m7 platform
    :return: returns None => this function submits a failed attempt into internal SQL database.
    """

    dfr_row = dfi_row.copy()
    dfr_row["comment"] = j["Text"]
    gcp.write_to_postgresql(dfr_row, tbl_name=tbl_failed_attemps, schema_name=schema_m7)

    if order_status == "amended":
        q = "DELETE FROM " + schema_m7 + "." + tbl_active_orders + " WHERE order_id IN ({});".format(order_id)
        q = q.replace("\'", '"')
        q = q.replace("[", "")
        q = q.replace("]", "")
        q = q.replace('"', "'")

        # Execute the query
        _ = gcp.query_postgresql(q, select_query=False)
    logging.info("submit failed attempt-sql")

def insert_order_on_db(x, j):

    """
    :param x: a row instance of dfo,
    :param y: a row instance of content read from internal database,
    :param j: json response from igloo m7 after submition (comes with x only),
    :param new_order: boolean which sets if a submitted order is new or not (comes with x only),
    :param amended_order: boolean which sets if a submitted order is an amendment (comes with x only)
    :return: none is returned. This is a function that sorts out your internal database arrangement.
    """
    if j is None:
        pass
    else:

        if trade_hh_only:
            block_id = np.nan

        if j["Success"]:
            dt = parser.parse(j["Order"]["CreateTime"])
            ts_created = j["Order"]["CreateTime"]
            ts_insert = j["Order"]["CreateTime"]
            order_id = j["Order"]["Id"]
        else:
            dt = pd.to_datetime("today")
            ts_created = dt
            ts_insert = dt
            order_id = "failed_order"

        dt = datetime.strftime(dt, '%Y-%m-%d')
        dt = datetime.strptime(dt, '%Y-%m-%d')
        order_status = x["post_order_type"]
        is_selling = x["is_selling"]
        site_name = x["sites"]
        block_id = block_id
        hh_id = x["sp"]
        igloo_product = x["igloo_product"]
        mw_order = x["mw_to_trade"]
        p_order = x["p_to_trade"]
        p_margin = x["p_margin"]
        p_asset = x["p_asset"]
        p_mkt = x["p_mkt"]
        p_ool = x["p_ool"]
        r_ot = x["r_ot"]
        price_setter = "asset" if p_order == p_asset else "mkt" if p_order == p_mkt else "ool"

        df_row = pd.DataFrame(columns=c_sql.keys())
        df_row = df_row.astype(c_sql)

        df_row.loc[0, c_sql.keys()] = [
            order_id, dt, ts_created, ts_insert, order_status, is_selling, site_name, block_id, hh_id, igloo_product,
            mw_order, p_order, p_margin, price_setter, p_asset, p_mkt, p_ool, r_ot
        ]
        if j["Success"]==False:
            print(j["Text"])
        else:

            if order_status=="new":

                # If successful -> update the active order table
                gcp.write_to_postgresql(df_row, tbl_name=tbl_active_orders, schema_name=schema_m7)
            elif order_status == "amended":

                # Write the query for: Insert the old version of the amended order
                # from active_orders table into the archived_orders table.
                q = "INSERT INTO " + schema_m7 + "." + tbl_archived_orders
                q = q + " SELECT * FROM " + schema_m7 + "." + tbl_active_orders
                q = q + " WHERE order_id = '" + df_row["order_id"].values[0] + "';"

                # Execute the query
                _ = gcp.query_postgresql(q, select_query=False)

                # Write the query for: Update the old version of the amended order in active_orders to the new state.
                q = "UPDATE " + schema_m7 + "." + tbl_active_orders
                q = q + " SET p_order = " + str(df_row["p_order"].values[0])
                q = q + ", p_asset = " + str(df_row["p_asset"].values[0])
                q = q + ", p_mkt = " + str(df_row["p_mkt"].values[0])
                q = q + ", p_margin = " + str(df_row["p_margin"].values[0])
                q = q + ", order_status = '" + str(order_status) + "'"
                q = q + " WHERE order_id = " + "'" + str(df_row["order_id"].values[0]) + "';"
                # Execute the order
                _ = gcp.query_postgresql(q, select_query=False)

            else:
                print("Code shouldn't end up here.")
    logging.info("insert order internally-sql")

def get_post_order_type(x):

    """
    :param mw_ordered: volume that was ordered before this iteration
    :param p_ordered: price that was ordered before this iteration
    :param p_to_trade: price that we want to trade at this iteration
    :return: Returns the post order type out of: new, amended, cancelled.
    Note that a cancelled order is one that is niether new, nor amended
    """

    if x["mw_ordered"] != x["mw_ordered"]:
        r = "new"

    elif (x["p_ordered"] != x["p_to_trade"])|(x["mw_ordered"]!=x["mw_to_trade"]):
        r = "amended"
    else:
        r = "unchanged"

    return r

def submit_a_post_order(x):

    if x["high_r_ot"]==False:

        if x["post_order_type"] == "new":
            j = post_new_order(token=x["token"], product=x["igloo_product"],
                               site=x["sites"], is_selling=x["is_selling"],
                               volume=x["mw_to_trade"], price=x["p_to_trade"])

        elif x["post_order_type"]  == "amended":
            j = amend_order(token=x["token"], order_id=x["id_ordered"],
                            volume=x["mw_to_trade"], price=x["p_to_trade"])

        elif x["post_order_type"]=="unchanged":
            j = None
        else:
            print("Invalid post order type")
    else:
        print("OTR is too high!")

    logging.info("submit post order-process")

    return j

def get_active_order_details(dfi, dfm, orders):
    """Flag orders from dfo thLiat already exists on the market."""
    dfr = dfi.copy()
    orders_i = orders.copy()
    orders_i = orders_i[(orders_i["is_active"])&(orders_i["trader"]=="smoody")]
    orders_i = orders_i.rename(columns={"order_id":"id_ordered","p_order":"p_ordered","mw_order":"mw_ordered"})
    cl_merge_orders = ["igloo_product","site_name","is_selling"]
    cl_merge_dfr = ["igloo_product","sites","is_selling"]

    dfr = dfr.merge(orders_i,how="left", left_on=cl_merge_dfr, right_on=cl_merge_orders)

    if trade_hh_only:
        cl_merge_dfo = ["sites","is_selling","sp"]
        cl_merge_df_sql = ["site_name","is_selling","hh_id"]
        cl = ["sites","is_selling","sp","mw_traded"]

        # Pull live orders from the temporary live_orders table on GCP.
        df_sql = gcp.query_postgresql("select * from " + schema_m7 + "." + tbl_active_orders + ";")
        no_active_orders = True if df_sql is None else False

        if no_active_orders:
            dfr[cl_ordered] = [np.nan, np.nan, np.nan]
        else:

            # Original Live Orders Column Labels (OLOCL); Sorry.
            cl_sql= df_sql.columns

            # merge live_orders (from internal db) with market orders on order id.
            df_sql = df_sql.merge(dfm, how="left", left_on="order_id", right_on="Id")
            df_sql = df_sql.loc[:, cl_sql]
    else:
        print("Igloo is supposed to provide us with a solution that we won't "
              "need to store live orders on an internal database")
    logging.info("get active orders internally-sql")

    return dfr.loc[:,cl_ordered], df_sql

def check_status(token=None):

    q = "SELECT autotrader_status from " + schema_m7 + "." + tbl_autotrader_status + ";"
    autotrader_status = gcp.query_postgresql(q).iloc[0,0]

    if autotrader_status==False:
        q = "SELECT * FROM " + schema_m7 + "." + tbl_active_orders + ";"
        df_sql = gcp.query_postgresql(q)
        df_sql["order_status"] = "cancelled"

        # Cancel orders on market
        n_cancelled = 0
        for row in df_sql.iterrows():
            order_id = row[1]["order_id"]
            if token is None:
                token = json.loads((requests.post(base_url + '/oauth/token', data=cred_m7)).text)["Token"]

            j = cancel_order(id=order_id, token=token)
            n_cancelled +=1

        if n_cancelled == df_sql.shape[0]:
            gcp.write_to_postgresql(df_sql, tbl_name=tbl_archived_orders, schema_name=schema_m7)
            for row in df_sql.iterrows():

                q = "DELETE FROM " + schema_m7 + "." + tbl_active_orders
                q = q + " WHERE order_id IN ('{}');".format(str(row[1]["order_id"]))
                q = q.replace("\'", '"')
                q = q.replace("[", "")
                q = q.replace("]", "")
                q = q.replace('"', "'")
                _ = gcp.query_postgresql(q, select_query=False)
        else:
            print("Auto-trader failed to remove all active orders from the market.")
    else:
        logging.info("Check autotrader status.")
        return autotrader_status

    logging.info("check autotrader status-sql")
    return autotrader_status


def post_new_order(token, product, site, is_selling, volume, price, is_active=True):
    side = "Offer" if is_selling else "Bid"
    data = {
        "Market": "EPEX",
        "Product": product,
        "Trader": "smoody",
        "Strategy": site,
        "OrderType": "Limit",
        "Side": side,
        "Price": price,
        "Volume": volume,
        "Active": True,
        "AllOrNothing": False
    }
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}

    url = base_url + "/order/new"

    response = requests.post(url, headers=headers, data=data)
    logging.info("post new order-m7api")

    return json.loads(response.text)


def amend_order(token, order_id, volume, price, request_id='abc123', allornothing=False, text="text1"):
    data = {
        "RequestId": request_id,
        "Id": order_id,
        "Price": price,
        "Volume": volume,
        "AllOrNothing": allornothing,
        "Text": text
    }
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}
    url = base_url + "/order/amend"
    response = requests.post(url, headers=headers, data=data)
    logging.info("amend order-m7api")
    return json.loads(response.text)


def cancel_order(id, token, request_id='abc123'):
    data = {
        "RequestId": request_id,
        "Id": id
    }
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}
    url = base_url + "/order/cancel"
    response = requests.post(url, headers=headers, data=data)
    logging.info("cancel order-m7api")
    return json.loads(response.text)

def get_igloo_depth(token, products):

    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}
    dfr = pd.DataFrame()

    for product in products:
        url = base_url+"/marketdata/depth"+"?product="+str(product)
        response = requests.get(url, headers=headers)
        j = json.loads(response.text)
        dfr = pd.concat([dfr, pd.DataFrame(j["Bids"]),pd.DataFrame(j["Offers"])])
    logging.info("get market depth-m7api")

    return dfr

def get_trades(token, order_id =None, trade_date=None, trade_time = None, product=None, id=None, market = "EPEX", account=None,
               strategy=None, side=None, price=None, volume=None, text = None):
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}

    data = {
        "Id": id,
        "Market": market,
        "Account": account,
        "OrderId": order_id,
        "Product": product,
        "Trader": "smoody",
        "Strategy": strategy,
        "Side": side,
        "Price": price,
        "Volume": volume,
        "TradeDate": trade_date,
        "TradeTime": trade_time,
        "Text": text
    }

    if product != None:
        url = base_url + "/trade/trades" +"?"+str(product)
    else:
        url = base_url + "/trade/trades"


    response = requests.get(url, headers=headers, data = data)
    logging.info("get trades-m7api")
    return json.loads(response.text)

def get_orders(token, status="all", product=None):

    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}

    if product != None:
        url = base_url + "/order/orders" +"?"+str(product)
    else:
        url = base_url + "/order/orders"

    if status=="all":
        url = url
    else:
        is_active = True if status =="active" else False
        url = url + "&active=" + str(is_active)

    response = requests.get(url, headers=headers)
    logging.info("get orders-m7api")
    return json.loads(response.text)

def check_inactive_orders(token, df_sql, dfm, trades):
    if df_sql is not None:
        dfr = df_sql.copy()
        dfm_i = dfm.copy()
        dfr = dfr.merge(dfm_i.loc[:,"Id"],how="left",left_on="order_id",right_on="Id")
        dfr["is_inactive"] = np.where(dfr["Id"].isnull(), True, False)

        if dfr["is_inactive"].sum()>=1:
            dfr = dfr[dfr["is_inactive"]]
            obj = trades.loc[:,["order_id","trade_time","trader"]]
            dft = pd.DataFrame(np.array(obj),columns=["order_id", "trade_time", "trader"])
            dfr = dfr.merge(dft, on="order_id", how="left")
            dfr["order_status"] = np.where(dfr["trade_time"].isnull(), "expired", "traded")

            if trade_hh_only:
                dfr["st"] = np.where(dfr['site_name'].isin(bm_sites),"bm","nbm")
                t_bm_tot  = t_gc_bm+t_headroom_bm+t_headroom
                t_mkt_tot = t_gc_market+t_headroom
                dfr["t_gc"] = np.where(dfr["st"]=="bm",t_bm_tot , t_mkt_tot)
                dfr["expiry_time"] = (
                        dfr["created_time"].dt.normalize() +
                        dfr.loc[:,"hh_id"].apply(lambda x: timedelta(minutes=((x-1)*n_mohh)))-
                        dfr.loc[:,"t_gc"].apply(lambda x: timedelta(minutes=x))
                )
                dfr["amended_time"] = np.where(dfr["order_status"]=="traded", dfr["trade_time"], dfr["expiry_time"])
                dfr["amended_time"] = pd.to_datetime(dfr["amended_time"])
                dfr["amended_time"] = dfr["amended_time"].dt.strftime('%Y-%m-%d %H:%M:%S')

                dfr = dfr.loc[:, df_sql.columns]
                dfr = dfr.astype(c_sql)
                gcp.write_to_postgresql(dfr.loc[:, df_sql.columns], tbl_archived_orders, schema_m7)

                for row in dfr.iterrows():
                    # Write the query for: Delete the expired, traded, or cancelled order from the active_orders table
                    q = "DELETE FROM " + schema_m7 + "." + tbl_active_orders
                    q = q + " WHERE order_id IN ('{}');".format(str(row[1]["order_id"]))
                    q = q.replace("\'", '"')
                    q = q.replace("[", "")
                    q = q.replace("]", "")
                    q = q.replace('"', "'")
                    _ = gcp.query_postgresql(q, select_query=False)
            else:
                print("Fill this section when you start trading 2h, 4h products")
    logging.info("check inactive orders internally-sql")

def calc_otr(trades, orders, dfo):

    dfr = dfo.copy()
    no_orders = False
    no_trades = False

    if trades.empty==False:
        piv_t = pd.pivot_table(data=trades, values="is_selling", index="igloo_product", aggfunc="count")
        piv_t.rename(columns={"is_selling":"n_trades"},inplace=True)
    else:
        n_trades=0
        no_trades=True

    if orders.empty==False:
        piv_o = pd.pivot_table(data=orders, values="is_selling", index="igloo_product", aggfunc="count")
        piv_o.rename(columns={"is_selling": "n_orders"}, inplace=True)
    else:
        n_orders=0
        no_orders=True

    if (no_orders) & (no_trades):
        r_ot = (n_orders / (n_trades + 1)) / 100
        dfr["high_r_ot"] = False
        return dfr

    if (no_orders ==False) & (no_trades==False):
        piv = pd.merge(left=piv_t, right=piv_o, on="igloo_product", how="outer")
        piv.fillna(0,inplace=True)
        piv["r_ot"] = (piv["n_orders"]/(piv["n_trades"]+1))/100

    elif (no_orders==False) & (no_trades==True):
        piv = piv_o
        piv["r_ot"] = piv["n_orders"]/100
    elif (no_orders == True) & (no_trades == False):
        print("I think no_order=True and no_trade=False can never happen. If you see this on screen"
              " then it has happened.")
    #     piv = piv_t
    #     piv["r_ot"] = (piv["n_orders"]/(n_trades+1))/100

    piv.reset_index(inplace=True)
    dfr = dfr.merge(piv.loc[:, ["igloo_product", "r_ot"]], left_on="igloo_product", right_on="igloo_product", how="left")
    dfr["r_ot"].fillna(0, inplace=True)
    dfr["high_r_ot"] = np.where(dfr["r_ot"] > r_otr_thresh, True, False)
    # dfr["r_ot"] = 0
    # dfr["high_r_ot"] = False
    logging.info("otr-process")

    return dfr

def get_strategies(dfo):

    if trade_hh_only:
        sites = dfo["sites"].unique()
        strategies = ["asset", "mkt"]
        sps = range(1, 49)

        d={}
        for s in strategies:

            d[s]={}
            for site in sites:

                d[s][site] = {}
                for sp in sps:

                    d[s][site][sp] = asset_strategy if s=="asset" else strategy
    else:
        print("2h & 4h products need further work.")
    # print(d)
    # dfo["asset_strategy"] = dfo.loc[:,["sites","sp"]].apply(lambda x: d["asset"][x["sites"]][])
    logging.info("get strategies-process")

    return d

def remove_multiple_orders_by_product_by_side(o,token):

    fa = o.loc[(o.trader == "smoody") & (o.is_active == True) & (o.is_selling == True), :].copy()
    fb = o.loc[(o.trader=="smoody")& (o.is_active==True) & (o.is_selling==False),:].copy()

    fa.sort_values(by=["igloo_product", "p_order"],inplace=True)
    fb.sort_values(by=["igloo_product","p_order"],ascending=False,inplace=True)

    if fa.empty==False:
        b_fa = fb.duplicated(subset=["igloo_product"],keep='first')
        for id in fa.loc[b_fa,"order_id"]:
            cancel_order(id, token)

    if fb.empty==False:
        b_fb = fb.duplicated(subset=["igloo_product"],keep='first')
        for id in fb.loc[b_fb,"order_id"]:
            cancel_order(id, token)

def check_order_competitiveness(x, token):
    """
    :param x: is an instance of dfo
    :param token: required for igloo m7 access
    :return: bool which specifies if x is the most competitive order to be set on market.
    """
    # get all m7 orders on market
    o = get_orders_or_trades(token, ret_type="orders")

    # if there are no orders on the market; then x == most competitive.
    if o.empty:
        o_is_competitive = True
        return o_is_competitive

    # filter it: a) to auto-trader generated, b) active, c) x's product, d) x's trade side, e) not x's site.
    # if it is x's site then we might be trying with an amended post order. What bothers us at this point 'is this order
    # that belongs to site x1 is the most competitive in our currently available series of x1,...,xn.'

    m = o.loc[(o.trader=="smoody")&
              (o.is_active==True)&
              (o.igloo_product == x[1]["igloo_product"]) &
              (o.is_selling==x[1]["is_selling"]) &
              (o.site_name!=x[1]["sites"]),:]

    # if there are no orders -> x is competitive.
    if m.empty:
        o_is_competitive = True
    else:
        # initiate assuming x is competitive
        o_is_competitive = True

        # iterate through all instances of m. hypothetically, m should not have more than 1 instance. We are looping
        # though as this might not be the case.

        for i in m.iterrows():
            if i[1]["is_selling"]:
                #  left hand of the inequality -> order that is already on the market,
                # right   "  "  "        "     -> order that we want to put on the market.

                # if the former ask order is cheaper than what we're trying to put on the market -> then what we intend
                # to put on the market is not competitive; hence, return o_is_competitive==False.

                if i[1]["p_order"]<= x[1]["p_to_trade"]:
                    o_is_competitive = False

                # if latter ask order is cheaper -> what we're trying to put on the market is more competitive than what
                # we already have. Therefore, cancel the one we have on the market and return true.
                else:
                    cancel_order(i[1]["order_id"],token)
            else:

                # the bid side; therefore, opposite of above.
                if i[1]["p_order"]>=x[1]["p_to_trade"]:
                    o_is_competitive = False
                else:
                    cancel_order(i[1]["order_id"], token)
    logging.info("order competitiveness-process")
    return o_is_competitive

def check_if_traded_after_check(token, x):

    # Pull trades from igloo m7 api.
    t = get_orders_or_trades(token, ret_type="trades")

    now=pd.to_datetime("today")

    # Convert trade_time column to datetime type
    t["trade_time"] = pd.to_datetime(t["trade_time"])

    # Filter trades to those instances that has the same site-name, same trade side, and same product.
    j = t.loc[
        (t.site_name == x[1]["sites"]) &
        (t.is_selling == x[1]["is_selling"]) &
        (t.igloo_product == x[1]["igloo_product"]) &
        (t.trade_time>=x[1]["ts_trades_checked"])
    ]

    # # Sort so that the most recent traded volume sits at top of the dataframe.
    # j.sort_values(by="trade_time",ascending=False, inplace=True)
    if j.empty:
        r = False
    else:
        r = True
    return r

    # if j.empty == False:
    #
    #     # Latest timestamp that has traded.
    #     ts_last_traded = j["trade_time"].values[0]
    #
    #     # Latest timestamp that we pulled content from dft.
    #     ts_last_dfo_updated = x[1]["ts_trades_checked"]
    #
    #     # last time a trade went thorough for this combination return true else false
    #     if ts_last_traded >= ts_last_dfo_updated:
    #
    #         # This means that for x prodct, y side, z asset => a trade after we checked etrm has gone through.
    #         # Therefore, do not post this order.
    #         r = True
    #
    #     else:
    #
    #         # No trades has gone through since we have updated from etrm. You can post your order.
    #         r = False
    #
    # else:
    #     # No trades has gone through. You can post your order.
    #     r = False

    logging.info("check untradedness-process")
    return r

def cancel_hanging_orders(token):
    """
    :param token: m7 igloo token
    :return: Gate closure happens for mkt and bm assets with 15, 60 minutes + process headroom (3 minutes) before the
    moment of delivery. Limit orders sometimes stays untraded on the market and we need a mechanism that removes those
    orders once the auto-trader considers them expired. This function does that
    """

    def get_sp_from_igloo_product(x):
        # sp string
        sp_str = ""
        for s in x:
            if s.isdigit():
                sp_str = sp_str + s
        return int(sp_str)

    if trade_hh_only:

        # Pull active auto-traded orders from the market
        o = get_orders_or_trades(token, ret_type="orders")
        m = o.loc[(o.trader == "smoody") & (o.is_active == True)].copy()

        # Set the site type: bm|nbm
        m["st"] = m["site_name"].apply(lambda x: "bm" if x in bm_sites else "nbm")

        # Extract the sp from igloo_product string
        m["sp"] = m["igloo_product"].apply(get_sp_from_igloo_product)

        # Get now and today
        now = pd.to_datetime("today")
        today = now.normalize()

        # Convert sp to timestamp
        m["ts"]=m["sp"].apply(lambda x: timedelta(minutes=((x-1)*n_mohh))) + today

        # Calculate gc for mkt and bm
        t_mkt_tot = t_gc_market+t_headroom
        t_bm_tot = t_gc_bm + t_headroom+t_headroom_bm
        m["ts_gc_mkt"] = m["ts"] - timedelta(minutes= t_mkt_tot)
        m["ts_gc_bm"] = m["ts"] - timedelta(minutes= t_bm_tot)

        # flag if now
        m["f_mkt_open"] = m["ts_gc_mkt"].apply(lambda x: True if x > now else False)
        m["f_bm_open"] = m["ts_gc_bm"].apply(lambda x: True if x > now else False)
        m["f_open"] = np.where(m["st"]=="bm",m["f_bm_open"], m["f_mkt_open"])

        # Cancel orders that are sitting on lob, post gc.
        for order in m.iterrows():
            if order[1]["f_open"]:
                pass
            else:
                cancel_order(order[1]["order_id"], token)
                print("Cancelled hanging order.")
    else:
        print("It won't work with 2h or 4h products.")
    logging.info("Check hanging orders-process")

def mf_trade(dfo):

    if dfo.empty==False:

        logging.info("started")

        d = get_strategies(dfo)

        # Calculate asset strategy prices. This is the minimum bid and offer prices that we expect from the asset.
        dfo = calc_p_asset(dfo, asset_strategy)

        token, dfm, trades, orders = get_mkt_information(dfo)

        # Deal with live orders: if they're still alive -> flag them on dfo, if not archive them.
        dfo.reset_index(inplace=True, drop=True)
        dfo.loc[:,cl_ordered], df_sql = get_active_order_details(dfo, dfm, orders)

        dfo = calc_otr(trades, orders, dfo)
        dfp = init_mkt_prices(dfm)

        check_inactive_orders(token, df_sql, dfm, trades)

        # Calculate the flagged strategy prices (all of them).
        dfp = dfp.apply(calc_p_strategy, axis=1)
        logging.info("calc strategies-process")

        # Simplify dfo before submit; add columns that are the summary of multiple columns and delete interim columns.
        dfo = simplify_dfo(dfo, dfp, token)

        for x in dfo.iterrows():

            autotrader_status = check_status(token)

            if autotrader_status==False:
                return autotrader_status

            has_traded_after_check = check_if_traded_after_check(token, x)
            if has_traded_after_check:
                print("Asset has traded since we checked dft in ETRM")
                continue

            order_is_competitive = check_order_competitiveness(x, token)
            if order_is_competitive==False:
                print("There is a more competitive order by another asset on the market.")
                continue

            # j = submit_a_post_order(x[1])
            insert_order_on_db(x[1], j)

        cancel_hanging_orders(token)

        print(pd.to_datetime("today").strftime("%H:%M:%S"))

        autotrader_status = check_status(token)

        return autotrader_status

    else:
        print("The order dataframe (dfo) is empty, for one reason or another.")

