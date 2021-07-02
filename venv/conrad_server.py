import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import gcp as gcp
import igloo_etrm as etrm
import sqlalchemy as sql
import numpy as np
import itertools
import ion as ion
import logging

# Boolean for Logging content
blc = True

# from constants import *

pd.set_option('display.max_columns', 5)

# number of date offsets to use, when iterating backwards when finding an empty df in conrad database.
n_o_dt = 5

# number of queries to pull from conrad database (1. SRMC, 2. Availability, & 3. Gas cashout price)
n_q_db = 3

                                        # conversion factor from pence/therm => £/MWh => /100 * 0.02930743
p_therm_to_gbp_mwh = 2.930743
c_pp_th_to_p_mwh = 0.34121  # 1 p/th -> 0.01 £/th -> 0.01/0.0293001 £/MWh = 0.34121
conrad_db_cred = {
    "driver":"{SQL Server Native Client 11.0}",
    "server":"conrad-mssqlserver.database.windows.net",
    "database":"BMRS",
    "username":"DataAdmin",
    "password":"tdot5643@13!"
}

n_hour_new_id_gas_thresh = 4
c_th_to_mwh = 0.0293001
c_mwh_to_th = 34.121

def read_from_mssql(query_str, cn_inp=""):

    q_db_cred = 'mssql+pyodbc://'
    q_db_cred = q_db_cred + f'{conrad_db_cred["username"]}:{conrad_db_cred["password"]}@{conrad_db_cred["server"]}'
    q_db_cred = q_db_cred + '/BMRS?driver=SQL+Server'

    engine = sql.create_engine(q_db_cred)

    conn = engine.connect()
    df = pd.read_sql_query(query_str, con=conn)

    conn.close()
    engine.dispose()

    if cn_inp != "":
        df.columns = cn_inp

    return df

# def read_from_mssql(db_details, query_str, cn_inp=""):
#
#     cnxn = pyodbc.connect ('Driver='+ db_details["driver"] +
#                            ';Server=' + db_details["server"] +
#                            ';Database='+ db_details["database"] +
#                            ';UID='+db_details["username"] +
#                            ';PWD='+db_details["password"])
#     dfr = pd.read_sql (query_str, con=cnxn)
#
#     csr = cnxn.cursor()
#     csr.close()
#     cnxn.close()
#     if cn_inp != "":
#         dfr.columns = cn_inp
#
#     return dfr

def construct_queries(dt_str):
    return {
        "srmc": "exec dbo.getSRMC @SRMCdate = '" + dt_str + "';",
        "av": "SELECT * FROM BMRS.dbo.tblOpsSiteAvailability where SettlementDate = '" +
              dt_str + "'order by SiteID,SettlementDate,SettlementPeriod,LastUpdated;",
        "gas_co":"SELECT * FROM BMRS.dbo.prices_daily_sapgas where ApplicableFor='"+dt_str+"';"}

def get_df_from_server(query_str, dt_rng):
    """
    This function returns a df from conrad server, based on input
    a) query string,
    b) today's date if available, if not iterate backwards until input constant number of offset dates.

    """
    # iterate over each day (1 to n_o_dt)


    for dt in dt_rng:
        dt_str = dt.strftime("%d-%b-%Y")
        q_dict = construct_queries(dt_str)
        # print(q_dict[query_str])

        # df = read_from_mssql(db_details=conrad_db_cred, query_str=q_dict[query_str])
        df = read_from_mssql(query_str=q_dict[query_str])

        # if there is a dataframe -> good get out.
        if not df.empty:
            return df

        # if we've reached the last allowed offset date -> generate a custom error.
        if dt == dt_rng[-1]:
            return print("No entry for", query_str,
                         " query type, between ", dts[0].strftime("%d-%b-%Y"), " & ", dt_str)

def process_srmc(dfi, pp_gas_prompt):

    dfr = dfi.copy()

    # # Make a copy of df_gas_forwards and chane column name for merging
    # df = df_gas_forwards.copy()
    # df.rename(columns={"asset":"siteid"}, inplace=True)
    #
    # # Fill the resulting dataframe with forward gas trades
    # dfr=pd.merge(dfr, df, on="siteid", how="left")
    #
    # # Fill the rest with recent prompt gas price
    # dfr["pp_b"].fillna(pp_gas_prompt, inplace=True)
    # dfr["buy_id_gas"].fillna(True,inplace=True)


    # # Determine the price of gas
    # dfr["pp_gas"] = np.where(dfr["buy_id_gas"], pp_gas_prompt, dfr["pp_b"])
    dfr["pp_gas"] = pp_gas_prompt

    # Convert values to £/MWh
    dfr["p_mwh_gas_b"] = dfr["pp_gas"] * c_pp_th_to_p_mwh

    # Invert efficiency so that we can account fo sites with EFF==0.
    dfr["inv_efficiency"] = dfr["efficiency"].apply(lambda x: 1 / x if x != 0 else 1)

    # Calculate the gas related costs.
    dfr["gas_related_cost"] = dfr["pp_gas"] * dfr["inv_efficiency"]*c_pp_th_to_p_mwh

    # Add gas related costs to SRMC. Note before this funciton SRMC is all expenses except gas costs.
    dfr["SRMC"] = dfr["SRMC"] + dfr["gas_related_cost"]

    dfr = dfr.sort_values(by=["settlementdate","siteid","settlementperiod","SRMC"])

    # there are multiple SRMC values for site x sp combos. Don't know why. Drop all except highest SRMC.
    dfr = dfr.drop_duplicates(subset=["settlementdate","siteid","settlementperiod"], keep="last")
    logging.info("df_srmc-conrad_server")

    return dfr

def get_wide_srmc(dfi):

    dfr = dfi.copy()
    df_srmc_w = pd.pivot_table(dfr.loc[:, ["settlementdate", "settlementperiod", "siteid", "SRMC"]],
                               values="SRMC",
                               index=["settlementdate", "siteid"],
                               columns="settlementperiod")

def get_wide_av(dfi):
    dfr = dfi.copy()
    # Only keep the most recent availability numbers.
    dfr = dfr.drop_duplicates(subset=["SiteID","SettlementPeriod","SettlementDate"],keep="first")
    dfr.reset_index(inplace=True,drop=True)
    dfr["SettlementDate"] = dfr["SettlementDate"].dt.strftime("%d-%b-%y")
    dfr = pd.pivot_table(data = dfr.loc[:,["SettlementDate","SiteID","SettlementPeriod","MWExport"]],
                             values="MWExport",
                             index=["SettlementDate","SiteID"],
                             columns="SettlementPeriod")

    return dfr

def get_gas_forward_profile(df_av):

    # Pull all the input locked in gas prices for today
    df = etrm.get_gas_locked_in_prices()

    # Populate a reference array with all asset and is_selling combinations
    dfr = pd.DataFrame(itertools.product(df["Asset"].unique(), [True,False]), columns=["Asset", "BuySell"])
    dfr=pd.merge(dfr,df,how="left",on=["Asset","BuySell"])
    dfr.fillna(0,inplace=True)

    # Calculate revenue
    dfr["rev"] = dfr["Volume"]*dfr["VWAP"]

    # Pivot table so that volume is neted (sum of buys + sells) and price is averaged (no need for weighted average
    dfr = pd.pivot_table(dfr, values=["Volume","rev"],
                         columns="BuySell", index="Asset",
                         aggfunc=np.sum).reset_index()

    # Rename columns for ease of use
    dfr.columns = ["asset", "th_b", "th_a", "rev_b", "rev_a"]

    # calculate weighted average p/th price of asked gas
    dfr["pp_a"]= dfr["rev_a"]/dfr["th_a"]

    # calc. WA p/th price of bought gas
    dfr["pp_b"] = dfr["rev_b"] / dfr["th_b"]

    # Fill NA values on volume columns so that we can calculate net.
    dfr.loc[:,["th_b","th_a"]]=dfr.loc[:,["th_b","th_a"]].fillna(value=0)

    # Calculate net therm for every asset.
    dfr["th_net"] = abs(dfr["th_b"]) - abs(dfr["th_a"])

    # Calculate the net price of gas: if net volume is long -> buy price, and vice versa
    dfr["pp_net"] = np.where(dfr["th_net"]>0,dfr["pp_b"],dfr["pp_a"])

    # Exclude entries where net therms is not positive (we're short of gas).
    dfr = dfr.loc[dfr.th_net>0, ["asset", "th_net", "pp_b"]]

    # Get today's forward volumes (including MA, WA, and DA stages).
    df_vols = ion.get_todays_vols()

    dfr = pd.merge(dfr,df_vols.loc[:,["asset","eff","th_g_req_gen"]],on="asset",how="left")

    dfr["th_g_forward_extra"] = dfr["th_net"] - dfr["th_g_req_gen"]
    dfr["mwh_g_forward_extra"] = dfr["th_g_forward_extra"]*c_th_to_mwh
    t_gc_market = 15  # minutes before market gate closure.
    t_headroom = 3  # assumed headroom time for processing.
    now = (pd.to_datetime("today")+timedelta(minutes= t_gc_market + t_headroom))
    n_minutes = (1 if now.minute<31 else 2)*30
    ts_first_tradable_product = now.normalize()+timedelta(hours=now.hour, minutes=n_minutes)
    df1 = (df_av.loc[df_av["Start(GMT)"] == ts_first_tradable_product, :]).copy()
    df1.rename(columns={"SiteID":"asset","MWExport":"mw_av"},inplace=True)
    dfr = pd.merge(dfr,df1.loc[:,["asset","mw_av"]],"left","asset")
    dfr["th_req_for_one_hh"] = (dfr["mw_av"]/2)*c_mwh_to_th
    dfr["buy_id_gas"] = np.where(dfr["th_req_for_one_hh"]>=dfr["th_g_forward_extra"], True, False)
    return dfr

def mf_conrad_server(check_id_prices=True):
    logging.info("started")

    # create a date range where start date is today and end date is today - input number of offset dates
    dt_rng = pd.date_range(end=datetime.today()-timedelta(days=0),periods=n_o_dt, normalize=True)[::-1]

    df_av = get_df_from_server("av", dt_rng)
    df_av.sort_values(by=["SiteID","SettlementPeriod","LastUpdated"],inplace=True)
    df_av.drop_duplicates(subset=["SiteID","SettlementPeriod"],keep='last',inplace=True)
    logging.info("df_av-import")

    # pull values from the sql server.
    df_srmc = get_df_from_server("srmc", dt_rng)
    # df_gas_forwards = get_gas_forward_profile(df_av)
    logging.info("SRMC-import")

    if check_id_prices:
        q = "select * from m7.id_gas where inserted_time=(select max(inserted_time) from m7.id_gas);"
        df_gas_id = gcp.query_postgresql(q)
        logging.info("pp_gas_id-sql")
        ts_most_recent_id_gas = df_gas_id.loc[0,"inserted_time"]
        new_id_gas_is_needed = pd.to_datetime("today") - timedelta(hours=n_hour_new_id_gas_thresh) > ts_most_recent_id_gas

        if new_id_gas_is_needed==False:
            pp_gas_prompt = df_gas_id.loc[0,"p_id_gas"]
        else:
            df_gas_co = get_df_from_server("gas_co", dt_rng)
            logging.info("pp_gas_co-sql")
            pp_gas_prompt = df_gas_co.loc[df_gas_co["tag"] == "SAP", "Price"][0]
    else:
        df_gas_co = get_df_from_server("gas_co", dt_rng)
        logging.info("pp_gas_co-sql")
        pp_gas_prompt = df_gas_co.loc[df_gas_co["tag"] == "SAP", "Price"][0]


    df_srmc = process_srmc(df_srmc, pp_gas_prompt)

    return df_av, df_srmc

if __name__ == "__main__":

    mf_conrad_server()