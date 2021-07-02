import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import sys

# from constants import *

credentials = {'Username': 'smoody', 'Password': 'Conrad1234!'}
base_url = r'https://conrad-api.iglootradingsolutions.com/IglooWebApi/api/v1.0'
token_url = r'/Token/RequestToken'
trade_url = r"/Asset/GetAssetTradeSummary"
finance_url = r"/Trade/GetFinanceTrades"
market_url = r'/Market/GetMarketTrades'

def get_token():

    # Create the query for getting a token
    url = base_url + token_url
    response = requests.post(url, data=credentials)
    token = json.loads(response.text)["Token"]
    logging.info("token-api")
    return token

def get_wide_arrays_from_trades (dfi):
    is_selling = [0,1,0,1]
    var_col_labels = ["Volume","Volume","VWAP","VWAP"]

    for i, label in enumerate(var_col_labels):

        dfr = dfi[dfi["BuySell"] == is_selling[i]]

        dfr = pd.pivot_table(
            data = dfr.loc[:,["ScheduleDate","Asset","SettlementPeriod",label]],
            values=label,
            index=["ScheduleDate","Asset"],
            columns="SettlementPeriod",
            aggfunc=np.sum,
            fill_value=0)

        if i == 0:
            df_b_vol = dfr
        elif i == 1:
            df_s_vol = dfr
        elif i == 2:
            df_b_price = dfr
        else:
            df_s_price = dfr

    df_net_vol = pd.pivot_table(
        data = dfi.loc[:,["ScheduleDate","Asset","SettlementPeriod","Volume"]],
        values="Volume",
        index=["ScheduleDate","Asset"],
        columns="SettlementPeriod",
        aggfunc=np.sum,
        fill_value=0
    )

    return df_b_vol, df_s_vol, df_b_price, df_s_price, df_net_vol

def return_content_from_etrm(q_type, token):

    if q_type == "use_trade_url":
        url = trade_url
    elif q_type == "use_finance_url":
        url = finance_url
    elif q_type == "use_market_url":
        url = market_url
    else:
        print("You have specified an incorrect query type.")

    # Get the date range for query
    s_start = pd.to_datetime(("today")).strftime('%Y-%m-%d')
    s_end = (pd.to_datetime("today") + timedelta(days=1)).strftime('%Y-%m-%d')

    headers = {
        'Accept': 'application/json',
        'token': token
    }

    if q_type == "use_trade_url":
        input_data = {
            'FromDate': s_start,
            'ToDate': s_end,
            'location': 'UK'
        }
        full_url = base_url + url
        response = requests.post(full_url, data=input_data, headers=headers)

    elif q_type == "use_finance_url":
        input_data = {
            "fromTradeDate": s_start,
            "toTradeDate": s_end,
        }
        full_url = base_url + url
        response = requests.get(full_url, params=input_data, headers=headers)
    elif q_type == "use_market_url":
        input_data = {
            "fromtradedate": s_start,
            "totradedate": s_end,
        }
        full_url = base_url + url
        response = requests.get(full_url, params=input_data, headers=headers)

    # this will return a list of dictionaries by trade which needs to be parsed to panda then uploaded to db
    dfr = pd.DataFrame(json.loads(response.text))
    ts_trade_checked = pd.to_datetime("today")
    # Convert ScheduleDate to DateTime type
    dfr["ScheduleDate"] = pd.to_datetime(dfr["ScheduleDate"])

    # For some reason the query filtering does not work; therefore, we do a filter for all today's trades.
    dfr = dfr[(dfr["ScheduleDate"] >= s_start) & (dfr["ScheduleDate"] < s_end)]
    logging.info("pull initial dft-api")
    return dfr, ts_trade_checked

def pivot_df_trades_for_net_vals(dfi):
    """

    :param dfi: df_trades
    :return: a pivoted df_trades where the net of trades for a HH is returned.
    """
    dfr = dfi.copy()
    dfr2 = dfi.copy()

    # Separate sell and buy VWAPs so that we can np.where it later.
    dfr2["VWAP_sell"] = np.where(dfr2["Volume"]<0,dfr2["VWAP"],0)
    dfr2["VWAP_buy"] = np.where(dfr2["Volume"] >0, dfr2["VWAP"], 0)

    # Pivot on Volume & Merge resulting dfr with interim resulting dfr2.
    dfr = pd.pivot_table(dfr2,values="Volume",index=["Asset","ScheduleDate","SettlementPeriod"],aggfunc=np.sum)
    dfr.reset_index(inplace=True)

    dfr = dfr.merge(
        dfr2.loc[:,["Asset","ScheduleDate","SettlementPeriod","VWAP_sell","VWAP_buy"]],
        how="left",
        on=["Asset","ScheduleDate","SettlementPeriod"])

    # Pick the correct VWAP and drop the sell and buy only ones
    dfr["VWAP"]=np.where(dfr["VWAP_sell"]<0,dfr["VWAP_buy"],dfr["VWAP_sell"])

    # If volume == 0 => Asset has no previous positions => VWAP=0
    dfr["VWAP"] = np.where(dfr["Volume"]==0,0,dfr["VWAP"])

    dfr.drop(labels=["VWAP_sell","VWAP_buy"], axis=1, inplace=True)

    # Pivot table does the netting but it returns 0 sums for some half-hours both for
    dfr= dfr[(dfr["VWAP"]!=0)&(dfr["Volume"]!=0)]

    # Strip \r\n
    dfr["Asset"] = dfr["Asset"].apply(lambda x: x.rstrip('\r\n'))

    #Recreate Buysell flag
    dfr["BuySell"] = np.where(dfr["Volume"]<0,True,False)
    logging.info("pivot dft-process")
    return dfr

def mf_igloo_etrm():
    logging.info("started")
    # Get token
    token = get_token()
    # Return content from igloo etrm: asset trade summary
    # q = "get_asset_trade_summary"
    q_type = "use_trade_url"
    # q_type = "use_finance_url"
    # q_type = "use_market_url"
    df_trades, ts_trade_checked = return_content_from_etrm(q_type, token)
    df = df_trades.copy()
    df_trades = pivot_df_trades_for_net_vals(df_trades)

    return df_trades, ts_trade_checked

def get_gas_locked_in_prices():

    token = get_token()
    dt = pd.to_datetime(("today")).strftime('%Y-%m-%d')


    data = {
        'FromDate': dt,
        'ToDate': dt,
        'location': 'NBP',
    }

    headers = {
        'Accept': 'application/json',
        'token': token
    }

    response = requests.post(
        'https://conrad-api.iglootradingsolutions.com/IglooWebApi/api/v1.0/Asset/GetAssetGasTradeSummary', data=data,
        headers=headers)

    # this will return a list of dictionaries by trade which needs to be parsed to panda then uploaded to db
    df = pd.DataFrame(json.loads(response.text))
    return df

if __name__ == "__main__":
    mf_igloo_etrm()