import conrad_server as conrad
import igloo_m7 as m7
import igloo_etrm as etrm
import prepare_orders as po
import trade as trade
import gcp as gcp
import json, requests
import pandas as pd
import threading
import __main__ as main

# from constants import *

base_url = 'https://conrad-api.iglootradingsolutions.com:8443/igloo/iglootrader'
cred_m7 = {'Username': 'smoody', 'Password': 'Conrad1234!'}

def clear_active_orders():
    token = json.loads((requests.post(base_url + '/oauth/token', data=cred_m7)).text)["Token"]

    k = trade.get_orders(token)
    cl_orders = [
        "order_id", "igloo_product", "trader", "site_name", "is_selling", "p_order",
        "mw_order", "created_time", "amended_time", "is_active"
    ]
    orders = pd.DataFrame(columns=cl_orders)
    for x in k:
        v = [x["Id"], x["Product"], x["Trader"], x["Strategy"], x["Side"],
             x["Price"], x["Volume"], x["CreateTime"], x["ModifyTime"], x["Active"]]
        orders.loc[orders.shape[0], :] = v
    autotrader_active_order_ids = orders.loc[(orders.trader == "smoody") & (orders.is_active == True), "order_id"]

    if blc: log1.log_content(trade.cancel_order.__name__, (main.__file__).split("/")[-1])
    for id in autotrader_active_order_ids.iteritems():
        trade.cancel_order(id[1], token)

def mf_main():

    autotrader_status = True
    q = f"update m7.autotrader_status set autotrader_status = '{autotrader_status}' where uid = 0;"
    _ = gcp.query_postgresql(q, select_query=False)

    df_av, df_srmc = conrad.mf_conrad_server()

    while autotrader_status == True:
        dft, ts_trade_checked = etrm.mf_igloo_etrm()
        dfo = po.mf_get_orders(df_av, df_srmc, dft, ts_trade_checked, data_for_frontend_demo=False)
        autotrader_status = trade.mf_trade(dfo)

def mf_mode(on_production):

    if on_production:

        # A try & except block to catch errors when auto-trader is on_production.
        try:
            mf_main()

        # If an error happens clear active orders from the market.
        except:
            clear_active_orders()

    else:
        # Development mode
        mf_main()

    # If code reaches this point the stop_autotrader == True (set either by the analyst|trader).
    clear_active_orders()


if __name__ == "__main__":
    mf_mode(on_production=False)
