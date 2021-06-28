# Import libraries & declare constants
import requests, json, datetime
import pandas as pd
# from constants import *
credentials = {'Username': 'smoody', 'Password': 'Conrad1234!'}


def amend_order(id,price,volume,request_id='abc123',allornothing=False,text = "text1"):

    data = {
        "RequestId": request_id,
        "Id": id,
        "Price": price,
        "Volume": volume,
        "AllOrNothing": allornothing,
        "Text": text
    }
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}
    url = base_url + "/order/amend"
    response = requests.post(url, headers=headers, data=data)
    return json.loads(response.text)


def cancel_order(id,request_id='abc123'):

    data = {
        "RequestId": request_id,
        "Id": id
    }
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}
    url = base_url + "/order/cancel"
    response = requests.post(url, headers=headers, data=data)
    return json.loads(response.text)


def get_token(base_url):
    url = base_url+'/oauth/token'
    response = requests.post(url, data=credentials)
    token = json.loads(response.text)["Token"]
    return token

def post_new_order(side, price, volume, product, is_active, strategy, token,base_url,
                   request_id = "abc123", market = "EPEX", trader = 'Trader1',
                   ordertpe = "Limit",allornothing= False, text = "text1"):
    data = {
        "RequestId": request_id,
        "Market": market,
        "Product": product,
        "Trader": trader,
        "Strategy":strategy,
        "OrderType": ordertpe,
        "Side": side,
        "Price": price,
        "Volume": volume,
        "Active": is_active,
        "AllOrNothing": allornothing,
        "Text": text
    }
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}

    url = base_url + "/order/new"

    # if is_active:
    #     url = base_url + "/order/new" +"?active="+str(is_active)
    # else:
    #     url = base_url + "/order/new"

    response = requests.post(url, headers=headers, data=data)
    return json.loads(response.text)


def get_orders(status, product=None):

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
    return json.loads(response.text)


def get_trades(trade_date=None,trade_time = None,product=None, id=None, market = "EPEX",
               account=None,order_id = None, trader = None, strategy=None,
               side=None,price=None,volume=None, text = None):

    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}

    data = {
        "Id": id,
        "Market": market,
        "Account": account,
        "OrderId": order_id,
        "Product": product,
        "Trader": trader,
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
    return json.loads(response.text)


def cancelall():

    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}

    url = base_url + "/order/cancelall"

    response = requests.post(url, headers=headers)
    return json.loads(response.text)


def get_depth(base_url, token, product):
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}
    url = base_url+"/marketdata/depth"+"?product="+str(product)
    response = requests.get(url, headers=headers)
    return json.loads(response.text)

def get_lasttrade(product):
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + token}
    url = base_url+"/marketdata/lasttrade"+"?product="+str(product)
    response = requests.get(url, headers=headers)
    return json.loads(response.text)

def get_base_url(is_demo):
    if is_demo:
        return 'http://conrad-demo.iglootradingsolutions.com:8080/Igloo/IglooTrader'
    else:
        return "https://conrad-api.iglootradingsolutions.com:8443/igloo/iglootrader"

def print_orders(orders):

    df = pd.DataFrame(columns=["Volume", "Price"])
    for x in orders["Bids"]:
        s = pd.Series([x["Volume"], x["Price"]], index=df.columns)
        df = df.append(s, ignore_index=True)
    df.sort_values(by=["Price"], ascending=False, inplace=True)
    print("###### Bids ######")
    print(df);

    df = pd.DataFrame(columns=["Volume", "Price"])
    for x in orders["Offers"]:
        s = pd.Series([x["Volume"], x["Price"]], index=df.columns)
        df = df.append(s, ignore_index=True)
    df.sort_values(by=["Price"], ascending=True, inplace=True)
    print("###### Offers ######")
    print(df)

def mf_interact_with_m7_igloo():

    is_demo = False
    product = "UKP HH40 Tue"

    base_url = get_base_url(is_demo)
    token = get_token(base_url)

    # print(get_lasttrade(product))
    orders = get_depth(base_url, token, product)
    print(orders)
    for order in orders:
        print(orders["Offers"][0])
    _ = print_orders(orders)

if __name__ == "__main__":
    mf_interact_with_m7_igloo()