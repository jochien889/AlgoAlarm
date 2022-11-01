import requests
from datetime import datetime
import time
import boto3
import os
from lineNotify import lineMess
import pandas as pd

os.environ["threshold"] = 0.1

def fundRatePull(symbol, realTime = False, limit = 1):
    baseUrl = "https://fapi.binance.com"
    realTimeFundingRate = '/fapi/v1/premiumIndex'
    historyFundingRate = '/fapi/v1/fundingRate'
    params = {
        "symbol" : symbol,
        "limit" : limit
        } 
    fundingRatePara = '&'.join(f"{key}={params[key]}" for key in params.keys())
    bodyURL = realTimeFundingRate if realTime else historyFundingRate
    finalURL = baseUrl + bodyURL + '?' + fundingRatePara
    # print("[finalURL]: ", finalURL)
    fundInfo = requests.get(finalURL).json()
    return fundInfo

dynamodb = boto3.resource(
    'dynamodb'
)
ExchangeInfo = dynamodb.Table('USD_Future_ExchangeInfo').scan()['Items']
symbols = [i['symbol'] for i in ExchangeInfo if i['status'] != 'SETTLING']

result = {}
for symbol in symbols:
    fundInfo = fundRatePull(symbol, realTime = True)
    fundInfo = fundInfo if type(fundInfo) == list else [fundInfo]
    if fundInfo:
        result[fundInfo[0]['symbol']] = round(float(fundInfo[0]['lastFundingRate']), 8)

detail = {k:round(abs(v) * 100, 6)  for k,v in result.items()}
print("[DETAIL]: ", detail)


result = {k:round(v * 100, 6) for k,v in result.items() if abs(v) * 100 > float(os.environ["threshold"])}
print("[RESULT]: ", result)
if result:
    mess = ''
    for k, v in result.items():
        tmpMess = "\n{}: {}".format(k, v)
        mess += tmpMess
    
    lineMess(os.environ["lineAuthorization"], mess)

# google sheet api
gsheetConfig = {
    '00:00:00':'B1', 
    '08:00:00':'C1', 
    '16:00:00':'D1'
    }
gc = pygsheets.authorize(service_file='credentials.json')
sht = gc.open_by_url('https://docs.google.com/spreadsheets/d/1LRBWkuPrHsC2_QQYNaY2vzT8_5Sjtf6uZQ-YrM3uC_o')
df = pd.DataFrame(wks.get_all_records())
wks = sht[0]

#C1
colSymbol = ['Symbol']
symbols = [k for k, v in result.items()]
symbols = col + symbols
symbols

#C2
tmpdf = pd.DataFrame()
colDateTime = datetime.fromtimestamp(int(fundInfo['nextFundingTime']), tz=tw).strftime("%H:%M:%S")
tmpdf['Symbol'] = [k for k, v in result.items()]
tmpdf[colDateTime] = [v for k, v in result.items()]

insertFund = [[i] for i in tmpdf[colDateTime]]
wks.update_values(gsheetConfig[colDateTime],insertFund)