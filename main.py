import csv
import sys
import time

from dateutil import parser

from ftx_client import FtxClient

year_in_seconds = 31556926
fifteen_days_in_seconds = 1296000
ten_days_in_seconds = 864000
one_day_in_seconds = 86400

RESOLUTION = {"1m": 60, "5m": 300, "1h": 3600, "1d": 86400}


def get_funding_rates(iterations):
    ftxClient = FtxClient()

    end_time = time.time()
    start_time = end_time - one_day_in_seconds

    keys = ['future', 'rate', 'time']

    file = open('ftx-funding-rates.csv', 'w', newline='')
    dict_writer = csv.DictWriter(file, keys)
    dict_writer.writeheader()

    for x in range(iterations):
        funding_rates = ftxClient.get_all_funding_rates(start_time, end_time, 'BTC-PERP')

        dict_writer.writerows(funding_rates)

        end_time -= one_day_in_seconds
        start_time = end_time - one_day_in_seconds

    file.close()


def get_historical_candles(symbol, resolution, iterations):
    ftx_client = FtxClient()

    end_time = time.time()
    start_time = end_time - one_day_in_seconds

    # keys = ['close', 'high', 'low', 'open', 'startDate', 'startTime', 'volume', 'time']
    keys = ['close', 'high', 'low', 'open', 'startDate', 'startTime']
    file_name = symbol + "-" + list(RESOLUTION.keys())[list(RESOLUTION.values()).index(resolution)] + ".csv"
    file = open(file_name, 'w', newline='')
    dict_writer = csv.DictWriter(file, keys)
    dict_writer.writeheader()
    print("before for ")
    for x in range(iterations):
        response = ftx_client.get_historical_future_candles(symbol, resolution, start_time, end_time)
        response.reverse()

        new_list = []

        for r in response:
            d = dict()
            d['close'] = r['close']
            d['high'] = r['high']
            d['low'] = r['low']
            d['open'] = r['open']
            dt = parser.parse(r['startTime'])
            d['startDate'] = dt.strftime("%Y/%m/%d")
            d['startTime'] = dt.strftime("%H:%M:%S")
            new_list.append(d)
            print(d)
        dict_writer.writerows(new_list)
        #print(new_list)
        end_time -= one_day_in_seconds
        start_time = end_time - one_day_in_seconds

    file.close()
    print("finished")

def get_his_candles_wj (symbol, resolution, iterations):
    ftx_client = FtxClient()

    end_time = time.time()
    start_time = end_time - one_day_in_seconds

    keys = ['open', 'high', 'low', 'close','volume', 'startDate', 'startTime']
    file_name = symbol + "-" + list(RESOLUTION.keys())[list(RESOLUTION.values()).index(resolution)] + ".csv"
    file = open(file_name, 'w', newline='')
    dict_writer = csv.DictWriter(file, keys)
    dict_writer.writeheader()
    print("before for ")
    for x in range(iterations):
        response = ftx_client.get_historical_prices(symbol, resolution, start_time, end_time)
        response.reverse()

        new_list = []

        for r in response:
            d = dict()
            d['open'] = r['open']
            d['high'] = r['high']
            d['low'] = r['low']
            d['close'] = r['close']
            d['volume'] = r['volume']
            dt = parser.parse(r['startTime'])
            d['startDate'] = dt.strftime("%Y/%m/%d")
            d['startTime'] = dt.strftime("%H:%M:%S")
            new_list.append(d)
            print(d)
        dict_writer.writerows(new_list)
        #print(new_list)
        end_time -= one_day_in_seconds
        start_time = end_time - one_day_in_seconds

    file.close()
    print("finished")


if __name__ == '__main__':
    get_his_candles_wj("DOGE-PERP",60,2)


    #symbolArg = "BTC" # str(sys.argv[1])   #odczytuje btc jako arg 1
    #resolutionArg = RESOLUTION["5m"] # RESOLUTION[str(sys.argv[2])]
    #iterationsArg = 2 # int(sys.argv[3])

    #print("DOWNLOADING...")
    #get_historical_candles(symbolArg, resolutionArg, iterationsArg)
    #print("FINISHED")

    #ftx_client = FtxClient()
    #get_funding_rates(3)
    #listMy = ftx_client.get_all_futures()
    #orderBook = ftx_client.get_orderbook("1INCH-PERP",2)
    #print(orderBook)
    #print(orderBook["bids"])
    #print("hello guys")
