import csv
import sys
import time

from dateutil import parser

from ftx_client import FtxClient

year_in_seconds = 31556926
fifteen_days_in_seconds = 1296000
ten_days_in_seconds = 864000
one_day_in_seconds = 86400
six_hours_in_seconds = 21600

RESOLUTION = {"15s": 15, "1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}


#oryginalna metody piotrka
def get_historical_candles(symbol, resolution, iterations):
    ftx_client = FtxClient()

    end_time = time.time()
    start_time = end_time - one_day_in_seconds
    number_of_days = iterations

    # keys = ['close', 'high', 'low', 'open', 'startDate', 'startTime', 'volume', 'time']
    keys = ['close', 'high', 'low', 'open', 'startDate', 'startTime']
    file_name = symbol + "-" + list(RESOLUTION.keys())[list(RESOLUTION.values()).index(resolution)] + str(number_of_days) +" dni "+ ".csv"
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

#do danych 1 minutowych i wyżej
def get_his_candles_wj (symbol, resolution, iterations):
    ftx_client = FtxClient()

    end_time = time.time()
    start_time = end_time - one_day_in_seconds
    number_of_days = iterations

    keys = ['open', 'high', 'low', 'close','volume', 'startDate', 'startTime']
    file_name = symbol + "-" + list(RESOLUTION.keys())[list(RESOLUTION.values()).index(resolution)]+" " + str(number_of_days) +" dni "+ ".csv"
    file = open(file_name, 'w', newline='')
    dict_writer = csv.DictWriter(file, keys)
    dict_writer.writeheader()
    print("before for ")
    #pobiera 1500 rekordów a na minutówce mamy 1440 rekordów więc pobierze 1440 rekordów i przejdzie do następnego dnia i znowu pobierzez 1440 rekordów
    #w przypadku 15 sekundóek pobierze 1500 a to nie jest całość więc powinien pobrać co
    #15s  = 5760 rekordów
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
            #print(d)
        dict_writer.writerows(new_list)
        #print(new_list)
        end_time -= one_day_in_seconds
        start_time = end_time - one_day_in_seconds

    file.close()
    print("finished")

#do danych 15s
def get_15s_candles_wj (symbol, resolution, iterations):
    ftx_client = FtxClient()
    days = iterations
    iterations = iterations*4
    end_time = time.time()
    start_time = end_time - six_hours_in_seconds
    number_of_days = iterations

    keys = ['open', 'high', 'low', 'close','volume', 'startDate', 'startTime']
    file_name = symbol + "-" + list(RESOLUTION.keys())[list(RESOLUTION.values()).index(resolution)]+" " + str(days) +" dni v15s"+ ".csv"
    file = open(file_name, 'w', newline='')
    dict_writer = csv.DictWriter(file, keys)
    dict_writer.writeheader()
    print("before for ")
    #pobiera 1500 rekordów a na minutówce mamy 1440 rekordów więc pobierze 1440 rekordów i przejdzie do następnego dnia i znowu pobierzez 1440 rekordów
    #w przypadku 15 sekundóek pobierze 1500 a to nie jest całość więc powinien pobrać co
    #15s  = 5760 rekordów
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
            #print(d)
        dict_writer.writerows(new_list)
        #print(new_list)
        end_time -= six_hours_in_seconds
        start_time = end_time - six_hours_in_seconds

    file.close()
    print("finished")

#do danych 1h tylko z fundingiem
def get_1h_candles_with_funding_wj (symbol, iterations):
    ftx_client = FtxClient()

    end_time = time.time()
    start_time = end_time - one_day_in_seconds

    keys = ['open', 'high', 'low', 'close','volume', 'startDate', 'startTime','rate']
    file_name = symbol + "-" + " 1h "+" " + str(iterations) +" dni funding"+ ".csv"
    file = open(file_name, 'w', newline='')
    dict_writer = csv.DictWriter(file, keys)
    dict_writer.writeheader()
    print("before for ")
    #response_funding = ftx_client.get_all_funding_rates(start_time, end_time, symbol)
    #response_funding.reverse()
    #print(response_funding)

    for x in range(iterations):
        response = ftx_client.get_historical_prices(symbol, 3600, start_time, end_time)
        response.reverse()
        response_funding= ftx_client.get_all_funding_rates(start_time, end_time, symbol)
        #response_funding.reverse()
        print(response_funding)

        new_list = []
        #for (r, f) in itertools.zip_longest(response, response_funding):
        #for (r, f) in map(None, response, response_funding):
        for (r, f) in zip(response, response_funding):
        #for (r, f) in izip(response, response_funding):
        #for r in response:

            d = dict()
            d['open'] = r['open']
            d['high'] = r['high']
            d['low'] = r['low']
            d['close'] = r['close']
            d['volume'] = r['volume']
            dt = parser.parse(r['startTime'])
            d['startDate'] = dt.strftime("%Y/%m/%d")
            d['startTime'] = dt.strftime("%H:%M:%S")
            d['rate'] = f['rate']
            new_list.append(d)
            #print(d)
        dict_writer.writerows(new_list)
        print(new_list)
        end_time -= one_day_in_seconds
        start_time = end_time - one_day_in_seconds

    file.close()
    print("finished")



#do danych 15s
def return_list_from_file (file_name):
    #file_name = "ETH-PERP-1h 1 dni v15s.csv"
    file = open(file_name, 'r')
    list_as_file = []
    for x in file:
        list_as_file.append(x)
    #print(list_as_file)
    file.close()
    return list_as_file

#zwraca dicta z key and values
def create_new_data_list(symbol,resolution, iterations):
    ftx_client = FtxClient()
    #iterations = iterations * 4
    end_time = time.time()
    start_time = end_time - six_hours_in_seconds
    ftx_client = FtxClient()
    new_list = []
    for x in range(iterations):
        response = ftx_client.get_historical_prices(symbol, resolution, start_time, end_time)
        response.reverse()
        local_list = []
        print(response)
        print("nowy response")
        for r in response:
            local_list.append(r['open'])
            local_list.append(r['high'])
            local_list.append(r['low'])
            local_list.append(r['close'])
            local_list.append(r['high'])
            local_list.append(r['volume'])
            dt = parser.parse(r['startTime'])
            local_list.append(dt.strftime("%Y/%m/%d"))
            local_list.append(dt.strftime("%H:%M:%S"))

            new_list.append(local_list)
        end_time -= six_hours_in_seconds
        start_time = end_time - six_hours_in_seconds

    return new_list



def get_15s_candles_wj_overriden (symbol, resolution, iterations, file_name):
    list_of_new_data = create_new_data_list(symbol, resolution, iterations)
    list_from_existing_file = return_list_from_file("ETH-PERP-1h 1 dni v15s.csv")



def remove_common(a, b):
    for i in a[:]:
        if i in b:
            a.remove(i)
            b.remove(i)

    #for x in a:
        #print (x)
    #for x in b:
        #print(x)


if __name__ == '__main__':
    #get_15s_candles_wj_overriden("ETH-PERP", 3600, 1)
    #return_list_from_file("ETH-PERP-1h 1 dni v15s.csv")
    list_from_existing_file = return_list_from_file("ETH-PERP-1h 1 dni v15s.csv")
    list_of_new_data = create_new_data_list("ETH-PERP", 3600, 1)


    #print(list_from_existing_file[1])

    #for x in list_of_new_data:
        #print(x)

    #print(list_of_new_data)


    #print(len(list_from_existing_file))
    #print(len(list_of_new_data))

    #remove_common (list_from_existing_file, list_of_new_data )





    li = [1,2,3,4,5,6]
    li2 = [4,5,6,7,8,9,10]
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
