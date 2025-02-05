import os
import re
import time
import threading
import numpy as np
import pandas as pd
from datetime import datetime

from binance import ThreadedDepthCacheManager

def main():
    
    # start is required to initialise its internal loop
    dcm.start()

    def handle_depth_cache(depth_cache):
        # global start_engine_successful
        # if not start_engine_successful:
        #     start_engine_successful = True
        #     print("start engine successfully.")

        symbol = depth_cache.symbol
        market = depth_cache.market
        market_symbol = f"{market}_{symbol.lower()}"
        config = configs[market_symbol]

        # check_save_interval
        if config["ws_interval"] == config["save_interval"]:
            pass
        elif dataframes[market_symbol].empty:
            pass
        else:
            this_timestamp = depth_cache.update_time
            last_timestamp = dataframes[market_symbol].index[-1]
            if this_timestamp - last_timestamp < 0.95*config["save_interval"]:
                return

        saved_depth = config["saved_depth"]
        price_decimal_digits = config["price_decimal_digits"]
        volume_decimal_digits = config["volume_decimal_digits"]
        
        # dt_time = datetime.utcfromtimestamp(depth_cache.update_time/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
        delay = str(time.time() * 1000 - depth_cache.update_time)[:10]
        print("\rdelay(ms): ", delay, "len(dataframe):", len(dataframes[market_symbol]), end=' ')

        bids = depth_cache.get_bids()[:saved_depth]
        asks = depth_cache.get_asks()[:saved_depth]

        if len(bids) < saved_depth:
            for i in range(saved_depth - len(bids)):
                bids.append([0,0])
        
        if len(asks) < saved_depth:
            for i in range(saved_depth - len(asks)):
                asks.append([0,0])

        for i in range(saved_depth):
            bids[i][0] = f"{bids[i][0]:.{price_decimal_digits}f}"
            bids[i][1] = f"{float(bids[i][1]):.{volume_decimal_digits}f}"
            asks[i][0] = f"{asks[i][0]:.{price_decimal_digits}f}"
            asks[i][1] = f"{float(asks[i][1]):.{volume_decimal_digits}f}"
        
        # print(asks, bids)
        
        bids_price, bids_qty = np.array(bids).T.tolist()
        asks_price, asks_qty = np.array(asks).T.tolist()

        with lock:
            dataframes[market_symbol].loc[depth_cache.update_time] = bids_price + asks_price + bids_qty + asks_qty
        

    
    for market_symbol, config in configs.items():
        market, symbol = market_symbol.split('_')
        if market == "spot":
            dcm_pool[market_symbol] = dcm.start_depth_cache(
                handle_depth_cache, 
                symbol=symbol.upper(), 
                limit=config["rest_depth"], 
                ws_interval=config["ws_interval"], 
                )
        elif market == "futures":
            dcm_pool[market_symbol] = dcm.start_futures_depth_cache(
                handle_depth_cache, 
                symbol=symbol.upper(), 
                limit=config["rest_depth"], 
                ws_interval=config["ws_interval"], 
                )

    
    dcm.join()

def check_and_save_data():
    while True:
        to_renew = []
        for symbol, dataframe in dataframes.items():

            # if not start_engine_successful:
            #     if time.time() - server_start_time > 10:
            #         print("start engine failed.")
                    
            #         for dcm_name in dcm_pool.values():
            #             dcm.stop_socket(dcm_name)
            #         dcm.stop()

            #         os._exit(1)

            if len(dataframe) > 300:
                    start_time = dataframe.index[0]
                    end_time = dataframe.index[-1]

                    start_time_dt = datetime.utcfromtimestamp(start_time/1000).strftime('%Y_%m_%d_%H_%M_%S')
                    end_time_dt = datetime.utcfromtimestamp(end_time/1000).strftime('%Y_%m_%d_%H_%M_%S')

                    # start_time = re.sub(r'[^a-zA-Z0-9]', '_', start_time)
                    # end_time = re.sub(r'[^a-zA-Z0-9]', '_', end_time)

                    date = datetime.utcfromtimestamp(start_time/1000).strftime('%Y%m%d')
                    if not os.path.exists(f"data/{symbol}/{date}"):
                        os.makedirs(f"data/{symbol}/{date}")

                    file_name = f"data/{symbol}/{date}/{symbol}-{start_time_dt}-{end_time_dt}.csv"
                    dataframe.to_csv(file_name)
                    to_renew.append(symbol)
                    print(f"Saved: {file_name}.")

        
        for symbol in to_renew:
            config = configs[symbol]
            saved_depth = config["saved_depth"]
            columns = ["timestamp"] + \
            [f"bid_{i+1}_price" for i in range(saved_depth)] + \
            [f"ask_{i+1}_price" for i in range(saved_depth)] + \
            [f"bid_{i+1}_qty" for i in range(saved_depth)] + \
            [f"ask_{i+1}_qty" for i in range(saved_depth)]

            new_table = pd.DataFrame(columns=columns)
            new_table.set_index("timestamp", inplace=True)
            with lock:
                dataframes[symbol] = new_table

if __name__ == "__main__":
    # start_engine_successful = False
    # server_start_time = time.time()

    configs = {
        # btc
        # "spot_btcusdt":{
        #     "rest_depth": 2000,
        #     "ws_interval": 1000, # 100, 1000
        #     "save_interval": 1000,
        #     "saved_depth": 100,
        #     "price_decimal_digits": 2,
        #     "volume_decimal_digits": 5
        # },
        # "futures_btcusdt":{
        #     "rest_depth": 1000,
        #     "ws_interval": 500, # 100, 250, 500
        #     "save_interval": 1000,
        #     "saved_depth": 10,
        #     "price_decimal_digits": 1,
        #     "volume_decimal_digits": 3
        # },
        # eth
        # "spot_ethusdt":{
        #     "rest_depth": 2000,
        #     "ws_interval": 1000, # 100, 1000
        #     "save_interval": 1000,
        #     "saved_depth": 100,
        #     "price_decimal_digits": 2,
        #     "volume_decimal_digits": 4
        # },
        # "futures_ethusdt":{
        #     "rest_depth": 1000,
        #     "ws_interval": 500, # 100, 250, 500
        #     "save_interval": 1000,
        #     "saved_depth": 10,
        #     "price_decimal_digits": 2,
        #     "volume_decimal_digits": 3
        # },
        # bnb
        "spot_bnbusdt":{
            "rest_depth": 2000,
            "ws_interval": 1000, # 100, 1000
            "save_interval": 1000,
            "saved_depth": 50,
            "price_decimal_digits": 2,
            "volume_decimal_digits": 3
        },
        # "futures_bnbusdt":{
        #     "rest_depth": 1000,
        #     "ws_interval": 500, # 100, 250, 500
        #     "save_interval": 1000,
        #     "saved_depth": 10,
        #     "price_decimal_digits": 2,
        #     "volume_decimal_digits": 2
        # },
        # small
        # "spot_trumpusdt":{
        #     "rest_depth": 1000,
        #     "ws_interval": 1000, # 100, 1000
        #     "save_interval": 1000,
        #     "saved_depth": 50,
        #     "price_decimal_digits": 2,
        #     "volume_decimal_digits": 3
        # },
        # "spot_dogeusdt":{
        #     "rest_depth": 1000,
        #     "ws_interval": 1000, # 100, 1000
        #     "save_interval": 1000,
        #     "saved_depth": 50,
        #     "price_decimal_digits": 5,
        #     "volume_decimal_digits": 0
        # },
        # "spot_shibusdt":{
        #     "rest_depth": 1000,
        #     "ws_interval": 1000, # 100, 1000
        #     "save_interval": 1000,
        #     "saved_depth": 50,
        #     "price_decimal_digits": 8,
        #     "volume_decimal_digits": 0
        # },
        # "spot_pepeusdt":{
        #     "rest_depth": 1000,
        #     "ws_interval": 1000, # 100, 1000
        #     "save_interval": 1000,
        #     "saved_depth": 50,
        #     "price_decimal_digits": 8,
        #     "volume_decimal_digits": 0
        # },
        "spot_xvsusdt":{
            "rest_depth": 1000,
            "ws_interval": 1000, # 100, 1000
            "save_interval": 1000,
            "saved_depth": 50,
            "price_decimal_digits": 2,
            "volume_decimal_digits": 2
        },
        "spot_cakeusdt":{
            "rest_depth": 1000,
            "ws_interval": 1000, # 100, 1000
            "save_interval": 1000,
            "saved_depth": 50,
            "price_decimal_digits": 3,
            "volume_decimal_digits": 2
        },
        "spot_1mbabydogeusdt":{
            "rest_depth": 1000,
            "ws_interval": 1000, # 100, 1000
            "save_interval": 1000,
            "saved_depth": 50,
            "price_decimal_digits": 7,
            "volume_decimal_digits": 0
        },
        "spot_biousdt":{
            "rest_depth": 1000,
            "ws_interval": 1000, # 100, 1000
            "save_interval": 1000,
            "saved_depth": 50,
            "price_decimal_digits": 4,
            "volume_decimal_digits": 1
        },
        "futures_kasusdt":{
            "rest_depth": 1000,
            "ws_interval": 500, # 100, 1000
            "save_interval": 1000,
            "saved_depth": 50,
            "price_decimal_digits": 5,
            "volume_decimal_digits": 0
        }
    }

    # init
    dataframes = {}
    for symbol, config in configs.items():
        if not os.path.exists(f"data/{symbol}"):
            os.makedirs(f"data/{symbol}")

        saved_depth = config["saved_depth"]
        columns = ["timestamp"] + \
        [f"bid_{i+1}_price" for i in range(saved_depth)] + \
        [f"ask_{i+1}_price" for i in range(saved_depth)] + \
        [f"bid_{i+1}_qty" for i in range(saved_depth)] + \
        [f"ask_{i+1}_qty" for i in range(saved_depth)]

        dataframe = pd.DataFrame(columns=columns)
        dataframe.set_index("timestamp", inplace=True)

        dataframes[symbol] = dataframe

    lock = threading.Lock()

    dcm = ThreadedDepthCacheManager()
    dcm_pool = {}

    # 保存文件的线程
    dump_thread = threading.Thread(target=check_and_save_data, daemon=True)
    dump_thread.start()

    main()

    