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
        global start_engine_successful
        if not start_engine_successful:
            start_engine_successful = True
            print("start engine successfully.")

        symbol = depth_cache.symbol
        saved_depth = configs[symbol]["saved_depth"]
        price_decimal_digits = config["price_decimal_digits"]
        volume_decimal_digits = config["volume_decimal_digits"]
        
        # dt_time = datetime.utcfromtimestamp(depth_cache.update_time/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
        delay = str(time.time() * 1000 - depth_cache.update_time)[:10]
        print("\rdelay(ms): ", delay, "len(dataframe):", len(dataframes[symbol]), end=' ')

        bids = depth_cache.get_bids()[:saved_depth]
        asks = depth_cache.get_asks()[:saved_depth]

        for i in range(saved_depth):
            bids[i][0] = f"{bids[i][0]:.{price_decimal_digits}f}"
            bids[i][1] = f"{float(bids[i][1]):.{volume_decimal_digits}f}"
            asks[i][0] = f"{asks[i][0]:.{price_decimal_digits}f}"
            asks[i][1] = f"{float(asks[i][1]):.{volume_decimal_digits}f}"
        
        # print(asks, bids)
        
        bids_price, bids_qty = np.array(bids).T.tolist()
        asks_price, asks_qty = np.array(asks).T.tolist()

        with lock:
            dataframes[symbol].loc[depth_cache.update_time] = bids_price[::-1] + asks_price + bids_qty[::-1] + asks_qty
        

    
    for symbol, config in configs.items():
        dcm_pool[symbol] = dcm.start_depth_cache(
            handle_depth_cache, 
            symbol=symbol, 
            limit=config["rest_depth"], 
            ws_interval=config["ws_interval"], 
            )
    
    dcm.join()

def check_and_save_data():
    while True:
        to_renew = []
        for symbol, dataframe in dataframes.items():

            if not start_engine_successful:
                if time.time() - server_start_time > 10:
                    print("start engine failed.")
                    
                    for dcm_name in dcm_pool.values():
                        dcm.stop_socket(dcm_name)
                    dcm.stop()

                    os._exit(1)

            if len(dataframe) > 300:
                    start_time = dataframe.index[0]
                    end_time = dataframe.index[-1]

                    start_time_dt = datetime.utcfromtimestamp(start_time/1000).strftime('%Y_%m_%d_%H_%M_%S')
                    end_time_dt = datetime.utcfromtimestamp(end_time/1000).strftime('%Y_%m_%d_%H_%M_%S')

                    # start_time = re.sub(r'[^a-zA-Z0-9]', '_', start_time)
                    # end_time = re.sub(r'[^a-zA-Z0-9]', '_', end_time)

                    file_name = f"data/{symbol}SPOT-{symbol}-{start_time_dt}-{end_time_dt}.csv"
                    dataframe.to_csv(file_name)
                    to_renew.append(symbol)
                    print(f"Saved: {file_name}.")

        
        for symbol in to_renew:
            new_table = pd.DataFrame(columns=columns)
            new_table.set_index("timestamp", inplace=True)
            with lock:
                dataframes[symbol] = new_table

if __name__ == "__main__":
    start_engine_successful = False
    server_start_time = time.time()

    configs = {
        "BTCUSDT":{
            "rest_depth": 2000,
            "ws_interval": 1000, # 100, 1000
            "saved_depth": 100,
            "price_decimal_digits": 2,
            "volume_decimal_digits": 5
        }
    }

    # init
    dataframes = {}
    for symbol, config in configs.items():
        if not os.path.exists(f"data/{symbol}"):
            os.makedirs(f"data/{symbol}")

        saved_depth = config["saved_depth"]
        columns = ["timestamp"] + \
        [f"bid_{saved_depth-i}_price" for i in range(saved_depth)] + \
        [f"ask_{i+1}_price" for i in range(saved_depth)] + \
        [f"bid_{saved_depth-i}_qty" for i in range(saved_depth)] + \
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

    