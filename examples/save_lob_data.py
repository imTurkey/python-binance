import re
import time
import threading
import numpy as np
import pandas as pd
from datetime import datetime

from binance import ThreadedDepthCacheManager
def main():
    
    dcm = ThreadedDepthCacheManager(testnet=True)
    # start is required to initialise its internal loop
    dcm.start()

    def handle_depth_cache(depth_cache):
        dt_time = datetime.fromtimestamp(depth_cache.update_time/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
        bids = depth_cache.get_bids()[:saved_depth]
        asks = depth_cache.get_asks()[:saved_depth]

        bids = np.array(bids).T.flatten().tolist()
        asks = np.array(asks).T.flatten().tolist()

        with lock:
            dataframes[depth_cache.symbol].loc[dt_time] = bids + asks
        

    dcm_pool = {}
    for symbol in symbols:
        dcm_pool[symbol] = dcm.start_depth_cache(handle_depth_cache, symbol=symbol, limit=5000, ws_interval=spot_interval)
        # dcm_futures_pool[symbol] = dcm.start_futures_depth_cache(handle_depth_cache, symbol=symbol, limit=1000, ws_interval=futures_interval)
    
    time.sleep(30)

    for dcm_name in dcm_pool.values():
        dcm.stop_socket(dcm_name)

    dcm.stop()
    
    # dcm.join()

def check_and_save_data():
    while True:
        to_renew = []
        for symbol, dataframe in dataframes.items():
            if len(dataframe) > 1000:
                    start_time = dataframe.index[0].split('.')[0]
                    end_time = dataframe.index[-1].split('.')[0]

                    start_time = re.sub(r'[^a-zA-Z0-9]', '_', start_time)
                    end_time = re.sub(r'[^a-zA-Z0-9]', '_', end_time)

                    dataframe.to_csv(f"{symbol}-{start_time}-{end_time}.csv")
                    to_renew.append(symbol)

        
        for symbol in to_renew:
            new_table = pd.DataFrame(columns=columns)
            new_table.set_index("time", inplace=True)
            with lock:
                dataframes[symbol] = new_table

if __name__ == "__main__":
    symbols = ["BTCUSDT"]
    spot_interval = 100 # 100, 1000
    futures_interval = 100 # 100, 250, 500

    saved_depth = 20

    columns = ["time"] + \
    [f"bid_{i+1}_price" for i in range(saved_depth)] + \
    [f"bid_{i+1}_qty" for i in range(saved_depth)] + \
    [f"ask_{i+1}_price" for i in range(saved_depth)] + \
    [f"ask_{i+1}_qty" for i in range(saved_depth)]

    dataframes = {symbol: pd.DataFrame(columns=columns) for symbol in symbols}

    for dataframe in dataframes.values():
        dataframe.set_index("time", inplace=True)

    lock = threading.Lock()

    # 保存文件的线程，每1000行保存一次
    dump_thread = threading.Thread(target=check_and_save_data, daemon=True)
    dump_thread.start()

    main()

    