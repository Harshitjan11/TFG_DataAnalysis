from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

stocks = [f"STOCK{i}" for i in range(1, 51)]


def process_stock(stock_symbol):
    print(f"⏳ Processing {stock_symbol}")
    time.sleep(random.uniform(0.5, 2))
    print(f"✅ Finished {stock_symbol}")
    return stock_symbol

# Use ThreadPoolExecutor with 10 threads
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(process_stock, stock) for stock in stocks]


    for future in as_completed(futures):
        result = future.result()

        print(f"🎉 Result: {result}")
