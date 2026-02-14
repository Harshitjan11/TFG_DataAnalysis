import yfinance as yf
import mysql.connector
import pandas as pd
from datetime import datetime
import requests
from io import StringIO
import time

# Step 1: Download stock symbols from NSE
nse_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
response = requests.get(nse_url)
data = StringIO(response.text)
df_symbols = pd.read_csv(data)

print("Total stocks listed:", len(df_symbols))

symbols = df_symbols['SYMBOL'].tolist()
symbols = [sym + ".NS" for sym in symbols]

# Step 2: Connect to MySQL
connection = mysql.connector.connect(
    host="db",
    user="root",
    password="rootmysecretpassword",
    database="stock_report"
)

if connection.is_connected():
    print("Connected to MySQL database")

cursor = connection.cursor()

# Step 3: Ensure `stock` and `daily` tables exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_code VARCHAR(20) UNIQUE,
        stock_name VARCHAR(255)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_id INT,
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT,
        UNIQUE KEY unique_stock_date (stock_id, date),
        FOREIGN KEY (stock_id) REFERENCES stock(id)
    )
""")

# Step 4: Loop through all symbols
for original_symbol in symbols:
    stock = yf.Ticker(original_symbol)
    try:
        stock_info = stock.info
    except Exception as e:
        print(f"Failed to fetch info for {original_symbol}: {e}")
        continue

    stock_name = stock_info.get("longName") or original_symbol
    stock_code = original_symbol

    print(f"Processing {original_symbol}: {stock_name}")

    # Insert into stock table if not exists
    cursor.execute("SELECT id FROM stock WHERE stock_code = %s", (stock_code,))
    row = cursor.fetchone()
    if row:
        stock_id = row[0]
    else:
        cursor.execute(
            "INSERT INTO stock (stock_code, stock_name) VALUES (%s, %s)",
            (stock_code, stock_name)
        )
        connection.commit()
        stock_id = cursor.lastrowid
        print(f"Inserted new stock: {stock_code} with ID {stock_id}")


    try:
        print("Fetching data for " + stock_code)
        data = yf.download(
            tickers=stock_code,
            interval="1d",
            start="1900-01-01",
            end=datetime.now().date(),
            progress=False
        )
    except Exception as e:
        print(f"Error downloading data for {stock_code}: {e}")
        continue

    if data.empty:
        print(f"No data returned for {stock_code}. Skipping.")
        continue

    data = data.dropna()
    data.columns = ['_'.join(col).strip() for col in data.columns.values]
    data.reset_index(inplace=True)

    # Insert into daily table
    for _, row in data.iterrows():
        try:
            cursor.execute("""
                           SELECT COUNT(*)
                           FROM daily
                           WHERE stock_id = %s
                             AND date = %s
                           """, (stock_id, row['Date'].date()))
            result = cursor.fetchone()


            if result[0] == 0:
                cursor.execute("""
                               INSERT INTO daily
                                   (stock_id, date, open, high, low, close, volume)
                               VALUES (%s, %s, %s, %s, %s, %s, %s)
                               """, (
                                   stock_id,
                                   row['Date'].date(),
                                   row[f'Open_{stock_code}'],
                                   row[f'High_{stock_code}'],
                                   row[f'Low_{stock_code}'],
                                   row[f'Close_{stock_code}'],
                                   row[f'Volume_{stock_code}'],
                               ))
        except Exception as e:
            print(f"Error inserting daily data for {stock_code} on {row['Date']}: {e}")

    connection.commit()
    print(f"Finished processing {stock_code}\n")
    time.sleep(1)  # to avoid rate limits

# Cleanup
cursor.close()
connection.close()
print("All stock data inserted into the 'daily' table.")
