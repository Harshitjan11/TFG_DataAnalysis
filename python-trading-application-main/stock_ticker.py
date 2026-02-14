import yfinance as yf
import mysql.connector
import pandas as pd
from datetime import datetime

# Read CSV
df = pd.read_csv('stocks copy.csv')

# MySQL Connection
connection = mysql.connector.connect(
    host="host.docker.internal",
    user="root",
    password="rootmysecretpassword",
    database="stock_report"
)

if connection.is_connected():
    print("Connected to MySQL database")

cursor = connection.cursor()

# Create a new table to store daily stock data (combined for all stocks)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_stock_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        ticker VARCHAR(10),
        stock_id INT,
        UNIQUE KEY unique_date_ticker (date, ticker)
    )
""")
print("Created 'daily_stock_data' table if it didn't exist.")

# Loop through each row in the CSV file
for index, row in df.iterrows():
    stock = row['Ticker'] + ".NS"
    stock_code = stock.replace(".", "_").upper()  # Normalize stock code for DB usage

    # Query to check if the stock exists in the database
    cursor.execute("SELECT * FROM stock WHERE stock_code = %s", (stock,))
    rows = cursor.fetchall()

    if rows:
        print(f"Stock {stock} already exists in the database, skipping insertion.")
        for row in rows:
            stock_id = row[0]  # Assuming the first column is the stock_id
    else:
        print(f"No data found for the stock code: {stock}. Inserting new record.")
        cursor.execute("INSERT INTO stock (stock_code, stock_name) VALUES (%s, %s)", (stock, stock))
        connection.commit()
        stock_id = cursor.lastrowid
        print(f"Inserted new stock: {stock}")

    # Fetch stock data using yfinance
    data = yf.download(
        tickers=stock,
        interval="1d",
        start="1900-01-01",
        end=datetime.now().date(),
        progress=False
    )

    if data.empty:
        print(f"No data returned for {stock}, skipping...")
        continue  # Skip to next stock if no data is fetched

    data = data.dropna()  # Drop rows with missing data
    data.columns = ['_'.join(col).strip() for col in data.columns.values]  # Flatten multi-level columns
    data.reset_index(inplace=True)

    # Insert stock data into the consolidated 'daily_stock_data' table
    for _, data_row in data.iterrows():
        insert_query = """
            INSERT INTO daily_stock_data (date, open, high, low, close, ticker, stock_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            data_row['Date'].date(),
            data_row[f'Open_{stock}'],
            data_row[f'High_{stock}'],
            data_row[f'Low_{stock}'],
            data_row[f'Close_{stock}'],
            stock,
            stock_id
        ))

    print(f"Data for {stock} inserted into 'daily_stock_data' successfully.")

# Commit all changes
connection.commit()

# Close the connection
cursor.close()
connection.close()

print("Data insertion completed for all stocks.")
