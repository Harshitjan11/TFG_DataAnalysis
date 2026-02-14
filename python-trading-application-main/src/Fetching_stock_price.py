import yfinance as yf
from datetime import datetime
import mysql.connector

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Password@123",
    database="trading_bot"
)
cursor = connection.cursor(dictionary=True)
cursor.execute("SELECT * FROM stock")
stocks = cursor.fetchall()

for stock in stocks:
    stock_id = stock['id']
    stock_code = stock['stock_code']

    print(f"Fetching data for {stock_code}...")

    data = yf.download(
        tickers=stock_code,
        interval="1d",
        start="1900-01-01",
        end=datetime.now().date(),
        progress=False
    )

    for date, row in data.iterrows():
        insert_query = """
                       INSERT IGNORE INTO daily (date, open, high, low, close, volume ,stock_id)
                       VALUES (%s, %s, %s, %s, %s,%s, %s)
                       """
        cursor.execute(insert_query, (
            date.date(),
            float(row['Open'].iloc[0]),
            float(row['High'].iloc[0]),
            float(row['Low'].iloc[0]),
            float(row['Close'].iloc[0]),
            row['Volume'].iloc[0],
            stock_id
        ))

    connection.commit()
    print(f"Inserted data for {stock_code}.")

cursor.close()
connection.close()
print("All data inserted successfully.")
