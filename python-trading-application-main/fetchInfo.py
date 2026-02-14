import yfinance as yf
import mysql.connector
import pandas as pd
from datetime import datetime

# MySQL Connection
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",  
    database="stock_report"
)

if connection.is_connected():
    print("Connected to MySQL database")

cursor = connection.cursor()

# Define the stock code to fetch data for
stock = "CARTRADE.NS"
stock_code = stock.replace(".", "_").upper()

cursor.execute("SELECT * FROM stock WHERE stock_code=%s", (stock,))
rows = cursor.fetchall()

if rows:
    for row in rows:
        stock_id = row[0]
else:
    print(f"No data found for the stock code: {stock}")
    connection.close()  
    exit() 

# yesterday = datetime.now() - timedelta(days=1)
# start_time = datetime(yesterday.year, yesterday.month, yesterday.day, 9, 15)
# end_time = datetime.now()


# data = yf.download(
#     tickers=stock,
#     interval="1d",
#     start=start_time,
#     end=end_time,
#     progress=False
# )

data = yf.download(
    tickers=stock,
    interval="1d",
    start="1900-01-01",  # This ensures data starts from the listing date
    end=datetime.now().date(),
    progress=False
)


data = data.dropna()
data.columns = ['_'.join(col).strip() for col in data.columns.values]
data.reset_index(inplace=True)
print(data.columns)


table_name = f"{stock_code}_DAILY"

cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        stock_id INT,
        UNIQUE KEY unique_date_stock (date, stock_id)
    )
""")


for index, row in data.iterrows():
    insert_query = f"""
        INSERT INTO {table_name} (date, open, high, low, close, stock_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    
    cursor.execute(insert_query, (
        row['Date'].date(),  
        row['Open_' + stock],  
        row['High_' + stock],  
        row['Low_' + stock],  
        row['Close_' + stock],  
        int(stock_id) 
    ))


connection.commit()
cursor.close()
connection.close()

print("Data inserted successfully.")
