import ta
import pandas as pd
from sqlalchemy import create_engine

# Connect to the database
engine = create_engine("mysql+mysqlconnector://root:@localhost/stock_report")

# Stock ID for which you want to fetch the data
stock_id = 1

# Query to fetch date, close, high, and low prices
query = """
            SELECT date, open, high, low, close
            FROM daily
            WHERE stock_id = %s
            ORDER BY date ASC
        """

# Load the data into a pandas DataFrame
df = pd.read_sql(query, engine, params=(stock_id,))

# Make sure the 'date' column is in datetime format for proper indexing
df['date'] = pd.to_datetime(df['date'])

# Calculate ADX, +DI, and -DI using the ta library (corrected method)
df['ADX'] = ta.trend.adx(df['high'], df['low'], df['close'], window=14)
df['+DI'] = ta.trend.adx_pos(df['high'], df['low'], df['close'], window=14)
df['-DI'] = ta.trend.adx_neg(df['high'], df['low'], df['close'], window=14)

# Print the ADX, +DI, and -DI values
print(df[['date', 'ADX', '+DI', '-DI']])
