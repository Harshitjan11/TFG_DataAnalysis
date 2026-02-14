import pandas as pd
import numpy as np
import mysql.connector

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Password@123",
    database="trading_bot"
)

# Fetch data
query = """
SELECT date, open, high, low, close, 
       sma_21, sma_50, adx, plus_di, minus_di, rsi, high_100
FROM daily 
WHERE stock_id = 1 
ORDER BY date
"""
df = pd.read_sql(query, conn)

# Preprocess
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(by='date').reset_index(drop=True)

# Strategy Conditions
df['startCondition'] = (
    (df['adx'] > 20) &
    (df['plus_di'] > df['minus_di']) &
    (df['sma_21'] > df['sma_50']) &
    (df['close'] > df['sma_21']) &
    (df['rsi'] > 50)
)

df['endCondition'] = (
    (df['close'] < df['sma_21']) |
    (df['close'] < 0.9 * df['high_100']) |
    (df['minus_di'] > df['plus_di'])
)

# Track position
in_position = False
positions = []

for idx, row in df.iterrows():
    if not in_position and row['startCondition']:
        in_position = True
    elif in_position and row['endCondition']:
        in_position = False
    positions.append(in_position)

df['inPosition'] = positions

# Show result
df['trend'] = np.where(df['inPosition'], 'ON', 'OFF')

print(df)

# Close DB
conn.close()