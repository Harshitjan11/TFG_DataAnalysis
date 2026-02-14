import ta
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

engine = create_engine("mysql+mysqlconnector://root:@localhost/stock_report")

logger.info("Fetching stock IDs from the 'stock' table...")
stock_query = "SELECT id FROM stock limit 2200 offset 300"
stock_ids_df = pd.read_sql(stock_query, engine)
logger.info(f"Fetched {len(stock_ids_df)} stock IDs.")

logger.info("Connecting to MySQL database...")
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="stock_report"
)
cursor = conn.cursor()

for stock_id in stock_ids_df['id']:
    logger.info(f"Processing stock_id {stock_id}...")

    query = """
            SELECT date, close
            FROM daily
            WHERE stock_id = %s
            ORDER BY date ASC \
            """
    df = pd.read_sql(query, engine, params=(stock_id,))
    logger.info(f"Fetched {len(df)} rows of daily data for stock_id {stock_id}.")

    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)


    ma_periods = [8, 10, 21, 50, 55, 100, 200]
    for period in ma_periods:
        logger.info(f"Calculating SMA and EMA for period {period}...")
        df[f'SMA_{period}'] = ta.trend.sma_indicator(df['close'], window=period)
        df[f'EMA_{period}'] = ta.trend.ema_indicator(df['close'], window=period)

    rows_updated = 0
    for date, row in df.iterrows():
        if pd.notnull(row['SMA_200']) and pd.notnull(row['EMA_200']):
            cursor.execute("""
                           UPDATE daily
                           SET sma_8   = %s,
                               sma_10  = %s,
                               sma_21  = %s,
                               sma_50  = %s,
                               sma_55  = %s,
                               sma_100 = %s,
                               sma_200 = %s,
                               ema_8   = %s,
                               ema_10  = %s,
                               ema_21  = %s,
                               ema_50  = %s,
                               ema_55  = %s,
                               ema_100 = %s,
                               ema_200 = %s
                           WHERE stock_id = %s
                             AND date = %s
                           """, (
                               row['SMA_8'], row['SMA_10'], row['SMA_21'],
                               row['SMA_50'], row['SMA_55'], row['SMA_100'], row['SMA_200'],
                               row['EMA_8'], row['EMA_10'], row['EMA_21'],
                               row['EMA_50'], row['EMA_55'], row['EMA_100'], row['EMA_200'],
                               stock_id, date.date()
                           ))
            rows_updated += 1

    logger.info(f"Updated {rows_updated} rows in the 'daily' table for stock_id {stock_id}.")

# Commit all updates
logger.info("Committing updates to the database...")
conn.commit()

# Cleanup
cursor.close()
conn.close()
logger.info("Database connection closed.")

