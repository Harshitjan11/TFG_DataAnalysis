from concurrent.futures import ThreadPoolExecutor, as_completed
import ta
import pandas as pd
from sqlalchemy import create_engine
from ta.trend import ADXIndicator
import mysql.connector
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Create SQLAlchemy engine for reading data
engine = create_engine("mysql+mysqlconnector://bot:password@localhost/trading_bot")

# Fetch stock IDs
logger.info("Fetching stock IDs from the 'stock' table...")
stock_query = "SELECT id FROM stock where id=1"
stock_ids_df = pd.read_sql(stock_query, engine)
stock_ids = stock_ids_df['id'].tolist()
logger.info(f"Fetched {len(stock_ids)} stock IDs.")

# Function to process one stock ID
def process_stock(stock_id):
    try:
        logger.info(f"Processing stock_id {stock_id}...")

        # Create a new DB connection per thread
        conn = mysql.connector.connect(
            host="localhost",
            user="bot",
            password="password",
            database="trading_bot"
        )
        cursor = conn.cursor()

        query = """
                SELECT date, high, low, close
                FROM daily
                WHERE stock_id = %s
                ORDER BY date ASC
                """
        df = pd.read_sql(query, engine, params=(stock_id,))
        if df.empty:
            logger.warning(f"No data found for stock_id {stock_id}. Skipping...")
            return

        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

        if not all(col in df.columns for col in ['high', 'low', 'close']):
            logger.error(f"Missing required columns for stock_id {stock_id}. Skipping...")
            return

        ma_periods = [8, 10, 21, 50, 55, 100, 200]
        rolling_periods = [21, 55, 100]

        for period in rolling_periods:
            df[f'high_{period}'] = df['close'].rolling(window=period).max()
            df[f'low_{period}'] = df['close'].rolling(window=period).min()

        for period in ma_periods:
            df[f'SMA_{period}'] = ta.trend.sma_indicator(df['close'], window=period)
            df[f'EMA_{period}'] = ta.trend.ema_indicator(df['close'], window=period)

        try:
            adx_indicator = ADXIndicator(high=df['high'], low=df['low'], close=df['close'], window=14)
            df['plus_di'] = adx_indicator.adx_pos()
            df['minus_di'] = adx_indicator.adx_neg()
            df['adx'] = adx_indicator.adx()
        except Exception as e:
            logger.error(f"Error calculating ADX for stock_id {stock_id}: {e}")
            return

        rows_updated = 0
        for date, row in df.iterrows():
            if pd.notnull(row['SMA_200']) and pd.notnull(row['EMA_200']):
                try:
                    cursor.execute(""" UPDATE daily
                                       SET sma_8    = %s,
                                           sma_10   = %s,
                                           sma_21   = %s,
                                           sma_50   = %s,
                                           sma_55   = %s,
                                           sma_100  = %s,
                                           sma_200  = %s,
                                           ema_8    = %s,
                                           ema_10   = %s,
                                           ema_21   = %s,
                                           ema_50   = %s,
                                           ema_55   = %s,
                                           ema_100  = %s,
                                           ema_200  = %s,
                                           high_21  = %s,
                                           low_21   = %s,
                                           high_55  = %s,
                                           low_55   = %s,
                                           high_100 = %s,
                                           low_100  = %s,
                                           plus_di  = %s,
                                           minus_di = %s,
                                           adx      = %s
                                       WHERE stock_id = %s
                                         AND date = %s """, (
                                       *(None if pd.isna(row[f]) else float(row[f]) for f in [
                                           'SMA_8', 'SMA_10', 'SMA_21', 'SMA_50', 'SMA_55', 'SMA_100', 'SMA_200',
                                           'EMA_8', 'EMA_10', 'EMA_21', 'EMA_50', 'EMA_55', 'EMA_100', 'EMA_200',
                                           'high_21', 'low_21', 'high_55', 'low_55', 'high_100', 'low_100',
                                           'plus_di', 'minus_di', 'adx'
                                       ]),
                                       stock_id, date.to_pydatetime()
                                   ))
                    rows_updated += 1
                except Exception as e:
                    logger.error(f"Error updating stock_id {stock_id}, date {date}: {e}")

        conn.commit()
        logger.info(f"Updated {rows_updated} rows in the 'daily' table for stock_id {stock_id}.")

    except Exception as e:
        logger.error(f"Error processing stock_id {stock_id}: {e}")

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


logger.info("Starting threaded stock processing...")
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(process_stock, stock_id) for stock_id in stock_ids]
    for future in as_completed(futures):
        pass

logger.info("✅ All stocks processed.")
