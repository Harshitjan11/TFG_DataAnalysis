import yfinance as yf
from datetime import datetime
import mysql.connector
import ta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from ta.trend import CCIIndicator
from ta.trend import ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import DonchianChannel, AverageTrueRange, BollingerBands
from ta.volume import VolumeWeightedAveragePrice, MFIIndicator
import mysql.connector
import logging
from technical_indicators import calculate_kama, safe_float

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Password@123",
    database="trading_bot"
)
cursor = conn.cursor(dictionary=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

engine = create_engine("mysql+mysqlconnector://bot:password@localhost/trading_bot")

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

    conn.commit()
    print(f"Inserted data for {stock_code}.")



try:
    for stock in stocks:
        stock_id = stock['id']
        logger.info(f"Processing stock_id {stock_id}...")

        query = """
                SELECT date, high, low, close, volume
                FROM daily
                WHERE stock_id = %s
                ORDER BY date ASC
                """
        df = pd.read_sql(query, engine, params=(stock_id,))
        logger.info(f"Fetched {len(df)} rows of daily data for stock_id {stock_id}.")

        if df.empty:
            logger.warning(f"No data found for stock_id {stock_id}. Skipping...")
            continue

        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

        # Verify we have the required columns
        if not all(col in df.columns for col in ['high', 'low', 'close']):
            logger.error(f"Missing required columns (high, low, close) for stock_id {stock_id}. Skipping...")
            continue

        ma_periods = [8, 10, 21, 50, 55, 100, 200]
        rolling_periods = [21, 55, 100]

        # Calculate rolling highs and lows
        for period in rolling_periods:
            logger.info(f"Calculating {period}-day high and low for stock_id {stock_id}...")
            df[f'high_{period}'] = df['close'].rolling(window=period).max()
            df[f'low_{period}'] = df['close'].rolling(window=period).min()

        # Calculate moving averages
        for period in ma_periods:
            logger.info(f"Calculating SMA and EMA for period {period} for stock_id {stock_id}...")
            df[f'SMA_{period}'] = ta.trend.sma_indicator(df['close'], window=period)
            df[f'EMA_{period}'] = ta.trend.ema_indicator(df['close'], window=period)

        # Calculate all technical indicators
        try:
            # ADX indicators
            adx_indicator = ADXIndicator(high=df['high'], low=df['low'], close=df['close'], window=14)
            df['plus_di'] = adx_indicator.adx_pos()
            df['minus_di'] = adx_indicator.adx_neg()
            df['adx'] = adx_indicator.adx()

            # CCI
            cci_indicator = CCIIndicator(high=df['high'], low=df['low'], close=df['close'], window=20)
            df['cci'] = cci_indicator.cci()

            # RSI
            df['rsi'] = RSIIndicator(close=df['close'], window=14).rsi()

            # KAMA - Using the imported function
            logger.info("Calculating KAMA...")
            df['kama'] = calculate_kama(df['close'], window=10, pow1=2, pow2=30)


            if df['kama'].isna().all():
                logger.info("Using EMA as KAMA substitute...")
                df['kama'] = ta.trend.ema_indicator(df['close'], window=14)

            # Donchian Channel
            dc = DonchianChannel(high=df['high'], low=df['low'], close=df['close'], window=20)
            df['dc_upper'] = dc.donchian_channel_hband()
            df['dc_lower'] = dc.donchian_channel_lband()

            # VWAP (if volume available)
            if 'volume' in df.columns and not df['volume'].isna().all():
                try:
                    df['vwap'] = VolumeWeightedAveragePrice(
                        high=df['high'],
                        low=df['low'],
                        close=df['close'],
                        volume=df['volume']
                    ).volume_weighted_average_price()
                except Exception as e:
                    logger.warning(f"VWAP calculation failed: {e}")
                    df['vwap'] = np.nan
            else:
                df['vwap'] = np.nan

            # ATR
            df['atr'] = AverageTrueRange(
                high=df['high'],
                low=df['low'],
                close=df['close'],
                window=14
            ).average_true_range()

            # Bollinger Bands
            bb = BollingerBands(close=df['close'], window=20, window_dev=2)
            df['bb_middle'] = bb.bollinger_mavg()
            df['bb_upper'] = bb.bollinger_hband()
            df['bb_lower'] = bb.bollinger_lband()

            # MFI (if volume available)
            if 'volume' in df.columns and not df['volume'].isna().all():
                try:
                    df['mfi'] = MFIIndicator(
                        high=df['high'],
                        low=df['low'],
                        close=df['close'],
                        volume=df['volume'],
                        window=14
                    ).money_flow_index()
                except Exception as e:
                    logger.warning(f"MFI calculation failed: {e}")
                    df['mfi'] = np.nan
            else:
                df['mfi'] = np.nan

        except Exception as e:
            logger.error(f"Error calculating indicators for stock_id {stock_id}: {e}")
            continue

        # Update database
        rows_updated = 0
        for date, row in df.iterrows():
            try:
                # Convert date to string format for MySQL
                date_str = date.strftime('%Y-%m-%d')

                # Only update if we have some calculated values (not just for SMA_200/EMA_200)
                has_indicators = any(not pd.isna(row.get(col)) for col in [
                    'SMA_21', 'EMA_21', 'rsi', 'kama', 'adx'
                ])

                if has_indicators:
                    values = (
                        safe_float(row.get('SMA_8')),
                        safe_float(row.get('SMA_10')),
                        safe_float(row.get('SMA_21')),
                        safe_float(row.get('SMA_50')),
                        safe_float(row.get('SMA_55')),
                        safe_float(row.get('SMA_100')),
                        safe_float(row.get('SMA_200')),
                        safe_float(row.get('EMA_8')),
                        safe_float(row.get('EMA_10')),
                        safe_float(row.get('EMA_21')),
                        safe_float(row.get('EMA_50')),
                        safe_float(row.get('EMA_55')),
                        safe_float(row.get('EMA_100')),
                        safe_float(row.get('EMA_200')),
                        safe_float(row.get('high_21')),
                        safe_float(row.get('low_21')),
                        safe_float(row.get('high_55')),
                        safe_float(row.get('low_55')),
                        safe_float(row.get('high_100')),
                        safe_float(row.get('low_100')),
                        safe_float(row.get('plus_di')),
                        safe_float(row.get('minus_di')),
                        safe_float(row.get('adx')),
                        safe_float(row.get('cci')),
                        safe_float(row.get('rsi')),
                        safe_float(row.get('kama')),  # This is your KAMA value
                        safe_float(row.get('dc_upper')),
                        safe_float(row.get('dc_lower')),
                        safe_float(row.get('vwap')),
                        safe_float(row.get('atr')),
                        safe_float(row.get('bb_middle')),
                        safe_float(row.get('bb_upper')),
                        safe_float(row.get('bb_lower')),
                        safe_float(row.get('mfi')),
                        stock_id,
                        date_str
                    )

                    update_query = """
                                   UPDATE daily
                                   SET sma_8     = %s,
                                       sma_10    = %s,
                                       sma_21    = %s,
                                       sma_50    = %s,
                                       sma_55    = %s,
                                       sma_100   = %s,
                                       sma_200   = %s,
                                       ema_8     = %s,
                                       ema_10    = %s,
                                       ema_21    = %s,
                                       ema_50    = %s,
                                       ema_55    = %s,
                                       ema_100   = %s,
                                       ema_200   = %s,
                                       high_21   = %s,
                                       low_21    = %s,
                                       high_55   = %s,
                                       low_55    = %s,
                                       high_100  = %s,
                                       low_100   = %s,
                                       plus_di   = %s,
                                       minus_di  = %s,
                                       adx       = %s,
                                       cci       = %s,
                                       rsi       = %s,
                                       kama      = %s,
                                       dc_upper  = %s,
                                       dc_lower  = %s,
                                       vwap      = %s,
                                       atr       = %s,
                                       bb_middle = %s,
                                       bb_upper  = %s,
                                       bb_lower  = %s,
                                       mfi       = %s
                                   WHERE stock_id = %s
                                     AND date = %s
                                   """

                    cursor.execute(update_query, values)

                    if cursor.rowcount > 0:
                        rows_updated += 1
                        if not pd.isna(row.get('kama')):
                            logger.debug(f"Updated KAMA={safe_float(row.get('kama')):.4f} for {date_str}")
                    else:
                        logger.warning(f"No rows updated for stock_id {stock_id}, date {date_str}")

            except Exception as e:
                logger.error(f"Error updating row for stock_id {stock_id}, date {date}: {e}")

        # Commit after each stock
        conn.commit()
        logger.info(f"Updated {rows_updated} rows in the 'daily' table for stock_id {stock_id}.")

except Exception as e:
    logger.error(f"Unexpected error: {e}")
finally:
    logger.info("Closing database connection...")
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    logger.info("Database connection closed.")