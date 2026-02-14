"""
Technical Indicators Module
Contains functions for calculating various technical indicators including KAMA
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

# Try to import KAMAIndicator, if not available we'll use manual calculation
try:
    from ta.trend import KAMAIndicator

    KAMA_AVAILABLE = True
except ImportError:
    KAMA_AVAILABLE = False
    logger.info("KAMAIndicator not available in ta library, using manual calculation")


def calculate_kama_manual(close_prices, window=10, pow1=2, pow2=30):
    """
    Manual KAMA calculation - Kaufman's Adaptive Moving Average

    KAMA = Previous KAMA + SC * (Price - Previous KAMA)
    where SC = Smoothing Constant = (ER * (fastest SC - slowest SC) + slowest SC)^2
    ER = Efficiency Ratio = Change/Volatility

    Parameters:
    - close_prices: pandas Series of closing prices
    - window: period for efficiency ratio calculation (default 10)
    - pow1: fast EMA period (default 2)
    - pow2: slow EMA period (default 30)

    Returns:
    - pandas Series with KAMA values
    """
    if len(close_prices) < window + 1:
        return pd.Series([np.nan] * len(close_prices), index=close_prices.index)

    kama_values = []
    close_list = close_prices.tolist()

    for i in range(len(close_list)):
        if i < window:
            kama_values.append(np.nan)
            continue

        # Calculate Efficiency Ratio (ER)
        change = abs(close_list[i] - close_list[i - window])

        # Calculate volatility (sum of absolute price changes)
        volatility = 0
        for j in range(i - window + 1, i + 1):
            if j > 0:  # Ensure we don't go out of bounds
                volatility += abs(close_list[j] - close_list[j - 1])

        # Avoid division by zero
        if volatility == 0:
            er = 0
        else:
            er = change / volatility

        # Calculate Smoothing Constant (SC)
        fastest_sc = 2.0 / (pow1 + 1)  # Fast EMA smoothing constant
        slowest_sc = 2.0 / (pow2 + 1)  # Slow EMA smoothing constant
        sc = (er * (fastest_sc - slowest_sc) + slowest_sc) ** 2

        # Calculate KAMA
        if i == window:
            # First KAMA value is just the current price
            kama_values.append(close_list[i])
        else:
            prev_kama = kama_values[-1]
            if pd.isna(prev_kama):
                kama_values.append(close_list[i])
            else:
                current_kama = prev_kama + sc * (close_list[i] - prev_kama)
                kama_values.append(current_kama)

    return pd.Series(kama_values, index=close_prices.index)


def calculate_kama_simple(close_prices, window=14):
    """
    Simplified KAMA calculation using exponential smoothing
    This is a fallback if the main KAMA calculation fails

    Parameters:
    - close_prices: pandas Series of closing prices
    - window: period for volatility calculation (default 14)

    Returns:
    - pandas Series with simplified KAMA values
    """
    try:
        # Simple adaptive moving average based on volatility
        volatility = close_prices.rolling(window=window).std()
        alpha = 2.0 / (window + 1)

        # Adjust alpha based on volatility (lower volatility = more smoothing)
        max_vol = volatility.rolling(window=50).max()
        min_vol = volatility.rolling(window=50).min()
        vol_ratio = (volatility - min_vol) / (max_vol - min_vol)
        vol_ratio = vol_ratio.fillna(0.5)  # Fill NaN with neutral value

        adaptive_alpha = alpha * (0.1 + 0.9 * vol_ratio)  # Alpha between 0.1*alpha and alpha

        kama = pd.Series(index=close_prices.index, dtype=float)
        kama.iloc[0] = close_prices.iloc[0]

        for i in range(1, len(close_prices)):
            if pd.isna(adaptive_alpha.iloc[i]):
                kama.iloc[i] = kama.iloc[i - 1]
            else:
                kama.iloc[i] = adaptive_alpha.iloc[i] * close_prices.iloc[i] + (1 - adaptive_alpha.iloc[i]) * kama.iloc[
                    i - 1]

        return kama
    except Exception as e:
        logger.error(f"Error in simple KAMA calculation: {e}")
        return pd.Series([np.nan] * len(close_prices), index=close_prices.index)


def calculate_kama(close_prices, window=10, pow1=2, pow2=30):
    """
    Main KAMA calculation function with multiple fallback methods

    Parameters:
    - close_prices: pandas Series of closing prices
    - window: period for efficiency ratio calculation (default 10)
    - pow1: fast EMA period (default 2)
    - pow2: slow EMA period (default 30)

    Returns:
    - pandas Series with KAMA values
    """
    kama_calculated = False
    kama_series = None

    # Method 1: Try ta library KAMAIndicator if available
    if KAMA_AVAILABLE:
        try:
            kama_indicator = KAMAIndicator(close=close_prices, window=window, pow1=pow1, pow2=pow2)
            kama_series = kama_indicator.kama()
            kama_calculated = True
            logger.info("KAMA calculated using ta.KAMAIndicator")
        except Exception as e:
            logger.warning(f"Error with ta.KAMAIndicator: {e}")

    # Method 2: Manual KAMA calculation
    if not kama_calculated:
        try:
            kama_series = calculate_kama_manual(close_prices, window=window, pow1=pow1, pow2=pow2)
            kama_calculated = True
            logger.info("KAMA calculated using manual method")
        except Exception as e:
            logger.warning(f"Error with manual KAMA: {e}")

    # Method 3: Simplified KAMA as fallback
    if not kama_calculated:
        try:
            kama_series = calculate_kama_simple(close_prices, window=14)
            kama_calculated = True
            logger.info("KAMA calculated using simplified method")
        except Exception as e:
            logger.error(f"All KAMA methods failed: {e}")
            kama_series = pd.Series([np.nan] * len(close_prices), index=close_prices.index)

    # Debug KAMA values
    if kama_series is not None:
        kama_non_null = kama_series.dropna()
        if len(kama_non_null) > 0:
            logger.info(f"KAMA Statistics:")
            logger.info(f"  - Non-null values: {len(kama_non_null)}")
            logger.info(f"  - Min: {kama_non_null.min():.4f}")
            logger.info(f"  - Max: {kama_non_null.max():.4f}")
            logger.info(f"  - Mean: {kama_non_null.mean():.4f}")
            logger.info(f"  - Latest 5 values: {[f'{x:.4f}' for x in kama_non_null.tail().tolist()]}")
        else:
            logger.warning("No valid KAMA values calculated!")

    return kama_series


def safe_float(val):
    """
    Convert value to float or None for MySQL NULL values

    Parameters:
    - val: value to convert

    Returns:
    - float value or None if NaN
    """
    return None if pd.isna(val) else float(val)