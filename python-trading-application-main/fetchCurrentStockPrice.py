import yfinance as yf
from datetime import datetime

ticker_symbol = "20MICRONS.NS"
stock = yf.Ticker(ticker_symbol)
data = stock.history(period="1d", interval="1m")


current_price = data['Close'].iloc[-1]
print(f"The current price of {ticker_symbol} is: {current_price}")

data = yf.download(
    tickers=ticker_symbol,
    interval="1d",
    start="1900-01-01",
    end=datetime.now().date(),
    progress=False,
    group_by='column'
)
print(data)
