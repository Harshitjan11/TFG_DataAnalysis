import pandas as pd
import yfinance as yf
import requests
from io import StringIO

url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

response = requests.get(url)
data = StringIO(response.text)
df = pd.read_csv(data)

print("Total stocks listed:", len(df))
print(df[['SYMBOL', 'NAME OF COMPANY']].head())


symbols = df['SYMBOL'].tolist()
symbols = [sym + ".NS" for sym in symbols]

# Fetch using yfinance
for symbol in symbols:
    stock = yf.Ticker(symbol)
    info = stock.info
    print(f"{symbol}: {info.get('longName')} - Market Cap: {info.get('marketCap')}")

results = []

for symbol in symbols:
    stock = yf.Ticker(symbol)
    info = stock.info
    results.append({
        "Symbol": symbol,
        "Name": info.get("longName"),
        "Market Cap": info.get("marketCap"),
        "Sector": info.get("sector"),
    })

pd.DataFrame(results).to_csv("nse_stock_summary.csv", index=False)


