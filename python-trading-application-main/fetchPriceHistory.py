import yfinance as yf
import pandas as pd
import time
import os
from datetime import datetime, timedelta

time_frame = 'max'
stocks = pd.read_csv("stocks.csv", header=0, usecols=["Ticker"])
# Set the bar time frame
data_interval = '5m'

# Set the maximum number of months to lookback
LOOKBACK_LIIMIT = 15 * 12 # Years in months

# Set minimum numbber of months that this BO should be after
MIN_BO_LENGTH = 50 #5 * 12 # Years in months

# Initialize a list to store the results
results = []

# Crore
One_Cr = 10000000

# Columnns in the report
report_columns = ["Stock", "mcap", "High Close", "High Close Date", "Current Close", "#MonthsBO", "Diff", "sector" , "industry"]

def write_dataframe_to_file(df, name):
    # Get the current timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # Create the filename
    filename = f'{name}_{timestamp}.csv'
    # Save the DataFrame as a CSV file with specific column names as the header
    df.to_csv(f'{output_path}/{filename}',index=False)


def main():
    print("Started...")
    # create an empty dataframe to store the results
    results_df = pd.DataFrame(columns=report_columns)
    # Iterate through the list of stocks
    for stock in stocks["Ticker"]:
        try:
            ticker = yf.Ticker(stock+".NS")
            data = ticker.history(period=time_frame,interval=data_interval,auto_adjust=False)
            
            data = data.dropna()
            print(f"Data for {stock}:")
            print("Open | High | Low | Close")
            print(data[['Open', 'High', 'Low', 'Close']])
            current_close = data['Close'].iloc[-1]
            print(f"Current Close for {stock}: {current_close}")

        except Exception as e:
            print(f'Error for ticker: {stock} ==> {e}')

    # print(results_df)
    #write_dataframe_to_file(results_df, "newHighMonthly_BO_")
    print("Done")

if __name__ == "__main__":
    main()