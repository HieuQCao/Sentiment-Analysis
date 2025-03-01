import pandas as pd
import yfinance as yf
import logging
import os
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of MAG7 stocks and BTC

tickers = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'AMD', 'INTC', 'CSCO',
    'XOM', 'CVX', 'COP', 'SLB', 'HAL', 'OXY', 'VLO', 'MPC', 'PSX', 'EOG',
    'JPM', 'BAC', 'WFC', 'C', 'GS', 'MS', 'PFE', 'JNJ', 'MRK', 'LLY',
    'PG', 'KO', 'PEP', 'WMT', 'TGT', 'COST', 'HD', 'LOW', 'DIS', 'NFLX',
    'T', 'VZ', 'CMCSA', 'TMUS', 'BA', 'CAT', 'GE', 'MMM', 'UPS'
]
def fetch_price_data(ticker, start_date, end_date):
    """Fetch historical closing prices for a given ticker and truncate to date only."""
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(start=start_date, end=end_date)
        if df.empty:
            logging.warning(f"No data retrieved for {ticker}")
            return None
        
        # Truncate the index to date only (remove time portion)
        df.index = df.index.date
        df.index = pd.to_datetime(df.index)  # Convert back to datetime for consistency
        
        return df['Close']  # Return closing prices
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {str(e)}")
        return None

def calculate_daily_returns(ticker, start_date, end_date):
    """Calculate daily returns for a ticker."""
    prices = fetch_price_data(ticker, start_date, end_date)
    if prices is None or len(prices) < 2:
        return None
    
    # Calculate daily percentage returns
    daily_returns = prices.pct_change().dropna()  # Returns as decimal
    return pd.Series(daily_returns, index=daily_returns.index, name=ticker)

def main():
    logging.info("Starting daily stock return calculation")
    
    # Define date range
    start_date = '2021-01-01'
    end_date = '2025-01-01'  # Includes 2024 fully, adjust if needed
    
    # Dictionary to store daily returns for each ticker
    returns_dict = {}
    
    # Calculate daily returns for each ticker
    for ticker in tickers:
        logging.info(f"Processing {ticker}")
        daily_returns = calculate_daily_returns(ticker, start_date, end_date)
        if daily_returns is not None:
            returns_dict[ticker] = daily_returns
    
    # Combine into a single DataFrame
    returns_df = pd.DataFrame(returns_dict)
    returns_df.index.name = 'Date'
    
    # Define the local folder and file path
    local_folder = os.path.join(os.getcwd(), 'data1')
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)
        print(f"Created folder: {local_folder}")
    else:
        print(f"Folder already exists: {local_folder}")
    
    filename = 'mag7_btc_daily_returns_2021_2024.csv'
    output_path = os.path.join(local_folder, filename)
    
    # Save to CSV with confirmation
    print(f"Saving CSV to: {output_path}")
    returns_df.to_csv(output_path)
    logging.info(f"Daily returns saved to {output_path}")
    print(f"Daily returns saved to: {output_path}")

if __name__ == "__main__":
    main()