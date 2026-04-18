import os
import yfinance as yf
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from main import DailyMetric, engine, load_dotenv

load_dotenv()
Session = sessionmaker(bind=engine)

def get_current_sp500_tickers():
    """Fetches the most up-to-date S&P 500 list from Wikipedia."""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        table = pd.read_html(url, attrs={'id': 'constituents'})[0]
        # Yahoo Finance uses '-' instead of '.' for tickers like BRK.B
        tickers = table['Symbol'].str.replace('.', '-', regex=False).tolist()
        return tickers
    except Exception as e:
        print(f"❌ Failed to fetch S&P 500 list: {e}")
        return []

def forward_fill_sp500():
    session = Session()
    tickers = get_current_sp500_tickers()
    
    if not tickers:
        return

    print(f"--- SYNCING {len(tickers)} S&P 500 TICKERS: {datetime.now()} ---")
    
    try:
        # Fetching in bulk is significantly faster than one-by-one
        data = yf.download(tickers, period="5d", interval="1d", group_by='ticker', threads=True)
        
        updated_count = 0
        for ticker in tickers:
            try:
                ticker_df = data[ticker]
                # Get the most recent valid trading day (usually yesterday)
                last_row = ticker_df.dropna().iloc[-1]
                
                metric = DailyMetric(
                    ticker=ticker,
                    date=last_row.name.date(),
                    close=float(last_row['Close']),
                    volume=int(last_row['Volume'])
                )
                
                # Merge ensures we don't create duplicates
                session.merge(metric)
                updated_count += 1
            except Exception:
                continue # Skip tickers with download errors (e.g., temporary YF delistings)
        
        session.commit()
        print(f"✅ Successfully synced {updated_count} tickers to Railway.")
        
    except Exception as e:
        print(f"❌ Bulk Sync Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    forward_fill_sp500()