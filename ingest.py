import yfinance as yf
import os
import time
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import DailyMetric, Base, load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL").replace("postgres://", "postgresql://", 1)
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

def get_clean_sp500_list():
    """
    A cleaner way to get the S&P 500 without scraping raw HTML.
    If the Wikipedia surgical strike failed, we use this hardcoded 
    top-tier list or a smaller tech-heavy list to get you started.
    """
    # For a professional setup, we usually use a static CSV or 
    # a dedicated financial data provider. 
    # Let's use a robust list of the top 100 S&P components 
    # to ensure the script doesn't choke on HTML.
    top_100 = [
        "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "GOOG", "META", "TSLA", "BRK-B", "UNH",
        "JPM", "XOM", "LLY", "AVGO", "V", "PG", "MA", "COST", "HD", "CVX", "MRK", "ABBV",
        "ADBE", "PEP", "KO", "ORCL", "TMO", "BAC", "CSCO", "CRM", "PFE", "ACN", "NFLX",
        "LIN", "AMD", "ABT", "DHR", "INTC", "DIS", "TXN", "PM", "CAT", "VZ", "AMGN",
        "INTU", "IBM", "UNP", "LOW", "COP", "GE", "AMAT", "HON", "BA", "RTX", "PLD",
        "SPGI", "AXP", "T", "MS", "ELV", "GS", "SYK", "SBUX", "MDLZ", "BLK", "TJX",
        "ISRG", "ADI", "LMT", "GILD", "MMC", "VRTX", "BKNG", "REGN", "ADP", "ETN",
        "MDT", "C", "SLB", "CB", "MU", "CI", "ZTS", "BSX", "DE", "MO", "PANW",
        "LRCX", "BMY", "ITW", "FI", "SNPS", "EOG", "CDNS", "CVS", "WM", "NOC", "SHW"
    ]
    return top_100

def ingest_scout_data():
    tickers = get_clean_sp500_list() # No more messy HTML prints!
    session = Session()
    today = datetime.now().date()
    
    print(f"--- STARTING v5.0 INGEST: {len(tickers)} TICKERS ---")

    for index, symbol in enumerate(tickers):
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # The "Upsert" logic
            target = session.query(DailyMetric).filter_by(ticker=symbol, date=today).first()
            if not target:
                target = DailyMetric(ticker=symbol, date=today)
                session.add(target)

            # Map v5.0 Data
            target.analyst_rating = info.get('recommendationMean', 3.0)
            target.volume = info.get('volume', 0)
            target.average_volume_30d = info.get('averageVolume', 1)
            target.short_float_pct = (info.get('shortPercentOfFloat', 0) or 0) * 100
            
            # Technical Stubs
            target.sentiment_score = 0.1 
            target.call_put_ratio = 1.0
            target.bb_width_30d_low = False
            target.rs_slope_5d = 0.1

            time.sleep(0.4) # Slight pause to be polite to Yahoo

            if index % 10 == 0 and index > 0:
                session.commit()
                print(f"Progress: {index}/{len(tickers)} processed...")

        except Exception as e:
            print(f"Skipping {symbol}: {e}")
            continue

    session.commit()
    session.close()
    print("--- INGEST COMPLETE ---")

if __name__ == "__main__":
    ingest_scout_data()