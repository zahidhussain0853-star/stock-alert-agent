import os
import requests
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import DailyMetric 

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

def fetch_and_save(ticker):
    print(f"INGEST: Working on {ticker}...")
    
    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Alpha Vantage Calls
        ov_url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}'
        q_url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={API_KEY}'
        
        ov_res = requests.get(ov_url).json()
        q_data = requests.get(q_url).json().get("Global Quote", {})

        if "Symbol" not in ov_res:
            print(f"INGEST ERROR: API limit or bad symbol for {ticker}")
            return

        new_entry = DailyMetric(
            ticker=ticker,
            date=datetime.now().date(),
            analyst_rating=float(ov_res.get('AnalystRatingStrongBuy', 0)) + float(ov_res.get('AnalystRatingBuy', 0)),
            sentiment_score=0.0,
            volume=int(q_data.get('06. volume', 0)) if q_data.get('06. volume') else 0,
            average_volume_30d=int(ov_res.get('AverageVolume', 0)) if ov_res.get('AverageVolume') else 0,
            call_put_ratio=1.0,
            short_float_pct=0.0,
            bb_width_30d_low=False,
            rs_slope_5d=0.0
        )

        # Basic Add
        existing = session.query(DailyMetric).filter_by(ticker=ticker, date=new_entry.date).first()
        if existing:
            print(f"INGEST: Updating {ticker}")
            for col in DailyMetric.__table__.columns:
                if col.name != 'id': setattr(existing, col.name, getattr(new_entry, col.name))
        else:
            print(f"INGEST: Inserting {ticker}")
            session.add(new_entry)

        session.commit()
        print(f"INGEST SUCCESS: {ticker} saved.")
        session.close()

    except Exception as e:
        print(f"INGEST CRITICAL: {e}")

if __name__ == "__main__":
    print("--- INGEST START ---")
    watchlist = ["NVDA", "AAPL", "TSLA"]
    for symbol in watchlist:
        fetch_and_save(symbol)
    print("--- INGEST END ---")