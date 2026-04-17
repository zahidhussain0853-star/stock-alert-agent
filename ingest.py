import os
import requests
import logging
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import DailyMetric  # Imports class from main.py

# --- LOGGING CONFIG ---
logging.basicConfig(level=logging.INFO, format='%(message)s', stream=sys.stderr)
logger = logging.getLogger("INGEST")

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def fetch_and_save(ticker):
    session = SessionLocal()
    logger.info(f"FETCHING: {ticker}...")
    
    try:
        # API Calls
        ov_url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}'
        q_url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={API_KEY}'
        
        ov_data = requests.get(ov_url).json()
        q_data = requests.get(q_url).json().get("Global Quote", {})

        if "Symbol" not in ov_data:
            logger.error(f"API ERROR: {ov_data.get('Note', 'Rate limit hit or bad ticker')}")
            return

        # Map to DB
        new_entry = DailyMetric(
            ticker=ticker,
            date=datetime.now().date(),
            analyst_rating=float(ov_data.get('AnalystRatingStrongBuy', 0)) + float(ov_data.get('AnalystRatingBuy', 0)),
            sentiment_score=0.0,
            volume=int(q_data.get('06. volume', 0)) if q_data.get('06. volume') else 0,
            average_volume_30d=int(ov_data.get('AverageVolume', 0)) if ov_data.get('AverageVolume') else 0,
            call_put_ratio=1.0,
            short_float_pct=0.0,
            bb_width_30d_low=False,
            rs_slope_5d=0.0
        )

        # Upsert
        existing = session.query(DailyMetric).filter_by(ticker=ticker, date=new_entry.date).first()
        if existing:
            for column in DailyMetric.__table__.columns:
                if column.name != 'id':
                    setattr(existing, column.name, getattr(new_entry, column.name))
        else:
            session.add(new_entry)

        session.commit()
        logger.info(f"SAVED: {ticker} successfully.")

    except Exception as e:
        session.rollback()
        logger.error(f"CRITICAL ERROR: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    watchlist = ["NVDA", "AAPL", "TSLA"]
    for t in watchlist:
        fetch_and_save(t)