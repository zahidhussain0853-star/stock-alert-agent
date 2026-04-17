import os
import requests
import logging
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import DailyMetric, Base

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("INGEST")

# Environment Variables
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def fetch_and_save(ticker):
    session = SessionLocal()
    logger.info(f"--- Scouting Ticker: {ticker} ---")
    
    try:
        # API Calls
        ov_url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}'
        q_url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={API_KEY}'
        
        ov_res = requests.get(ov_url).json()
        q_res = requests.get(q_url).json()

        if "Global Quote" not in q_res or not ov_res:
            logger.error(f"API Error for {ticker}: Check API Key limits or Ticker validity.")
            return

        quote = q_res["Global Quote"]
        
        # Mapping Data
        new_entry = DailyMetric(
            ticker=ticker,
            date=datetime.now().date(),
            analyst_rating=float(ov_res.get('AnalystRatingStrongBuy', 0)) * 1 + float(ov_res.get('AnalystRatingBuy', 0)) * 2,
            sentiment_score=0.0, # Placeholder until sentiment endpoint is verified
            volume=int(quote.get('06. volume', 0)),
            avg_volume_30d=int(ov_res.get('AverageVolume', 0)) if ov_res.get('AverageVolume') else 0,
            call_put_ratio=1.0,
            short_float_pct=float(ov_res.get('PercentVisible', 0)) if ov_res.get('PercentVisible') else 0,
            bb_width_30d_low=False,
            rs_slope_5d=0.0
        )

        # UPSERT Logic: Update if exists, else insert
        existing = session.query(DailyMetric).filter_by(ticker=ticker, date=new_entry.date).first()
        if existing:
            logger.info(f"Updating existing record for {ticker}")
            for column in new_entry.__table__.columns:
                setattr(existing, column.name, getattr(new_entry, column.name))
        else:
            logger.info(f"Inserting new record for {ticker}")
            session.add(new_entry)

        session.commit()
        logger.info(f"SUCCESS: {ticker} written to database.")

    except Exception as e:
        session.rollback()
        logger.error(f"DATABASE ERROR: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    if not DATABASE_URL or not API_KEY:
        logger.error("Missing Environment Variables! Check DATABASE_URL and ALPHA_VANTAGE_KEY.")
    else:
        # Test with a single ticker first
        fetch_and_save("NVDA")