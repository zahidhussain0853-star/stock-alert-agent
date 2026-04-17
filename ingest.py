import os
import requests
import logging
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# Import the model from your main.py
from main import DailyMetric 

# --- CONFIGURATION & LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("INGEST")

# Railway Environment Variables
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

# SQLAlchemy Fix for Railway/Postgres
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

def fetch_and_save(ticker):
    if not DATABASE_URL or not API_KEY:
        logger.error("MISSING CONFIG: Ensure DATABASE_URL and ALPHA_VANTAGE_KEY are in Railway Variables.")
        return

    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    logger.info(f"🚀 Starting ingestion for: {ticker}")

    try:
        # 1. Fetch Fundamental Overview (Analyst Ratings & Avg Volume)
        ov_url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}'
        ov_res = requests.get(ov_url).json()

        # Check for API Limit/Error
        if "Symbol" not in ov_res:
            info = ov_res.get("Information") or ov_res.get("Note") or "Unknown Error"
            logger.error(f"❌ API Rejected {ticker}: {info}")
            return

        # 2. Fetch Real-time Quote (Current Volume)
        q_url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={API_KEY}'
        q_res = requests.get(q_url).json()
        quote = q_res.get("Global Quote", {})

        if not quote:
            logger.error(f"⚠️ Quote data missing for {ticker}. Check API limits.")
            return

        # 3. Build the Entry (Mapping to your specific DB columns)
        # Note: Using 'real' equivalents for decimals
        new_entry = DailyMetric(
            ticker=ticker,
            date=datetime.now().date(),
            analyst_rating=float(ov_res.get('AnalystRatingStrongBuy', 0)) + float(ov_res.get('AnalystRatingBuy', 0)),
            sentiment_score=0.0, # Placeholder for expansion
            volume=int(quote.get('06. volume', 0)),
            average_volume_30d=int(ov_res.get('AverageVolume', 0)) if ov_res.get('AverageVolume') else 0,
            call_put_ratio=1.0,
            short_float_pct=float(ov_res.get('PercentVisible', 0)) if ov_res.get('PercentVisible') else 0,
            bb_width_30d_low=False,
            rs_slope_5d=0.0
        )

        # 4. UPSERT Logic (Update if ticker/date combo exists, else Insert)
        existing = session.query(DailyMetric).filter_by(ticker=ticker, date=new_entry.date).first()
        
        if existing:
            logger.info(f"🔄 Updating existing record for {ticker} on {new_entry.date}")
            for column in DailyMetric.__table__.columns:
                if column.name != 'id':
                    setattr(existing, column.name, getattr(new_entry, column.name))
        else:
            logger.info(f"📥 Inserting new record for {ticker}")
            session.add(new_entry)

        session.commit()
        logger.info(f"✅ SUCCESS: Data committed for {ticker}")

    except Exception as e:
        session.rollback()
        logger.error(f"💥 CRITICAL ERROR: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    # Add the tickers you want the Scout to monitor here
    watchlist = ["NVDA", "AAPL", "TSLA"]
    for symbol in watchlist:
        fetch_and_save(symbol)