import os
import requests
import logging
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import DailyMetric # Uses the updated class above

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("INGEST")

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def fetch_and_save(ticker):
    session = SessionLocal()
    try:
        ov_url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}'
        q_url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={API_KEY}'
        
        ov_res = requests.get(ov_url).json()
        q_res = requests.get(q_url).json()

        if "Global Quote" not in q_res:
            logger.error(f"API Error: {q_res}")
            return

        quote = q_res["Global Quote"]
        
        new_entry = DailyMetric(
            ticker=ticker,
            date=datetime.now().date(),
            analyst_rating=float(ov_res.get('AnalystRatingStrongBuy', 0)) + float(ov_res.get('AnalystRatingBuy', 0)),
            sentiment_score=0.0,
            volume=int(quote.get('06. volume', 0)),
            average_volume_30d=int(ov_res.get('AverageVolume', 0)) if ov_res.get('AverageVolume') else 0, # FIXED
            call_put_ratio=1.0,
            short_float_pct=float(ov_res.get('PercentVisible', 0)) if ov_res.get('PercentVisible') else 0,
            bb_width_30d_low=False,
            rs_slope_5d=0.0
        )

        # Upsert logic
        existing = session.query(DailyMetric).filter_by(ticker=ticker, date=new_entry.date).first()
        if existing:
            for column in new_entry.__table__.columns:
                if column.name != 'id':
                    setattr(existing, column.name, getattr(new_entry, column.name))
        else:
            session.add(new_entry)

        session.commit()
        logger.info(f"SUCCESS: {ticker} saved.")

    except Exception as e:
        session.rollback()
        logger.error(f"INGEST ERROR: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    fetch_and_save("NVDA")