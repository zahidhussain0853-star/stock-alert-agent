import os
import logging
from sqlalchemy import create_engine, Column, Integer, String, Date, Float, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timedelta

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SCOUT_ENGINE")

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class DailyMetric(Base):
    __tablename__ = "daily_metrics"
    id = Column(Integer, primary_key=True)
    ticker = Column(String)
    date = Column(Date)
    analyst_rating = Column(Float)
    sentiment_score = Column(Float)
    volume = Column(Integer)
    avg_volume_30d = Column(Integer)
    call_put_ratio = Column(Float)
    short_float_pct = Column(Float)
    bb_width_30d_low = Column(Boolean)
    rs_slope_5d = Column(Float)

def calculate_scores():
    session = SessionLocal()
    try:
        # Check if we have ANY data
        total_rows = session.query(DailyMetric).count()
        logger.info(f"Scout Engine waking up. Database contains {total_rows} records.")
        
        if total_rows == 0:
            logger.warning("No data found to analyze. Run ingest.py first.")
            return

        # Get the most recent tickers
        latest_entries = session.query(DailyMetric).order_by(DailyMetric.date.desc()).limit(10).all()
        for entry in latest_entries:
            logger.info(f"Analyzing {entry.ticker} for {entry.date}...")
            # (Insert your scoring logic here - keeping it empty for the debug run)
            logger.info(f"Score for {entry.ticker}: Ready for calculation.")

    except Exception as e:
        logger.error(f"ENGINE ERROR: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    calculate_scores()