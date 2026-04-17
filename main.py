import os
import logging
from sqlalchemy import create_engine, Column, Integer, String, Date, Float, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SCOUT_ENGINE")

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

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
    average_volume_30d = Column(Integer)  # FIXED: Matches your DB exactly
    call_put_ratio = Column(Float)
    short_float_pct = Column(Float)
    bb_width_30d_low = Column(Boolean)
    rs_slope_5d = Column(Float)

def calculate_scores():
    session = SessionLocal()
    try:
        logger.info("Connecting to database...")
        count = session.query(DailyMetric).count()
        logger.info(f"Database contains {count} records.")
        
        if count == 0:
            logger.warning("Database is empty. Check ingest.py logs.")
            return

        latest = session.query(DailyMetric).order_by(DailyMetric.date.desc()).all()
        for stock in latest:
            # Score logic goes here in next step
            logger.info(f"LIVE DATA FOUND -> {stock.ticker} on {stock.date} | Vol: {stock.volume}")

    except Exception as e:
        logger.error(f"ENGINE ERROR: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    calculate_scores()