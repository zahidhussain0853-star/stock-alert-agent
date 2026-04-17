import os
import sys
from sqlalchemy import create_engine, Column, Integer, String, Date, Float, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timedelta

# --- DB SETUP ---
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
    average_volume_30d = Column(Integer)
    call_put_ratio = Column(Float)
    short_float_pct = Column(Float)
    bb_width_30d_low = Column(Boolean)
    rs_slope_5d = Column(Float)

def run_analysis():
    session = SessionLocal()
    print("SCOUT: Starting analysis...")
    
    try:
        tickers = [t.ticker for t in session.query(DailyMetric.ticker).distinct().all()]
        print(f"SCOUT: Found {len(tickers)} tickers in database.")

        for ticker in tickers:
            current = session.query(DailyMetric).filter_by(ticker=ticker).order_by(DailyMetric.date.desc()).first()
            print(f"SCOUT REPORT: {ticker} | Date: {current.date} | Rating: {current.analyst_rating}")
            
    except Exception as e:
        print(f"SCOUT ERROR: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    print("--- MAIN START ---")
    run_analysis()
    print("--- MAIN END ---")