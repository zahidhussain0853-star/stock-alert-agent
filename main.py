import os
import logging
import sys
from sqlalchemy import create_engine, Column, Integer, String, Date, Float, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timedelta

# --- LOGGING CONFIG (Immediate Railway visibility) ---
logging.basicConfig(level=logging.INFO, format='%(message)s', stream=sys.stderr)
logger = logging.getLogger("SCOUT_ENGINE")

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

def get_scout_score(ticker):
    session = SessionLocal()
    today = datetime.now().date()
    
    try:
        # 1. Fetch current and historical data
        current = session.query(DailyMetric).filter_by(ticker=ticker).order_by(DailyMetric.date.desc()).first()
        d2_ago = session.query(DailyMetric).filter(DailyMetric.ticker == ticker, DailyMetric.date <= today - timedelta(days=2)).order_by(DailyMetric.date.desc()).first()
        d30_ago = session.query(DailyMetric).filter(DailyMetric.ticker == ticker, DailyMetric.date <= today - timedelta(days=30)).order_by(DailyMetric.date.desc()).first()

        if not current: return None

        base_score = 0
        
        # --- ENGINE 1: ANALYST VELOCITY ---
        if d2_ago and current.analyst_rating > d2_ago.analyst_rating:
            base_score += 20
        
        # --- ENGINE 2: VOLUME SPIKE ---
        if current.average_volume_30d > 0:
            rvol = current.volume / current.average_volume_30d
            if rvol > 2.0: base_score += 30

        # --- MULTIPLIERS ---
        multiplier = 1.0
        if current.short_float_pct > 10: multiplier *= 1.2
        
        final_score = base_score * multiplier
        return {"ticker": ticker, "score": round(final_score, 2)}

    finally:
        session.close()

if __name__ == "__main__":
    logger.info("--- Scout Analysis Starting ---")
    session = SessionLocal()
    tickers = [t.ticker for t in session.query(DailyMetric.ticker).distinct().all()]
    for t in tickers:
        result = get_scout_score(t)
        if result:
            logger.info(f"RESULT: {result['ticker']} | Score: {result['score']}")