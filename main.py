import os
from sqlalchemy import create_engine, Column, Integer, String, Date, Float, Boolean, Numeric
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timedelta

# Database Connection
DATABASE_URL = os.getenv("DATABASE_URL") # Railway provides this automatically
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class DailyMetric(Base):
    __tablename__ = "daily_metrics"
    id = Column(Integer, primary_key=True)
    ticker = Column(String)
    date = Column(Date)
    analyst_rating = Column(Float)  # Maps to 'real'
    sentiment_score = Column(Float)
    volume = Column(Integer)        # Maps to 'bigint'
    avg_volume_30d = Column(Integer)
    call_put_ratio = Column(Float)
    short_float_pct = Column(Float)
    bb_width_30d_low = Column(Boolean)
    rs_slope_5d = Column(Float)

def get_scout_score(ticker):
    session = SessionLocal()
    today = datetime.now().date()
    
    # 1. Fetch current and historical data
    current = session.query(DailyMetric).filter_by(ticker=ticker).order_by(DailyMetric.date.desc()).first()
    d2_ago = session.query(DailyMetric).filter(DailyMetric.ticker == ticker, DailyMetric.date <= today - timedelta(days=2)).order_by(DailyMetric.date.desc()).first()
    d30_ago = session.query(DailyMetric).filter(DailyMetric.ticker == ticker, DailyMetric.date <= today - timedelta(days=30)).order_by(DailyMetric.date.desc()).first()

    if not current:
        return "No data found for ticker."

    base_score = 0
    
    # --- ENGINE 1: ANALYST DECIMAL VELOCITY (40 pts) ---
    if d30_ago:
        # Move toward 1.0 (Buy) is positive delta
        total_delta = d30_ago.analyst_rating - current.analyst_rating
        if total_delta >= 0.3: base_score += 10
        
        if d2_ago:
            recent_snap = d2_ago.analyst_rating - current.analyst_rating
            if recent_snap >= 0.1: base_score += 10
            
            # The Threshold Cross (e.g., 3.1 to 2.9)
            if d2_ago.analyst_rating > 3.0 and current.analyst_rating <= 3.0:
                base_score += 20

    # --- ENGINE 2: SENTIMENT ARC (20 pts) ---
    if d2_ago:
        if d2_ago.sentiment_score < 0 and current.sentiment_score >= 0:
            base_score += 10 # Vibe Shift
        if (current.sentiment_score - d2_ago.sentiment_score) >= 0.2:
            base_score += 10 # Acceleration

    # --- ENGINE 3: VOLUME/SUPPLY (40 pts) ---
    rvol = current.volume / current.avg_volume_30d if current.avg_volume_30d else 0
    if rvol >= 3.0: base_score += 30
    if current.call_put_ratio >= 3.0: base_score += 10

    # --- MULTIPLIERS ---
    multiplier = 1.0
    if current.short_float_pct > 10: multiplier *= 1.25
    if current.bb_width_30d_low: multiplier *= 1.15
    if current.rs_slope_5d > 0: multiplier *= 1.10

    final_score = base_score * multiplier
    
    return {
        "ticker": ticker,
        "base_score": base_score,
        "multiplier": round(multiplier, 2),
        "final_score": round(final_score, 2),
        "status": "💎 CRYSTAL" if final_score >= 105 else "HIGH CONVICTION" if final_score >= 90 else "NEUTRAL"
    }

# Example usage
# print(get_scout_score("NVDA"))