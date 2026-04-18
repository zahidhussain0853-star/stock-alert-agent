import os
import sys
from sqlalchemy import create_engine, Column, Integer, String, Date, Float, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

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
    volume = Column(BigInteger if 'BigInteger' in globals() else Integer) # Handles large vol
    average_volume_30d = Column(Integer)
    call_put_ratio = Column(Float)
    short_float_pct = Column(Float)
    bb_width_30d_low = Column(Boolean)
    rs_slope_5d = Column(Float)

def calculate_scout_score(ticker, session):
    today = datetime.now().date()
    
    # Snapshots: Today, 2D (Velocity), 30D (Baseline)
    curr = session.query(DailyMetric).filter_by(ticker=ticker).order_by(DailyMetric.date.desc()).first()
    d2 = session.query(DailyMetric).filter(DailyMetric.ticker == ticker, DailyMetric.date <= today - timedelta(days=2)).order_by(DailyMetric.date.desc()).first()
    d30 = session.query(DailyMetric).filter(DailyMetric.ticker == ticker, DailyMetric.date <= today - timedelta(days=30)).order_by(DailyMetric.date.desc()).first()

    if not curr: return None
    
    raw_score = 0
    signals = []

    # ENGINE 1: ANALYST VELOCITY (Normalization check)
    # v5.0 detects if the trend is accelerating
    if d2 and curr.analyst_rating > d2.analyst_rating:
        velocity = (curr.analyst_rating - d2.analyst_rating) / d2.analyst_rating
        if velocity > 0.05: # >5% growth in 48 hours
            raw_score += 35
            signals.append("V5_VELOCITY_CRITICAL")
        else:
            raw_score += 20
            signals.append("SENTIMENT_POP")

    # ENGINE 2: RS SLOPE (The Trend Filter)
    # Prevents "Catching a Falling Knife"
    if curr.rs_slope_5d and curr.rs_slope_5d > 0:
        raw_score += 25
        signals.append("POSITIVE_TREND_CONFIRMED")
    elif curr.rs_slope_5d and curr.rs_slope_5d < -0.5:
        raw_score -= 20 # Negative penalty for bad trend
        signals.append("BEARISH_DIVERGENCE")

    # ENGINE 3: THE RVOL MULTIPLIER (The v5.0 "Force" logic)
    rvol = 1.0
    if curr.average_volume_30d and curr.average_volume_30d > 0:
        rvol = curr.volume / curr.average_volume_30d
        if rvol > 2.0:
            raw_score += 40
            signals.append("INSTITUTIONAL_ACCUMULATION")
        elif rvol > 1.5:
            raw_score += 25
            signals.append("VOLUME_MOMENTUM")

    # --- FINAL CALCULATION ---
    # Apply Squeeze Multiplier (Short Float > 15%)
    final_score = raw_score
    if curr.short_float_pct and curr.short_float_pct > 15:
        final_score *= 1.2
        signals.append("SQUEEZE_BOOST")

    return {
        "ticker": ticker,
        "score": round(final_score, 2),
        "signals": signals,
        "action": "🔥 STRONG BUY" if final_score >= 80 else "✅ BUY" if final_score >= 65 else "👀 WATCH" if final_score >= 40 else "❄️ HOLD"
    }

if __name__ == "__main__":
    print(f"--- SCOUT REPORT: {datetime.now().strftime('%Y-%m-%d')} ---")
    session = SessionLocal()
    try:
        tickers = [t.ticker for t in session.query(DailyMetric.ticker).distinct().all()]
        for t in tickers:
            res = calculate_scout_score(t, session)
            if res:
                print(f"{res['action']} | {res['ticker']} | Score: {res['score']} | Signals: {res['signals']}")
    except Exception as e:
        print(f"REPORT ERROR: {e}")
    finally:
        session.close()