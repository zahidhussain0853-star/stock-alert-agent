import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from main import DailyMetric, Base, load_dotenv

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL").replace("postgres://", "postgresql://", 1))
Session = sessionmaker(bind=engine)

def inject_crystal_scenario():
    session = Session()
    today = datetime.now().date()
    ticker = "SCOUT_TEST"
    
    # 1. Clear old test data
    session.query(DailyMetric).filter_by(ticker=ticker).delete()

    # 2. CREATE THE "SNAPSHOTS"
    # 30 Days Ago: A boring "Hold"
    session.add(DailyMetric(ticker=ticker, date=today-timedelta(days=30), analyst_rating=3.4, sentiment_score=-0.2, volume=1000, average_volume_30d=1000))
    
    # 10 Days Ago: Slight improvement
    session.add(DailyMetric(ticker=ticker, date=today-timedelta(days=10), analyst_rating=3.2, sentiment_score=-0.1, volume=1000, average_volume_30d=1000))
    
    # 2 Days Ago: The "Hold" before the Snap
    session.add(DailyMetric(ticker=ticker, date=today-timedelta(days=2), analyst_rating=3.1, sentiment_score=-0.1, volume=1000, average_volume_30d=1000))

    # TODAY: THE PERFECT STORM
    session.add(DailyMetric(
        ticker=ticker,
        date=today,
        analyst_rating=2.9,          # Triggers: WHISPER (+10), DELTA (+10), THE_SNAP (+20) = 40pts
        sentiment_score=0.3,         # Triggers: VIBE_SHIFT (+10), NARRATIVE_ACCEL (+10) = 20pts
        volume=4000,                 # Triggers: VOLUME_ROCKET (4.0x RVOL) = 30pts
        average_volume_30d=1000,
        call_put_ratio=4.0,          # Triggers: CALL_SPIKE = 10pts
        short_float_pct=12.0,        # Multiplier: SQUEEZE_FUEL (1.25x)
        bb_width_30d_low=True,       # Multiplier: VOL_COIL (1.15x)
        rs_slope_5d=0.5              # Multiplier: RS_CONFIRMED (1.10x)
    ))

    session.commit()
    print(f"--- {ticker} INJECTED: 100 Base Pts + All Multipliers ---")

if __name__ == "__main__":
    inject_crystal_scenario()