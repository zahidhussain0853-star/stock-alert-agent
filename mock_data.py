import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from main import DailyMetric, Base  # Import your existing setup
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def inject_mock():
    session = Session()
    today = datetime.now().date()
    
    # Tickers to simulate
    tickers = {
        "NVDA": {"base_rating": 45, "base_vol": 50000000},
        "AAPL": {"base_rating": 30, "base_vol": 60000000},
        "TSLA": {"base_rating": 20, "base_vol": 40000000}
    }

    print("--- INJECTING 30 DAYS OF MOCK DATA ---")

    for ticker, stats in tickers.items():
        for i in range(31, 0, -1):  # Go back 31 days
            date = today - timedelta(days=i)
            
            # Simulated Growth for NVDA (to trigger the algorithm)
            rating_boost = (31 - i) * 0.5 if ticker == "NVDA" else 0
            vol_spike = 2.5 if (ticker == "NVDA" and i == 0) else 1.0 # Huge spike today
            
            mock_entry = DailyMetric(
                ticker=ticker,
                date=date,
                analyst_rating=stats["base_rating"] + rating_boost,
                volume=int(stats["base_vol"] * vol_spike),
                average_volume_30d=stats["base_vol"],
                sentiment_score=0.5,
                call_put_ratio=1.0,
                short_float_pct=16.0 if ticker == "NVDA" else 5.0, # Squeeze trigger
                bb_width_30d_low=False,
                rs_slope_5d=0.1
            )
            
            # Check if exists to avoid duplicates
            existing = session.query(DailyMetric).filter_by(ticker=ticker, date=date).first()
            if not existing:
                session.add(mock_entry)
        
        print(f"Injected historical trail for {ticker}")

    session.commit()
    session.close()
    print("--- INJECTION COMPLETE ---")

if __name__ == "__main__":
    inject_mock()