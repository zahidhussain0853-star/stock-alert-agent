import os
import requests
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import DailyMetric  # Ensure this matches your main.py filename

# 1. Force immediate output to Railway logs
def log_debug(msg):
    print(f"DEBUG_INGEST: {msg}", flush=True)

# 2. Database Setup
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

def fetch_and_save(ticker):
    log_debug(f"Starting fetch for {ticker}...")
    
    if not API_KEY:
        log_debug("FAILED: ALPHA_VANTAGE_KEY is missing from Variables!")
        return

    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # --- API FETCH ---
        log_debug(f"Calling Alpha Vantage for {ticker}...")
        url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}'
        response = requests.get(url)
        data = response.json()

        # Check for the dreaded "Rate Limit" message
        if "Symbol" not in data:
            log_debug(f"API ERROR for {ticker}: {data}")
            return

        log_debug(f"API SUCCESS: Found {ticker}. Mapping to DB...")

        # --- DATA MAPPING ---
        # Note: Using your exact column 'average_volume_30d'
        new_entry = DailyMetric(
            ticker=ticker,
            date=datetime.now().date(),
            analyst_rating=float(data.get('AnalystRatingStrongBuy', 0)) + float(data.get('AnalystRatingBuy', 0)),
            sentiment_score=0.0,
            volume=1000000, # Placeholder for test if Quote API fails
            average_volume_30d=int(data.get('AverageVolume', 0)) if data.get('AverageVolume') else 0,
            call_put_ratio=1.0,
            short_float_pct=0.0,
            bb_width_30d_low=False,
            rs_slope_5d=0.0
        )

        # --- THE COMMIT ---
        log_debug(f"Attempting to commit {ticker} to DB...")
        
        # Simple Add for debug (ignoring upsert for a second to ensure it works)
        session.add(new_entry)
        session.commit()
        
        log_debug(f"DATABASE SUCCESS: {ticker} is now in the table.")
        session.close()

    except Exception as e:
        log_debug(f"CRITICAL EXCEPTION: {str(e)}")

if __name__ == "__main__":
    log_debug("Script Wake Up.")
    # Test with just one ticker to save API calls
    fetch_and_save("NVDA")