import os
import requests
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import DailyMetric 

# --- FORCE CONSOLE PRINT ---
def ingest_print(msg):
    print(f"INGEST_SERVICE: {msg}", flush=True)

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

def fetch_and_save(ticker):
    ingest_print(f"Connecting to Alpha Vantage for {ticker}...")
    
    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()

        ov_res = requests.get(f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}').json()
        q_res = requests.get(f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={API_KEY}').json()
        quote = q_res.get("Global Quote", {})

        if "Symbol" not in ov_res:
            ingest_print(f"API REJECTED {ticker}: Check rate limits.")
            return

        new_entry = DailyMetric(
            ticker=ticker,
            date=datetime.now().date(),
            analyst_rating=float(ov_res.get('AnalystRatingStrongBuy', 0)) + float(ov_res.get('AnalystRatingBuy', 0)),
            sentiment_score=0.0,
            volume=int(quote.get('06. volume', 0)) if quote.get('06. volume') else 0,
            average_volume_30d=int(ov_res.get('AverageVolume', 0)) if ov_res.get('AverageVolume') else 0,
            call_put_ratio=1.0,
            short_float_pct=0.0,
            bb_width_30d_low=False,
            rs_slope_5d=0.0
        )

        # Upsert
        existing = session.query(DailyMetric).filter_by(ticker=ticker, date=new_entry.date).first()
        if existing:
            ingest_print(f"Updating data for {ticker}...")
            for col in DailyMetric.__table__.columns:
                if col.name != 'id': setattr(existing, col.name, getattr(new_entry, col.name))
        else:
            ingest_print(f"Inserting new data for {ticker}...")
            session.add(new_entry)

        session.commit()
        ingest_print(f"SUCCESS: {ticker} committed to DB.")
        session.close()

    except Exception as e:
        ingest_print(f"CRITICAL ERROR: {str(e)}")

if __name__ == "__main__":
    ingest_print("Ingest process started.")
    watchlist = ["NVDA", "AAPL", "TSLA"]
    for symbol in watchlist:
        fetch_and_save(symbol)