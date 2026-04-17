import os
import requests
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# Import the DailyMetric class from your main.py
from main import DailyMetric, SessionLocal 

API_KEY = "YOUR_ALPHA_VANTAGE_KEY"

def fetch_and_save(ticker):
    session = SessionLocal()
    
    # --- A. Fetch Analyst Consensus & Volume ---
    # Using 'OVERVIEW' for fundamental data and 'TIME_SERIES_DAILY' for volume
    overview_url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}'
    quote_url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={API_KEY}'
    sentiment_url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&limit=5&apikey={API_KEY}'

    ov_data = requests.get(overview_url).json()
    quote_data = requests.get(quote_url).json()['Global Quote']
    sent_data = requests.get(sentiment_url).json()

    # --- B. Parse Sentiment Score ---
    # Alpha Vantage provides a score between -1 and 1
    articles = sent_data.get('feed', [])
    avg_sentiment = sum([float(a['ticker_sentiment'][0]['ticker_sentiment_score']) for a in articles]) / len(articles) if articles else 0

    # --- C. Create the Scout Entry ---
    new_entry = DailyMetric(
        ticker=ticker,
        date=datetime.now().date(),
        # Alpha Vantage Analyst Rating is often in the 'AnalystTargetPrice' 
        # For the 'Decimal Rating', we usually map their 'Recommedation' if available
        # or use a placeholder if the free tier Overview is restricted.
        analyst_rating=float(ov_data.get('AnalystRatingStrongBuy', 0)) * 1 + float(ov_data.get('AnalystRatingBuy', 0)) * 2, # Custom mapping logic
        sentiment_score=avg_sentiment,
        volume=int(quote_data.get('06. volume', 0)),
        avg_volume_30d=int(ov_data.get('AverageVolume', 0)), # From Overview
        call_put_ratio=1.0, # Options data often requires a paid tier; default to neutral
        short_float_pct=float(ov_data.get('PercentVisible', 0)), # Proxy for short interest
        bb_width_30d_low=False, # Calculate this if you add technical analysis lib
        rs_slope_5d=0.0
    )

    try:
        session.merge(new_entry) # Merge handles the 'Unique Constraint' by updating if exists
        session.commit()
        print(f"Successfully scouted {ticker}")
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    tickers_to_watch = ["NVDA", "AAPL", "TSLA"]
    for t in tickers_to_watch:
        fetch_and_save(t)