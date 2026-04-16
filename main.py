import yfinance as yf
import pandas as pd
import psycopg2
import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os
import feedparser
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import quote

load_dotenv()

# --- CONFIGURATION ---
TICKERS = ['AAPL', 'MSFT', 'NVDA', 'AVGO', 'GOOGL', 'AMZN', 'META', 'BRK-B', 'LLY', 'V', 
           'TSLA', 'WMT', 'JPM', 'UNH', 'MA', 'ORCL', 'COST', 'XOM', 'HD', 'PG', 
           'NFLX', 'JNJ', 'BAC', 'ABBV', 'SAP', 'CRM', 'WFC', 'KO', 'DIS', 'ADBE', 
           'CSCO', 'TMUS', 'MRK', 'TMO', 'ACN', 'AMD', 'PFE', 'LIN', 'PEP', 'ABT', 
           'MCD', 'AVGO', 'INTC', 'HON', 'NKE', 'CAT', 'TXN', 'QCOM', 'SBUX', 'LOW']

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_sentiment(ticker):
    analyzer = SentimentIntensityAnalyzer()
    titles = []
    
    # --- SOURCE 1: Yahoo Finance ---
    try:
        # Deep breath strategy for stability
        time.sleep(1.0) 
        stock = yf.Ticker(ticker)
        yf_news = stock.news
        if yf_news:
            for n in yf_news[:5]:
                # Safe get for title/headline to prevent KeyErrors
                t = n.get('title') or n.get('headline')
                if t: titles.append(t)
    except Exception as e:
        print(f"⚠️ Yahoo News failed for {ticker}, trying Google RSS...")

    # --- SOURCE 2: Google News RSS (Fallback/Supplement) ---
    if len(titles) < 3:
        try:
            rss_url = f"https://news.google.com/rss/search?q={ticker}+stock+when:1d&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(rss_url)
            for entry in feed.entries[:5]:
                if entry.title not in titles:
                    titles.append(entry.title)
        except Exception as e:
            print(f"⚠️ Google News RSS failed for {ticker}: {e}")

    # --- ANALYZE ALL GATHERED TITLES ---
    if not titles:
        return 0.0
        
    scores = [analyzer.polarity_scores(t)['compound'] for t in titles]
    avg_sentiment = sum(scores) / len(scores)
    
    return round(avg_sentiment, 2)
            

def get_relative_strength(ticker):
    try:
        data = yf.download([ticker, 'SPY'], period='1mo', progress=False)['Close']
        returns = data.pct_change().iloc[-1]
        return "Leader" if returns[ticker] > returns['SPY'] else "Lag"
    except:
        return "Lag"

def run_scanner():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS quant_signals (
            symbol TEXT,
            price FLOAT,
            final_score FLOAT,
            signal_label TEXT,
            analyst_transition TEXT,
            news_sentiment FLOAT,
            volume_delta FLOAT,
            insider_buying BOOLEAN,
            short_float_pct FLOAT,
            rs_status TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 2. SELF-HEALING: Add new columns if missing
    columns_to_add = {
        "num_analysts": "INT",
        "raw_rating": "FLOAT",
        "prev_raw_rating": "FLOAT" # <--- ADDED THIS NEW COLUMN
    }
    
    for col, col_type in columns_to_add.items():
        cur.execute(f"""
            DO $$ 
            BEGIN 
                BEGIN
                    ALTER TABLE quant_signals ADD COLUMN {col} {col_type};
                EXCEPTION
                    WHEN duplicate_column THEN RAISE NOTICE 'column {col} already exists';
                END;
            END $$;
        """)
    conn.commit()

    print(f"🚀 Starting Scan at {datetime.now()}")

    for symbol in TICKERS:
        try:
            stock