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
    
    # 1. Ensure Table and Unique Constraint (Crucial for fixing the 1011 row issue)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS quant_signals (
            symbol TEXT PRIMARY KEY,
            price FLOAT,
            final_score FLOAT,
            signal_label TEXT,
            analyst_transition TEXT,
            news_sentiment FLOAT,
            volume_delta FLOAT,
            insider_buying BOOLEAN,
            short_float_pct FLOAT,
            rs_status TEXT,
            num_analysts INT,
            raw_rating FLOAT,
            prev_raw_rating FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    print(f"🚀 Starting Scan at {datetime.now()}")

    for symbol in TICKERS:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # --- 1. DATA GATHERING ---
            price = info.get('currentPrice', 0)
            avg_vol = info.get('averageVolume', 1)
            curr_vol = info.get('regularMarketVolume', 0)
            vol_delta = curr_vol / avg_vol if avg_vol > 0 else 1
            
            raw_curr = info.get('recommendationMean')
            num_analysts = info.get('numberOfAnalystOpinions', 0)
            curr_rating = float(raw_curr) if raw_curr is not None else 3.0
            
            # --- 2. ANALYST MEMORY LOGIC ---
            cur.execute("SELECT raw_rating FROM quant_signals WHERE symbol = %s", (symbol,))
            prev_row = cur.fetchone()
            prev_rating = float(prev_row[0]) if prev_row and prev_row[0] is not NULL else curr_rating 
            
            # --- 3. SCORING ALGORITHM (Stepwise Refined) ---
            score = 0
            
            # THE FLOOR: Give points based on rating (1.0 is best, 5.0 is worst)
            # Math: (5.0 - 2.5 Buy Rating) * 10 = 25 base points.
            base_score = (5.0 - curr_rating) * 10
            score += base_score

            # THE BOOSTS
            if curr_rating < prev_rating: score += 20  # Analyst Upgrade
            if get_sentiment(symbol) > 0.1: score += 20
            if vol_delta > 1.5: score += 10
            if get_relative_strength(symbol) == "Leader": score += 20
            
            # Insider/Short logic
            short_pct = info.get('shortPercentOfFloat', 0) * 100
            if short_pct > 10: score += 10
            
            insider_data = stock.insider_transactions
            insider_buy = False
            if insider_data is not None and not insider_data.empty:
                insider_buy = any("Purchase" in str(x) for x in insider_data['Transaction'].head(5))
            if insider_buy: score += 10

            score = min(round(score, 0), 100)
            label = "💎 Crystal" if score >= 70 else "✅ Conviction" if score >= 45 else "ℹ️ Neutral"

            # --- 4. UPSERT LOGIC (The "Fix" for 1011 rows) ---
            cur.execute("""
                INSERT INTO quant_signals 
                (symbol, price, final_score, signal_label, analyst_transition, 
                 news_sentiment, volume_delta, insider_buying, short_float_pct, 
                 rs_status, num_analysts, raw_rating, prev_raw_rating, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (symbol) DO UPDATE SET
                    price = EXCLUDED.price,
                    final_score = EXCLUDED.final_score,
                    signal_label = EXCLUDED.signal_label,
                    analyst_transition = EXCLUDED.analyst_transition,
                    news_sentiment = EXCLUDED.news_sentiment,
                    volume_delta = EXCLUDED.volume_delta,
                    insider_buying = EXCLUDED.insider_buying,
                    short_float_pct = EXCLUDED.short_float_pct,
                    rs_status = EXCLUDED.rs_status,
                    num_analysts = EXCLUDED.num_analysts,
                    raw_rating = EXCLUDED.raw_rating,
                    prev_raw_rating = EXCLUDED.prev_raw_rating,
                    timestamp = CURRENT_TIMESTAMP;
            """, (symbol, price, score, label, f"{prev_rating:.1f} → {curr_rating:.1f}", 
                  get_sentiment(symbol), vol_delta, insider_buy, short_pct, 
                  get_relative_strength(symbol), num_analysts, curr_rating, prev_rating))
            
            conn.commit()
            print(f"✅ {symbol} | Final Score: {score}")

        except Exception as e:
            print(f"❌ Error scanning {symbol}: {e}")

    cur.close()
    conn.close()
    print("🏁 Scan Complete.")

if __name__ == "__main__":
    run_scanner()