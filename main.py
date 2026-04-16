import yfinance as yf
import pandas as pd
import psycopg2
import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os
import feedparser
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
TICKERS = ['AAPL', 'MSFT', 'NVDA', 'AVGO', 'GOOGL', 'AMZN', 'META', 'BRK-B', 'LLY', 'V', 
           'TSLA', 'WMT', 'JPM', 'UNH', 'MA', 'ORCL', 'COST', 'XOM', 'HD', 'PG', 
           'NFLX', 'JNJ', 'BAC', 'ABBV', 'SAP', 'CRM', 'WFC', 'KO', 'DIS', 'ADBE', 
           'CSCO', 'TMUS', 'MRK', 'TMO', 'ACN', 'AMD', 'PFE', 'LIN', 'PEP', 'ABT', 
           'MCD', 'INTC', 'HON', 'NKE', 'CAT', 'TXN', 'QCOM', 'SBUX', 'LOW']

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_sentiment(ticker):
    analyzer = SentimentIntensityAnalyzer()
    titles = []
    try:
        time.sleep(1.0) 
        stock = yf.Ticker(ticker)
        yf_news = stock.news
        if yf_news:
            for n in yf_news[:5]:
                t = n.get('title') or n.get('headline')
                if t: titles.append(t)
    except:
        pass

    if len(titles) < 3:
        try:
            rss_url = f"https://news.google.com/rss/search?q={ticker}+stock+when:1d&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(rss_url)
            for entry in feed.entries[:5]:
                if entry.title not in titles:
                    titles.append(entry.title)
        except:
            pass

    if not titles: return 0.0
    scores = [analyzer.polarity_scores(t)['compound'] for t in titles]
    return round(sum(scores) / len(scores), 2)

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
    
    # Ensure Table exists with new columns
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
            free_cash_flow BIGINT,
            operating_margin FLOAT,
            debt_to_equity FLOAT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    print(f"🚀 Starting Scan at {datetime.now()}")

    for symbol in TICKERS:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # 1. Price & Volume
            price = info.get('currentPrice', 0)
            avg_vol = info.get('averageVolume', 1)
            curr_vol = info.get('regularMarketVolume', 0)
            vol_delta = curr_vol / avg_vol if avg_vol > 0 else 1
            
            # 2. Fundamental Pillars (NEW)
            fcf = info.get('freeCashflow', 0)
            op_margin = info.get('operatingMargins', 0)
            debt_equity = info.get('debtToEquity', 0)
            
            # Sanitize None values
            fcf = fcf if fcf is not None else 0
            op_margin = op_margin if op_margin is not None else 0
            debt_equity = debt_equity if debt_equity is not None else 0

            # 3. Analyst Logic
            raw_curr = info.get('recommendationMean')
            num_analysts = info.get('numberOfAnalystOpinions', 0)
            curr_rating = float(raw_curr) if raw_curr is not None else 3.0
            
            cur.execute("SELECT raw_rating FROM quant_signals WHERE symbol = %s", (symbol,))
            prev_row = cur.fetchone()
            prev_rating = float(prev_row[0]) if prev_row and prev_row[0] is not None else curr_rating 

            # --- SCORING ALGORITHM ---
            # Base Score (Inverted: 1.0 is best)
            base_score = (5.0 - curr_rating) * 10 
            score = base_score
            
            if curr_rating < prev_rating: score += 20 # Upgrade boost
            
            sentiment = get_sentiment(symbol)
            if sentiment > 0.1: score += 20
            if vol_delta > 1.5: score += 10
            
            rs_status = get_relative_strength(symbol)
            if rs_status == "Leader": score += 20
            
            # New Fundamental Bonus (Maximum +15 pts)
            if op_margin > 0.25: score += 10    # Efficiency Reward
            if 0 < debt_equity < 100: score += 5  # Balance Sheet Reward
            
            short_pct = info.get('shortPercentOfFloat', 0) * 100
            if short_pct > 10: score += 10
            
            insider_data = stock.insider_transactions
            insider_buy = False
            if insider_data is not None and not insider_data.empty:
                insider_buy = any("Purchase" in str(x) for x in insider_data['Transaction'].head(5))
            if insider_buy: score += 10

            score = min(max(round(score, 0), 0), 100)
            label = "💎 Crystal" if score >= 70 else "✅ Conviction" if score >= 45 else "ℹ️ Neutral"
            transition_text = f"{prev_rating:.1f} → {curr_rating:.1f}"

            # 5. UPSERT (Update columns including new fundamental pillars)
            cur.execute("""
                INSERT INTO quant_signals 
                (symbol, price, final_score, signal_label, analyst_transition, 
                 news_sentiment, volume_delta, insider_buying, short_float_pct, 
                 rs_status, num_analysts, raw_rating, prev_raw_rating, 
                 free_cash_flow, operating_margin, debt_to_equity, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
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
                    free_cash_flow = EXCLUDED.free_cash_flow,
                    operating_margin = EXCLUDED.operating_margin,
                    debt_to_equity = EXCLUDED.debt_to_equity,
                    timestamp = CURRENT_TIMESTAMP;
            """, (symbol, price, score, label, transition_text, sentiment, 
                  vol_delta, insider_buy, short_pct, rs_status, num_analysts, 
                  curr_rating, prev_rating, fcf, op_margin, debt_equity))
            
            conn.commit()
            print(f"✅ {symbol} processed | Score: {score} | Margin: {op_margin:.2%}")

        except Exception as e:
            conn.rollback()
            print(f"❌ Error scanning {symbol}: {e}")

    cur.close()
    conn.close()
    print("🏁 Scan Complete.")

if __name__ == "__main__":
    run_scanner()