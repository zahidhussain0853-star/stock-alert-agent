import yfinance as yf
import pandas as pd
import psycopg2
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os
from datetime import datetime
from dotenv import load_dotenv

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
    try:
        stock = yf.Ticker(ticker)
        news = stock.news
        if not news:
            return 0.0
        
        scores = []
        for n in news[:5]:
            vs = analyzer.polarity_scores(n['title'])
            scores.append(vs['compound'])
        return sum(scores) / len(scores)
    except:
        return 0.0

def get_relative_strength(ticker):
    try:
        # RS relative to Sector or SPY (simplified to SPY here)
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
    
    # 2. SELF-HEALING: Add new columns if they are missing from previous versions
    columns_to_add = {
        "num_analysts": "INT",
        "raw_rating": "FLOAT"
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
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # 1. Price & Volume
            price = info.get('currentPrice', 0)
            avg_vol = info.get('averageVolume', 1)
            curr_vol = info.get('regularMarketVolume', 0)
            vol_delta = curr_vol / avg_vol if avg_vol > 0 else 1
            
            # 2. Analyst Memory Logic
            curr_rating = info.get('recommendationMean', 3.0)
            num_analysts = info.get('numberOfAnalystOpinions', 0)
            
            cur.execute("SELECT raw_rating FROM quant_signals WHERE symbol = %s ORDER BY timestamp DESC LIMIT 1", (symbol,))
            prev_row = cur.fetchone()
            prev_rating = prev_row[0] if prev_row else curr_rating
            
            transition_bonus = 30 if curr_rating < prev_rating else 0
            transition_text = f"{prev_rating} → {curr_rating}"

            # 3. Sentiment & RS
            sentiment = get_sentiment(symbol)
            rs_status = get_relative_strength(symbol)
            
            # 4. Squeeze & Insider
            short_pct = info.get('shortPercentOfFloat', 0) * 100
            
            insider_data = stock.insider_transactions
            insider_buy = False
            if insider_data is not None and not insider_data.empty:
                insider_buy = any("Purchase" in str(x) for x in insider_data['Transaction'].head(5))

            # --- SCORING ALGORITHM ---
            score = 0
            score += transition_bonus
            if sentiment > 0.1: score += 20
            if vol_delta > 1.5: score += 10
            if rs_status == "Leader": score += 20
            if insider_buy: score += 10
            if short_pct > 10: score += 10
            
            label = "💎 Crystal" if score >= 70 else "✅ Conviction" if score >= 45 else "ℹ️ Neutral"

            # 5. Save to DB
            cur.execute("""
                INSERT INTO quant_signals 
                (symbol, price, final_score, signal_label, analyst_transition, 
                 news_sentiment, volume_delta, insider_buying, short_float_pct, 
                 rs_status, num_analysts, raw_rating)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (symbol, price, score, label, transition_text, sentiment, 
                  vol_delta, insider_buy, short_pct, rs_status, num_analysts, curr_rating))
            
            conn.commit()
            print(f"✅ {symbol} processed | Score: {score}")

        except Exception as e:
            print(f"❌ Error scanning {symbol}: {e}")

    cur.close()
    conn.close()
    print("🏁 Scan Complete.")

if __name__ == "__main__":
    run_scanner()