import yfinance as yf
import pandas as pd
import numpy as np
import os
import psycopg2
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
TICKERS_MAPPING = {
    "AAPL": "XLK", "MSFT": "XLK", "NVDA": "XLK", "AVGO": "XLK", "AMD": "XLK",
    "ORCL": "XLK", "PLTR": "XLK", "CRM": "XLK", "ADBE": "XLK", "IBM": "XLK",
    "LLY": "XLV", "UNH": "XLV", "JNJ": "XLV", "ABBV": "XLV", "MRK": "XLV",
    "TMO": "XLV", "AMGN": "XLV", "ISRG": "XLV", "PFE": "XLV", "GILD": "XLV",
    "JPM": "XLF", "GS": "XLF", "V": "XLF", "MA": "XLF", "BAC": "XLF",
    "WFC": "XLF", "AXP": "XLF", "MS": "XLF", "BLK": "XLF", "SCHW": "XLF",
    "AMZN": "XLY", "TSLA": "XLY", "MCD": "XLY", "HD": "XLY", "NKE": "XLY",
    "BKNG": "XLY", "SBUX": "XLY", "TJX": "XLY", "LOW": "XLY", "MAR": "XLY",
    "XOM": "XLE", "CVX": "XLE", "COP": "XLE", "GE": "XLI", "CAT": "XLI",
    "HON": "XLI", "BA": "XLI", "WMT": "XLP", "COST": "XLP", "LIN": "XLB"
}

analyzer = SentimentIntensityAnalyzer()

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

import feedparser # Add this to requirements.txt

def get_sentiment(ticker):
    try:
        # Try Yahoo First
        stock = yf.Ticker(ticker)
        news = stock.news
        titles = [n.get('title', '') for n in news[:5]]
        
        # Fallback: Google News RSS if Yahoo is empty
        if not titles:
            rss_url = f"https://news.google.com/rss/search?q={ticker}+stock+when:1d"
            feed = feedparser.parse(rss_url)
            titles = [entry.title for entry in feed.entries[:5]]
            
        if not titles: return 0.0
        
        scores = [analyzer.polarity_scores(t)['compound'] for t in titles]
        return float(np.mean(scores))
    except:
        return 0.0

def check_rs(symbol, sector_etf):
    try:
        data = yf.download([symbol, sector_etf], period="1mo", progress=False)['Close']
        if data.empty or len(data) < 10: return False
        returns = data.pct_change(9).iloc[-1]
        return bool(returns[symbol] > returns[sector_etf])
    except: return False

def get_last_rating(cur, symbol):
    cur.execute("SELECT last_rating FROM analyst_ratings WHERE symbol = %s", (symbol,))
    row = cur.fetchone()
    return float(row[0]) if row else 3.0

def save_signals_to_db(cur, results):
    query = """
        INSERT INTO quant_signals (
            symbol, price, analyst_transition, news_sentiment, 
            rs_status, insider_buying, short_float_pct, 
            final_score, signal_label
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for res in results:
        cur.execute(query, (
            str(res['Ticker']), float(res['Price']), str(res['Consensus']),
            float(res['News']), str(res['RS']), bool(res['Insider'] == "YES"),
            float(res['Squeeze']), int(res['Score']), str(res['Signal'])
        ))

def run_screener():
    conn = get_db_connection()
    cur = conn.cursor()
    results = []

    print("🚀 Starting Production Scan...")

    for symbol, sector_etf in TICKERS_MAPPING.items():
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # 1. Price & Ratings
            price = info.get('regularMarketPrice') or info.get('currentPrice') or 0.0
            curr_rating = info.get("recommendationMean", 3.0)
            prev_rating = get_last_rating(cur, symbol)
            
            # 2. Contextual Data
            sentiment = get_sentiment(stock)
            is_leader = check_rs(symbol, sector_etf)
            short_pct = (info.get("shortPercentOfFloat", 0) * 100) if info.get("shortPercentOfFloat") else 0.0
            
            # 3. Insider Check
            try:
                insider = stock.insider_transactions
                insider_buy = any("Purchase" in str(x) for x in insider['Transaction'].head(5)) if insider is not None and not insider.empty else False
            except: insider_buy = False

            # 4. Scoring Logic
            score = 0
            # +30 for Upgrade (Current rating is lower/better than previous)
            if curr_rating < prev_rating: score += 30
            if sentiment > 0.1: score += 20
            if is_leader: score += 20
            if insider_buy: score += 20
            if short_pct > 10: score += 10

            signal = "💎 CRYSTAL" if score >= 70 else "✅ CONVICTION" if score >= 45 else "ℹ️ NEUTRAL"

            results.append({
                "Ticker": symbol, "Price": price, "Consensus": f"{prev_rating}→{curr_rating}",
                "News": sentiment, "RS": "Leader" if is_leader else "Lag",
                "Insider": "YES" if insider_buy else "No", "Squeeze": short_pct,
                "Score": score, "Signal": signal
            })

            # Update the rating memory for the next run
            cur.execute("""
                INSERT INTO analyst_ratings (symbol, last_rating, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (symbol) DO UPDATE SET last_rating = EXCLUDED.last_rating, updated_at = CURRENT_TIMESTAMP
            """, (symbol, curr_rating))
            
            print(f"Processed {symbol} | Score: {score}")

        except Exception as e:
            print(f"Error skipping {symbol}: {e}")

    if results:
        save_signals_to_db(cur, results)
        conn.commit()
        print(f"✅ Archived {len(results)} signals to PostgreSQL.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_screener()