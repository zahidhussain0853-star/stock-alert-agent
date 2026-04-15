import yfinance as yf
import pandas as pd
import numpy as np
import os
import psycopg2
import feedparser
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv

load_dotenv()
analyzer = SentimentIntensityAnalyzer()

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

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_sentiment(ticker_symbol, ticker_obj):
    titles = []
    try:
        # 1. Try Yahoo (using the object)
        news = ticker_obj.news
        if news:
            titles = [n.get('title', '') for n in news[:5]]
        
        # 2. Stronger Fallback: Google News with User-Agent
        if not titles:
            rss_url = f"https://news.google.com/rss/search?q={ticker_symbol}+stock+news&hl=en-US&gl=US&ceid=US:en"
            # We add a User-Agent to look like a real browser
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
            response = requests.get(rss_url, headers=headers, timeout=10)
            feed = feedparser.parse(response.content)
            titles = [entry.title for entry in feed.entries[:5]]
            
        if not titles:
            return 0.0
            
        scores = [analyzer.polarity_scores(t)['compound'] for t in titles]
        sentiment_avg = float(np.mean(scores))
        
        # Railway Log check
        if sentiment_avg != 0:
            print(f"✅ {ticker_symbol} Sentiment: {sentiment_avg:.2f}")
            
        return sentiment_avg
    except Exception as e:
        print(f"Sentiment Error for {ticker_symbol}: {e}")
        return 0.0

def check_rs(symbol, sector_etf):
    try:
        data = yf.download([symbol, sector_etf], period="1mo", progress=False)['Close']
        if data.empty or len(data) < 10: return False
        returns = data.pct_change(9).iloc[-1]
        return bool(returns[symbol] > returns[sector_etf])
    except:
        return False

def get_last_rating(cur, symbol):
    try:
        cur.execute("SELECT last_rating FROM analyst_ratings WHERE symbol = %s", (symbol,))
        row = cur.fetchone()
        return float(row[0]) if row else 3.0
    except:
        return 3.0

def save_signals_to_db(cur, results):
    query = """
        INSERT INTO quant_signals (
            symbol, price, analyst_transition, news_sentiment, 
            rs_status, insider_buying, short_float_pct, 
            final_score, signal_label, volume_delta
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for res in results:
        cur.execute(query, (
            str(res['Ticker']), float(res['Price']), str(res['Consensus']),
            float(res['News']), str(res['RS']), False, 0.0,
            int(res['Score']), str(res['Signal']), float(res['VolDelta'])
        ))

def run_screener():
    conn = get_db_connection()
    cur = conn.cursor()
    results = []

    print("🚀 Starting Production Scan...")

    for symbol, sector_etf in TICKERS_MAPPING.items():
        # Initialize defaults to prevent Scoping Errors
        price, curr_rating, vol_delta, sentiment, is_leader = 0.0, 3.0, 1.0, 0.0, False
        
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            price = info.get('regularMarketPrice') or info.get('currentPrice') or 0.0
            curr_rating = info.get("recommendationMean", 3.0)
            avg_vol = info.get("averageVolume10days") or info.get("averageVolume") or 1.0
            curr_vol = info.get("regularMarketVolume") or 0.0
            vol_delta = float(curr_vol / avg_vol) if avg_vol > 0 else 1.0

            sentiment = get_sentiment(symbol, stock)
            is_leader = check_rs(symbol, sector_etf)
            prev_rating = get_last_rating(cur, symbol)
            
            # Scoring logic
            score = 0
            if curr_rating < prev_rating: score += 30
            if sentiment > 0.15: score += 20
            if is_leader: score += 20
            if vol_delta > 1.5: score += 10

            signal = "💎 CRYSTAL" if score >= 70 else "✅ CONVICTION" if score >= 45 else "ℹ️ NEUTRAL"

            results.append({
                "Ticker": symbol, "Price": price, 
                "Consensus": f"{prev_rating:.1f} → {curr_rating:.1f}",
                "News": sentiment, "RS": "Leader" if is_leader else "Lag",
                "Score": score, "Signal": signal, "VolDelta": vol_delta
            })

            cur.execute("""
                INSERT INTO analyst_ratings (symbol, last_rating) 
                VALUES (%s, %s) 
                ON CONFLICT (symbol) DO UPDATE SET last_rating = EXCLUDED.last_rating
            """, (symbol, curr_rating))
            
            print(f"Processed {symbol} | Score: {score} | Vol: {vol_delta:.2f}x")

        except Exception as e:
            print(f"Error skipping {symbol}: {e}")

    if results:
        save_signals_to_db(cur, results)
        conn.commit()
        print(f"✅ Successfully archived {len(results)} signals to PostgreSQL.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_screener()