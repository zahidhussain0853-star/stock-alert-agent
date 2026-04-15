import yfinance as yf
import pandas as pd
import numpy as np
import os
import json
import psycopg2
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv

# Load .env for local testing (Railway uses its own Variables tab)
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

DB_FILE = "ratings_db.json"
analyzer = SentimentIntensityAnalyzer()

def get_db_connection():
    # Railway provides DATABASE_URL automatically in the variables
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_sentiment(ticker_obj):
    try:
        news = ticker_obj.news
        if not news: return 0
        scores = [analyzer.polarity_scores(n.get('title', ''))['compound'] for n in news[:5]]
        return np.mean(scores) if scores else 0
    except: return 0

def check_rs(symbol, sector_etf):
    try:
        data = yf.download([symbol, sector_etf], period="1mo", progress=False)['Close']
        if data.empty or len(data) < 11: return False
        returns = data.pct_change(10).iloc[-1]
        return returns[symbol] > returns[sector_etf]
    except: return False

def save_to_db(results):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
            INSERT INTO quant_signals (
                symbol, price, analyst_transition, news_sentiment, 
                rs_status, insider_buying, short_float_pct, 
                final_score, signal_label
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for res in results:
            data = (
                res['Ticker'],
                res['Price'],
                res['Consensus'],
                res['News'],
                res['RS'],
                res['Insider'] == "YES",
                float(res['Squeeze'].replace('%', '')),
                res['Score'],
                res['Signal']
            )
            cur.execute(query, data)
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Successfully archived {len(results)} signals to PostgreSQL.")
    except Exception as e:
        print(f"❌ Database Error: {e}")

def run_screener():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r") as f: prev_ratings = json.load(f)
    else: prev_ratings = {}

    results = []
    new_ratings_store = {}

    print("🚀 Starting Scan...")

    for symbol, sector_etf in TICKERS_MAPPING.items():
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            price = info.get('regularMarketPrice') or info.get('currentPrice') or 0
            
            curr_rating = info.get("recommendationMean", 3.0)
            prev_rating = prev_ratings.get(symbol, 3.0)
            
            sentiment = get_sentiment(stock)
            is_leader = check_rs(symbol, sector_etf)
            
            try:
                insider = stock.insider_transactions
                insider_buy = any("Purchase" in str(x) for x in insider['Transaction'].head(5)) if insider is not None and not insider.empty else False
            except: insider_buy = False
            
            short_pct = (info.get("shortPercentOfFloat", 0) * 100) if info.get("shortPercentOfFloat") else 0
            
            # Scoring
            score = 0
            if (prev_rating >= 2.8 and curr_rating <= 2.4): score += 30 # Upgrade
            if sentiment > 0.15: score += 20
            if is_leader: score += 20
            if insider_buy: score += 20
            if short_pct > 15: score += 10

            signal = "💎 CRYSTAL" if score >= 75 else "✅ CONVICTION" if score >= 50 else "ℹ️ NEUTRAL"

            results.append({
                "Ticker": symbol, "Price": price, "Consensus": f"{prev_rating}→{curr_rating}",
                "News": round(sentiment, 2), "RS": "Leader" if is_leader else "Lag",
                "Insider": "YES" if insider_buy else "No", "Squeeze": f"{short_pct}%",
                "Score": score, "Signal": signal
            })
            new_ratings_store[symbol] = curr_rating
            print(f"Processed {symbol}")
            
        except Exception as e:
            print(f"Error {symbol}: {e}")

    with open(DB_FILE, "w") as f: json.dump(new_ratings_store, f)
    if results: save_to_db(results)

if __name__ == "__main__":
    run_screener()