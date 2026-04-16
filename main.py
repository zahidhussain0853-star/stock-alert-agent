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

def get_sentiment(ticker_symbol):
    """Enhanced scraper using Sessions and Browser Headers"""
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        })

        rss_url = f"https://news.google.com/rss/search?q={ticker_symbol}+stock+news&hl=en-US&gl=US&ceid=US:en"
        response = session.get(rss_url, timeout=10)
        feed = feedparser.parse(response.content)
        
        titles = [entry.title for entry in feed.entries[:5]]
            
        if not titles:
            return 0.0
            
        scores = [analyzer.polarity_scores(t)['compound'] for t in titles]
        avg_score = float(np.mean(scores))
        return avg_score
    except Exception as e:
        print(f"⚠️ Sentiment Error for {ticker_symbol}: {e}")
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
            float(res['News']), str(res['RS']), bool(res['Insider']),
            float(res['Squeeze']), int(res['Score']), str(res['Signal']), 
            float(res['VolDelta'])
        ))

def run_screener():
    conn = get_db_connection()
    cur = conn.cursor()
    results = []

    print("🚀 Starting Intelligence Scan...")

    for symbol, sector_etf in TICKERS_MAPPING.items():
        # 1. Initialize variables with defaults to prevent Scoping/NoneType errors
        price, curr_rating, vol_delta, sentiment = 0.0, 3.0, 1.0, 0.0
        is_leader, insider_buy, short_pct = False, False, 0.0
        
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # --- MARKET DATA & VOLUME ---
            price = info.get('regularMarketPrice') or info.get('currentPrice') or 0.0
            curr_rating = info.get("recommendationMean", 3.0)
            
            avg_vol = info.get("averageVolume10days") or info.get("averageVolume") or 1.0
            curr_vol = info.get("regularMarketVolume") or 0.0
            vol_delta = float(curr_vol / avg_vol) if avg_vol > 0 else 1.0

            # --- SHORT INTEREST (SQUEEZE DATA) ---
            # yfinance returns this as a decimal (e.g., 0.15 for 15%)
            raw_short = info.get("shortPercentOfFloat") or 0.0
            short_pct = float(raw_short * 100) if raw_short < 1.0 else float(raw_short)

            # --- NEWS SENTIMENT ---
            sentiment = get_sentiment(symbol)
            
            # --- RELATIVE STRENGTH ---
            is_leader = check_rs(symbol, sector_etf)
            
            # --- INSIDER TRANSACTIONS ---
            try:
                insider_data = stock.insider_transactions
                if insider_data is not None and not insider_data.empty:
                    # Look for "Purchase" in the most recent 5 transactions
                    insider_buy = any("Purchase" in str(x) for x in insider_data['Transaction'].head(5))
            except:
                insider_buy = False

            # --- SCORING LOGIC (The Algorithm) ---
            prev_rating = get_last_rating(cur, symbol)
            score = 0
            
            # A) Analyst Upgrade Memory (+30)
            if curr_rating < prev_rating: 
                score += 30
            
            # B) Positive News Sentiment (+20)
            if sentiment > 0.10: 
                score += 20
                
            # C) RS Leadership (+20)
            if is_leader: 
                score += 20
                
            # D) Institutional Volume Spike (+10)
            if vol_delta > 1.5: 
                score += 10
                
            # E) Insider Buying Activity (+10)
            if insider_buy: 
                score += 10
                
            # F) Short Squeeze Potential (+10)
            if short_pct > 10.0: 
                score += 10

            # --- SIGNAL ATTRIBUTION ---
            if score >= 70:
                signal = "💎 CRYSTAL"
            elif score >= 45:
                signal = "✅ CONVICTION"
            else:
                signal = "ℹ️ NEUTRAL"

            # --- PACKAGE RESULTS ---
            results.append({
                "Ticker": symbol, 
                "Price": price, 
                "Consensus": f"{prev_rating:.1f} → {curr_rating:.1f}",
                "News": sentiment, 
                "RS": "Leader" if is_leader else "Lag",
                "Insider": insider_buy, 
                "Squeeze": short_pct,
                "Score": score, 
                "Signal": signal, 
                "VolDelta": vol_delta
            })

            # --- UPDATE DATABASE MEMORY ---
            cur.execute("""
                INSERT INTO analyst_ratings (symbol, last_rating) 
                VALUES (%s, %s) 
                ON CONFLICT (symbol) DO UPDATE SET last_rating = EXCLUDED.last_rating
            """, (symbol, curr_rating))
            
            print(f"✅ {symbol} processed | Score: {score} | Squeeze: {short_pct:.1f}%")

        except Exception as e:
            print(f"❌ Error skipping {symbol}: {e}")

    # --- FINAL ARCHIVE TO DB ---
    if results:
        save_signals_to_db(cur, results)
        conn.commit()
        print(f"📊 Intelligence archived. Total: {len(results)} Tickers.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_screener()