import pandas as pd
import yfinance as yf
import psycopg2
import os
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def get_sp500_tickers():
    print("Fetching full S&P 500 ticker list...")
    try:
        # Using a Header to prevent being blocked
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        header = {
          "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
          "X-Requested-With": "XMLHttpRequest"
        }
        r = requests.get(url, headers=header)
        tables = pd.read_html(r.text)
        df = tables[0]
        tickers = [t.replace('.', '-') for t in df['Symbol'].tolist()]
        print(f"Successfully retrieved {len(tickers)} tickers.")
        return tickers
    except Exception as e:
        print(f"❌ Failed to fetch list: {e}")
        return []

def calculate_score(sb, b, h, s, ss):
    total = sb + b + h + s + ss
    if total == 0: return 0
    return round(((sb*1) + (b*2) + (h*3) + (s*4) + (ss*5)) / total, 2)

def seed_history():
    tickers = get_sp500_tickers()
    
    if not tickers:
        print("Empty ticker list. Stopping.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
    except Exception as e:
        print(f"❌ DB Connection Error: {e}")
        return

    today = datetime.now().date()
    
    for index, symbol in enumerate(tickers):
        # We wrap the whole logic in a try/except so one bad ticker doesn't stop the script
        try:
            print(f"[{index+1}/{len(tickers)}] Processing {symbol}...")
            ticker_obj = yf.Ticker(symbol)
            recs = ticker_obj.recommendations_summary
            
            if recs is None or recs.empty:
                continue

            for i in range(60):
                target_date = today - timedelta(days=i)
                row = recs.iloc[0] if i < 30 else (recs.iloc[1] if len(recs) > 1 else recs.iloc[0])

                sb, b, h, s, ss = int(row['strongBuy']), int(row['buy']), int(row['hold']), int(row['sell']), int(row['strongSell'])
                score = calculate_score(sb, b, h, s, ss)

                cur.execute("""
                    INSERT INTO stock_ratings (ticker, date, score, sb, b, h, s, ss, total, event)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, date) DO NOTHING;
                """, (symbol, target_date, score, sb, b, h, s, ss, (sb+b+h+s+ss), ""))
            
            conn.commit()
            
            if (index + 1) % 10 == 0:
                time.sleep(1)

        except Exception as e:
            print(f"   ⚠️ Skipping {symbol} due to error: {e}")
            conn.rollback()

    cur.close()
    conn.close()
    print("\n✅ Migration Finished.")

if __name__ == "__main__":
    seed_history()