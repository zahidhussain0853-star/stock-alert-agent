import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

# Load Railway Environment Variables
load_dotenv()
raw_url = os.getenv("DATABASE_URL")

# Driver normalization
if raw_url and raw_url.startswith("postgres://"):
    DB_URL = raw_url.replace("postgres://", "postgresql://", 1)
else:
    DB_URL = raw_url

def get_sp500_tickers():
    """Fetches S&P 500 tickers without using Wikipedia."""
    print("Fetching S&P 500 Ticker List (Financial Data Source)...")
    try:
        # Fetching from a verified daily updated financial data repository
        # This is a common industry alternative to Wikipedia scraping
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        df = pd.read_csv(url)
        return df['Symbol'].tolist()
    except Exception as e:
        print(f"Primary fetch failed: {e}")
        # Secondary source: SlickCharts or similar financial aggregator
        try:
            df = pd.read_html("https://www.slickcharts.com/sp500", attrs={'class': 'table'})[0]
            return df['Symbol'].tolist()
        except:
            print("All dynamic fetches failed. Using core S&P 100 fallback.")
            return ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "JNJ", "V"]

def calc_score(sb, b, h, s, ss):
    sum_vals = sb + b + h + s + ss
    if sum_vals == 0: return 0
    return round(((sb*1) + (b*2) + (h*3) + (s*4) + (ss*5)) / sum_vals, 2)

def setup_railway_db():
    conn = psycopg2.connect(DB_URL, sslmode='require')
    cursor = conn.cursor()
    # Schema Locked per image: ticker, date, score, sb, b, h, s, ss, total, event
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_ratings (
            ticker VARCHAR(10),
            date DATE,
            score DECIMAL(4,2),
            sb INTEGER DEFAULT 0,
            b INTEGER DEFAULT 0,
            h INTEGER DEFAULT 0,
            s INTEGER DEFAULT 0,
            ss INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0,
            event TEXT,
            PRIMARY KEY (ticker, date)
        )
    ''')
    conn.commit()
    return conn, cursor

def run_production_sync():
    # PULL ENTIRE S&P 500 (WIKI-FREE)
    sp500_tickers = get_sp500_tickers()
    today = datetime.now().date()
    
    print(f"\n{'='*135}")
    print(f" PRODUCTION S&P 500 SYNC | DATABASE: railway | TABLE: stock_ratings")
    print(f" TOTAL TICKERS: {len(sp500_tickers)}")
    print(f"{'='*135}")
    
    for symbol in sp500_tickers:
        # Standardize for yfinance
        symbol = symbol.replace('.', '-')
        
        try:
            conn, cursor = setup_railway_db()
            ticker_obj = yf.Ticker(symbol)
            summ = ticker_obj.recommendations_summary
            
            try:
                events = ticker_obj.get_upgrades_downgrades()
            except:
                events = None

            if summ is None or summ.empty:
                cursor.close()
                conn.close()
                continue

            # Initialize Ledger
            row = summ.iloc[0]
            curr = {
                'sb': int(row['strongBuy']), 'b': int(row['buy']), 
                'h': int(row['hold']), 's': int(row['sell']), 'ss': int(row['strongSell'])
            }

            # Map Event Feed
            event_map = {}
            if events is not None and not events.empty:
                if not isinstance(events.index, pd.RangeIndex): events = events.reset_index()
                date_col = next((c for c in events.columns if any(x in c.lower() for x in ['date', 'period', 'grade'])), events.columns[0])
                events[date_col] = pd.to_datetime(events[date_col], format='mixed', errors='coerce', utc=True).dt.date
                for _, e_row in events.iterrows():
                    d = e_row[date_col]
                    if pd.notnull(d):
                        if d not in event_map: event_map[d] = []
                        event_map[d].append(e_row)

            # 60-Day Walkback
            batch_data = []
            for i in range(60):
                target_date = today - timedelta(days=i)
                day_event_str = "-"
                
                if target_date in event_map:
                    actions = event_map[target_date]
                    day_event_str = f"★ {actions[0].get('Firm', 'Analyst')}: {actions[0].get('ToGrade', 'Update')}"

                if i == 30 and len(summ) > 1:
                    row = summ.iloc[1]
                    curr = {
                        'sb': int(row['strongBuy']), 'b': int(row['buy']), 
                        'h': int(row['hold']), 's': int(row['sell']), 'ss': int(row['strongSell'])
                    }

                score = calc_score(curr['sb'], curr['b'], curr['h'], curr['s'], curr['ss'])
                total_sum = sum(curr.values())
                
                batch_data.append((
                    symbol, target_date, float(score), 
                    curr['sb'], curr['b'], curr['h'], curr['s'], curr['ss'], 
                    total_sum, day_event_str
                ))

                # Reverse Ledger Logic
                if target_date in event_map:
                    for action in event_map[target_date]:
                        tg = str(action.get('ToGrade', '')).lower()
                        if 'buy' in tg and 'strong' not in tg: curr['b'] = max(0, curr['b'] - 1); curr['h'] += 1
                        elif 'strong buy' in tg: curr['sb'] = max(0, curr['sb'] - 1); curr['b'] += 1
                        elif any(x in tg for x in ['hold', 'neutral']): curr['h'] = max(0, curr['h'] - 1); curr['b'] += 1

            # Railway UPSERT
            upsert_query = """
                INSERT INTO stock_ratings (ticker, date, score, sb, b, h, s, ss, total, event)
                VALUES %s
                ON CONFLICT (ticker, date) DO UPDATE SET
                    score = EXCLUDED.score, sb = EXCLUDED.sb, b = EXCLUDED.b,
                    h = EXCLUDED.h, s = EXCLUDED.s, ss = EXCLUDED.ss,
                    total = EXCLUDED.total, event = EXCLUDED.event;
            """
            execute_values(cursor, upsert_query, batch_data)
            conn.commit()
            
            print(f"Synced {symbol:<6} | Data Points: {len(batch_data)}")
            
            cursor.close()
            conn.close()
            time.sleep(0.4) # Safety pause for S&P 500 volume

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    print(f"\nLocked Production Sync Complete.")
    print(f"{'='*135}\n")

if __name__ == "__main__":
    run_production_sync()