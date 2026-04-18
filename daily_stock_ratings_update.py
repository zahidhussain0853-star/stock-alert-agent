import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
from datetime import datetime
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
    """Fetches S&P 500 tickers from verified financial data repository."""
    try:
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        df = pd.read_csv(url)
        return df['Symbol'].tolist()
    except Exception as e:
        print(f"Ticker fetch failed: {e}")
        return []

def calc_score(sb, b, h, s, ss):
    sum_vals = sb + b + h + s + ss
    if sum_vals == 0: return 0
    # 1=Strong Buy, 5=Strong Sell
    return round(((sb*1) + (b*2) + (h*3) + (s*4) + (ss*5)) / sum_vals, 2)

def connect_db():
    conn = psycopg2.connect(DB_URL, sslmode='require')
    return conn

def run_daily_sync():
    tickers = get_sp500_tickers()
    today = datetime.now().date()
    
    print(f"\n{'='*100}")
    print(f" DAILY REFRESH | {today} | TARGET: stock_ratings")
    print(f"{'='*100}")

    conn = connect_db()
    cursor = conn.cursor()
    
    batch_data = []
    success_count = 0

    for symbol in tickers:
        clean_symbol = symbol.replace('.', '-')
        try:
            ticker_obj = yf.Ticker(clean_symbol)
            summ = ticker_obj.recommendations_summary
            
            # Get latest upgrade/downgrade for the 'event' column
            try:
                events = ticker_obj.get_upgrades_downgrades()
                if events is not None and not events.empty:
                    latest = events.iloc[-1]
                    event_str = f"★ {latest.get('Firm', 'Analyst')}: {latest.get('ToGrade', 'Update')}"
                else:
                    event_str = "-"
            except:
                event_str = "-"

            if summ is not None and not summ.empty:
                # Use the '0' index which is the most current month
                row = summ.iloc[0]
                sb, b, h = int(row['strongBuy']), int(row['buy']), int(row['hold'])
                s, ss = int(row['sell']), int(row['strongSell'])
                
                score = calc_score(sb, b, h, s, ss)
                total = sb + b + h + s + ss

                batch_data.append((
                    symbol, today, float(score), sb, b, h, s, ss, total, event_str
                ))
                success_count += 1
                
                # Commit in chunks of 50 to manage memory/connection
                if len(batch_data) >= 50:
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
                    batch_data = []
                    print(f"Progress: {success_count} tickers synced...")

            time.sleep(0.2) # Throttling

        except Exception as e:
            print(f"Skipping {symbol}: {e}")

    # Final batch commit
    if batch_data:
        execute_values(cursor, upsert_query, batch_data)
        conn.commit()

    # Final Verification
    cursor.execute("SELECT COUNT(*) FROM stock_ratings")
    total_db_rows = cursor.fetchone()[0]
    
    print(f"\n{'='*100}")
    print(f" DAILY SYNC COMPLETE")
    print(f" New/Updated Rows: {success_count}")
    print(f" Total Rows in DB: {total_db_rows}")
    print(f"{'='*100}\n")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_daily_sync()