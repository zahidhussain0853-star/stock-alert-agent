import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def update_ticker_length():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        print("Expanding ticker column to 16 characters...")
        cur.execute("ALTER TABLE stock_ratings ALTER COLUMN ticker TYPE VARCHAR(16);")
        
        conn.commit()
        print("✅ SUCCESS: Ticker capacity expanded.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    update_ticker_length()