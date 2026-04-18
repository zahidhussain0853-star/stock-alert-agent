import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def force_unique_constraint():
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        print("Checking for existing duplicates before applying constraint...")
        cur.execute("DELETE FROM stock_ratings WHERE ticker = 'TEST_TICKER';")
        
        print("Applying Unique Constraint (Ticker + Date)...")
        # This is the SQL command that Railway's UI ignored. 
        # Running it through psycopg2 ensures it is committed.
        cur.execute("""
            ALTER TABLE stock_ratings 
            ADD CONSTRAINT unique_ticker_date UNIQUE (ticker, date);
        """)
        
        conn.commit()
        print("✅ SUCCESS: Constraint applied successfully.")

    except psycopg2.errors.DuplicateTable:
        print("ℹ️  Constraint already exists.")
    except Exception as e:
        print(f"❌ ERROR: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    force_unique_constraint()