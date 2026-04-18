import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def test_db_connection():
    conn = None
    try:
        print("--- Connecting to Railway PostgreSQL ---")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # 1. Clean up
        cur.execute("DELETE FROM stock_ratings WHERE ticker = 'TEST';")
        
        # 2. Test Valid Insert
        print("Testing Valid Insert...")
        # (ticker, date, score, sb, b, h, s, ss, total, event) - 10 columns
        cur.execute("""
            INSERT INTO stock_ratings (ticker, date, score, sb, b, h, s, ss, total, event)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, ('TEST', '2026-04-18', 1.25, 10, 5, 2, 0, 0, 17, 'Integrity Test'))
        
        # 3. Test Unique Constraint (The Duplicate Test)
        print("Testing Duplicate Prevention (Ticker + Date)...")
        try:
            cur.execute("""
                INSERT INTO stock_ratings (ticker, date, score)
                VALUES ('TEST', '2026-04-18', 5.00);
            """)
            print("❌ FAIL: Database allowed a duplicate!")
        except psycopg2.errors.UniqueViolation:
            print("✅ PASS: Database BLOCKED the duplicate. Integrity confirmed.")
            conn.rollback() # Required after an error to continue
            cur = conn.cursor()
        
        # 4. Final Verify
        cur.execute("SELECT ticker, score FROM stock_ratings WHERE ticker = 'TEST';")
        row = cur.fetchone()
        if row:
            print(f"✅ VERIFIED: Data persistent in DB.")

        conn.commit()
        cur.close()

    except Exception as e:
        print(f"❌ ERROR: {e}")
    finally:
        if conn:
            conn.close()
            print("--- Connection Closed ---")

if __name__ == "__main__":
    test_db_connection()