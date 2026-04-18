import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def rebuild_table():
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        print("Dropping old table...")
        cur.execute("DROP TABLE IF EXISTS stock_ratings CASCADE;")

        print("Creating table with Hard-Baked Composite Primary Key...")
        # Note: We are making ticker + date the ACTUAL Primary Key here.
        # This is the ultimate level of integrity.
        cur.execute("""
            CREATE TABLE stock_ratings (
                ticker VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                score DECIMAL(4,2),
                sb INTEGER DEFAULT 0,
                b INTEGER DEFAULT 0,
                h INTEGER DEFAULT 0,
                s INTEGER DEFAULT 0,
                ss INTEGER DEFAULT 0,
                total INTEGER DEFAULT 0,
                event TEXT DEFAULT '',
                PRIMARY KEY (ticker, date)
            );
        """)
        
        conn.commit()
        print("✅ SUCCESS: Table rebuilt with Ticker+Date as the Primary Key.")

    except Exception as e:
        print(f"❌ ERROR: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    rebuild_table()