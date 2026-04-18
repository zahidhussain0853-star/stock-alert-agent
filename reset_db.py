# Create a temporary file called reset_db.py and run it
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL").replace("postgres://", "postgresql://", 1))

setup_sql = """
ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS price DECIMAL(10, 2);
ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS short_float_pct DECIMAL(10, 2);
TRUNCATE TABLE daily_metrics RESTART IDENTITY;
"""

with engine.connect() as conn:
    conn.execute(text(setup_sql))
    conn.commit()
    print("✅ Schema updated and table wiped. Ready for backfill!")