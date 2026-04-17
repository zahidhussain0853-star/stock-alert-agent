import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Alpha Intelligence Command Center", 
    page_icon="📡", 
    layout="wide"
)

load_dotenv()

# --- DATABASE CONNECTION ---
@st.cache_data(ttl=30)
def get_data():
    try:
        db_url = os.getenv("DATABASE_URL")
        # Ensure compatibility with SQLAlchemy
        if db_url and db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        
        engine = create_engine(db_url)
        
        query = """
        SELECT DISTINCT ON (symbol) 
            symbol, price, final_score, signal_label, 
            analyst_transition, news_sentiment, volume_delta,
            insider_buying, short_float_pct, rs_status, 
            num_analysts, raw_rating, prev_raw_rating,
            sector, operating_margin, return_on_equity, free_cash_flow,
            timestamp
        FROM quant_signals
        ORDER BY symbol, timestamp DESC;
        """
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return pd.DataFrame()

# --- HELPER FUNCTIONS ---
def translate_rating(val):
    try:
        if pd.isna(val): return "Neutral"
        num = float(val)
        if num <= 1.5: return "Strong Buy"
        elif num <= 2.5: return "Buy"
        elif num <= 3.5: return "Hold"
        elif num <= 4.5: return "Sell"
        else: return "Strong Sell"
    except: return "Neutral"

def format_detailed_rating(row):
    """
    Format: Prev Label → Now Label (Count, Curr Avg, Prev Avg → Now Avg)
    """
    try:
        now_score = row.get('raw_rating')
        prev_score = row.get('prev_raw_rating')
        count = row.get('num_analysts')
        safe_prev = now_score if pd.isna(prev_score) else prev_score
        safe_count = 0 if pd.isna(count) else int(count)
        prev_label = translate_rating(safe_prev)
        now_label = translate_rating(now_score)
        return f"{prev_label} → {now_label} ({safe_count}, {now_score:.2f}, {safe_prev:.2f} → {now_score:.2f})"
    except:
        return "Pending"

# --- MAIN UI ---
def main():
    st.title("📡 Alpha Intelligence Command Center")
    
    df = get_data()
    if df.empty:
        st.warning("Awaiting scanner data...")
        return

    # 1. Metrics Bar
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Live Coverage", f"{len(df)} Stocks")
    m2.metric("💎 Crystal Signals", len(df[df['final_score'] >= 70]))
    m3.metric("Avg Market Score", f"{df['final_score'].mean():.1f}")
    m4.metric("Market Sentiment", f"{df['news_sentiment'].mean():.2f}")

    st.markdown("---")
    
    # 2. Legend
    st.info("""
    **Alpha Scoring Logic:**
    * **Momentum:** RS Leader (+20) | Sentiment > 0.1 (+20) | Vol Shift > 1.5x (+10)
    * **Analyst:** Base Score ((5-Rating)*10) | Upgrade (+20)
    * **Fundamentals:** Tech/Retail: Op. Margin > 25% (+10) & Debt/Equity < 100 (+5) | Finance: ROE > 15% (+15)
    * **Speculative:** Short Float > 10% (+10) | High Insider Holding > 5% (+10)
    """)

    # 3. Data Processing
    display_df = df.copy()
    
    # Fix: Multiply by 100 for proper percentage display
    display_df['operating_margin'] = display_df['operating_margin'] * 100
    display_df['return_on_equity'] = display_df['return_on_equity'] * 100
    
    display_df['Rating Details'] = display_df.apply(format_detailed_rating, axis=1)
    display_df['Insider'] = display_df['insider_buying'].apply(lambda x: '🟢 High' if x else '⚪ Standard')
    display_df['Trend'] = display_df['rs_status'].apply(lambda x: '🚀 Leader' if x == 'Leader' else '📉 Lag')
    display_df['Signal'] = display_df['final_score'].apply(
        lambda x: '💎 Crystal' if x >= 70 else '✅ Conviction' if x >= 45 else 'ℹ️ Neutral'
    )

    # 4. Styling Logic
    def style_rows(row):
        styles = [''] * len(row)
        if row['Score'] >= 70:
            styles[row.index.get_loc('Score')] = 'background-color: #004d1a; color: white; font-weight: bold'
        elif row['Score'] >= 45:
            styles[row.index.get_loc('Score')] = 'background-color: #28a745; color: white'
        
        if row['Short %'] > 10:
            styles[row.index.get_loc('Short %')] = 'color: #ff4b4b; font-weight: bold'
            
        return styles

    final_df = display_df.rename(columns={
        'symbol': 'Ticker', 'price': 'Price', 'final_score': 'Score', 
        'news_sentiment': 'News', 'volume_delta': 'Vol Shift', 'short_float_pct': 'Short %',
        'operating_margin': 'Op Margin', 'return_on_equity': 'ROE', 'sector': 'Sector'
    })

    # Ensure all required columns are in the list
    cols = [
        'Ticker', 'Sector', 'Price', 'Score', 'Signal', 'Trend', 
        'Short %', 'Insider', 'News', 'Vol Shift', 'Op Margin', 'ROE', 'Rating Details'
    ]

    # 5. Final Render
    st.dataframe(
        final_df[cols].style.apply(style_rows, axis=1),
        width="stretch", 
        hide_index=True,
        height=600,
        column_config={
            "Price": st.column_config.NumberColumn(format="$%.2f"),
            "Score": st.column_config.NumberColumn(format="%d"),
            "News": st.column_config.NumberColumn(format="%.2f"),
            "Vol Shift": st.column_config.NumberColumn(format="%.2fx"),
            "Short %": st.column_config.NumberColumn(format="%.1f%%"),
            "Op Margin": st.column_config.NumberColumn(format="%.1f%%"),
            "ROE": st.column_config.NumberColumn(format="%.1f%%"),
            "Rating Details": st.column_config.TextColumn(width="large"),
            "Sector": st.column_config.TextColumn(width="medium"),
            "Insider": st.column_config.TextColumn(width="small")
        }
    )

if __name__ == "__main__":
    main()