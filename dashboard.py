import streamlit as st
import pandas as pd
import psycopg2
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
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        query = """
        SELECT DISTINCT ON (symbol) 
            symbol, price, final_score, signal_label, 
            analyst_transition, news_sentiment, volume_delta,
            insider_buying, short_float_pct, rs_status, 
            num_analysts, raw_rating, prev_raw_rating, timestamp
        FROM quant_signals
        ORDER BY symbol, timestamp DESC;
        """
        df = pd.read_sql(query, conn)
        conn.close()
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
    Detailed transition: 
    Prev Label -> Curr Label (Count, Curr Avg, Prev Avg -> Curr Avg)
    """
    try:
        now_score = row.get('raw_rating')
        prev_score = row.get('prev_raw_rating')
        count = row.get('num_analysts')
        
        # Handle cases where there is no previous score yet
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
    st.subheader("🎯 High-Conviction Radar")
    
    # 2. Extended Legend (Locked as Baseline)
    st.info("""
    **Legend:** Score: 70+ (💎 Crystal), 45-65 (✅ Conviction), <45 (ℹ️ Neutral) | 
    **Rating:** Transition (Analyst Count, Raw Avg, Prev → Now) | 
    **News:** Sentiment (>0.10 = +20) | 
    **Vol Shift:** vs 10-day Avg (>1.5x = +10) | 
    **Trend:** RS vs Sector (🚀 Leader = +20) | 
    **Insider:** Purchase (🟢 Buy = +10) | 
    **Short %:** Squeeze Potential (>10% = +10)
    """)

    # 3. Data Processing
    display_df = df.copy()
    display_df['Rating Details'] = display_df.apply(format_detailed_rating, axis=1)
    display_df['Insider'] = display_df['insider_buying'].apply(lambda x: '🟢 Buy' if x else '⚪ None')
    display_df['Trend'] = display_df['rs_status'].apply(lambda x: '🚀 Leader' if x == 'Leader' else '📉 Lag')
    display_df['Signal'] = display_df['final_score'].apply(
        lambda x: '💎 Crystal' if x >= 70 else '✅ Conviction' if x >= 45 else 'ℹ️ Neutral'
    )

    # 4. Styling Logic
    def style_score(v):
        if v >= 70: return 'background-color: #004d1a; color: white; font-weight: bold' 
        if v >= 45: return 'background-color: #28a745; color: white' 
        if v >= 20: return 'background-color: #85e085; color: #00260a'
        return ''

    final_df = display_df.rename(columns={
        'symbol': 'Ticker', 'price': 'Price', 'final_score': 'Score', 
        'news_sentiment': 'News', 'volume_delta': 'Vol Shift', 'short_float_pct': 'Short %'
    })

    cols = ['Ticker', 'Price', 'Score', 'Signal', 'Rating Details', 'News', 'Vol Shift', 'Insider', 'Short %', 'Trend']

    # 5. Final Render
    st.dataframe(
        final_df[cols].style.map(style_score, subset=['Score']),
        use_container_width=True,
        hide_index=True,
        height=600,
        column_config={
            "Price": st.column_config.NumberColumn(format="$%.2f"),
            "Score": st.column_config.NumberColumn(format="%d"),
            "News": st.column_config.NumberColumn(format="%.2f"),
            "Vol Shift": st.column_config.NumberColumn(format="%.2fx"),
            "Short %": st.column_config.NumberColumn(format="%.1f%%"),
            "Rating Details": st.column_config.TextColumn(width="large")
        }
    )

if __name__ == "__main__":
    main()