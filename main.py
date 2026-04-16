import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# --- PAGE CONFIG ---
st.set_page_config(page_title="Alpha Intelligence Command Center", layout="wide")
load_dotenv()

# --- DATABASE CONNECTION ---
def get_data():
    # Convert 'postgres://' to 'postgresql://' if necessary for SQLAlchemy
    db_url = os.getenv("DATABASE_URL")
    if db_url and db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    
    # Create SQLAlchemy engine
    engine = create_engine(db_url)
    
    query = """
    SELECT DISTINCT ON (symbol) 
        symbol, price, final_score, signal_label, 
        analyst_transition, news_sentiment, volume_delta,
        insider_buying, short_float_pct, rs_status, 
        num_analysts, raw_rating, timestamp
    FROM quant_signals
    ORDER BY symbol, timestamp DESC;
    """
    # Using the engine instead of a raw connection object
    df = pd.read_sql(query, engine)
    return df

# --- RATING TRANSLATOR ---
def translate_rating(val):
    try:
        f_val = float(val)
        if f_val <= 1.5: return "Strong Buy"
        if f_val <= 2.5: return "Buy"
        if f_val <= 3.5: return "Hold"
        if f_val <= 4.5: return "Sell"
        return "Strong Sell"
    except:
        return "N/A"

# --- FORMATTER ---
def format_detailed_transition(row):
    try:
        parts = row['analyst_transition'].split(" → ")
        prev = translate_rating(parts[0])
        now = translate_rating(parts[1])
        count = int(row['num_analysts'])
        avg = float(row['raw_rating'])
        return f"{prev} → {now} ({count}, {avg:.1f})"
    except:
        return row['analyst_transition']

# --- STYLING FUNCTION ---
def style_dataframe(df):
    df['Detailed Rating'] = df.apply(format_detailed_transition, axis=1)
    
    styled_df = df[[
        'symbol', 'price', 'final_score', 'signal_label', 
        'Detailed Rating', 'news_sentiment', 'volume_delta',
        'insider_buying', 'short_float_pct', 'rs_status'
    ]].copy()
    
    styled_df.columns = [
        'Ticker', 'Price', 'Score', 'Signal', 
        'Rating (Prev→Now)', 'News', 'Vol Shift',
        'Insider', 'Short %', 'Trend'
    ]
    
    styled_df['Insider'] = styled_df['Insider'].apply(lambda x: "🟢 Buy" if x else "⚪ None")
    styled_df['Trend'] = styled_df['Trend'].apply(lambda x: "🚀 Leader" if x == "Leader" else "📉 Lag")
    styled_df = styled_df.sort_values(by='Score', ascending=False)
    
    return styled_df

# --- MAIN DASHBOARD UI ---
def main():
    st.title("📡 Alpha Intelligence Command Center")
    
    if st.button("🔄 Force Refresh Data"):
        st.cache_data.clear()

    try:
        df = get_data()
        
        # --- METRICS BAR ---
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Live Coverage", f"{len(df)} Stocks")
        with col2:
            crystals = len(df[df['final_score'] >= 70])
            st.metric("💎 Crystal Signals", crystals)
        with col3:
            avg_score = df['final_score'].mean()
            st.metric("Avg Market Score", f"{avg_score:.1f}")
        with col4:
            mkt_sentiment = df['news_sentiment'].mean()
            st.metric("Market Sentiment", f"{mkt_sentiment:.2f}")

        st.markdown("---")

        # --- DATA TABLE ---
        st.subheader("🎯 High-Conviction Radar")
        
        st.info("""
        **Legend:** **Score:** 70+ (💎 Crystal), 45-65 (✅ Conviction) | 
        **Rating:** Transition (Analyst Count, Raw Avg) | 
        **News:** Last 5 Sentiment (>0.10 = +20) | 
        **Vol Shift:** vs 10d Avg (>1.5x = +10) | 
        **Trend:** RS vs Sector (30d Leader = +20) | 
        **Insider:** Purchase (🟢 Buy = +10) | 
        **Short %:** Squeeze Potential (>10% = +10)
        """)
        
        clean_df = style_dataframe(df)

        st.dataframe(
            clean_df.style.background_gradient(cmap='Greens', subset=['Score'])
            .format({
                'Price': '${:.2f}',
                'News': '{:.2f}',
                'Vol Shift': '{:.2f}x',
                'Short %': '{:.1f}%'
            }),
            use_container_width=True,
            hide_index=True,
            height=800
        )

    except Exception as e:
        st.error(f"Error loading dashboard: {e}")

if __name__ == "__main__":
    main()