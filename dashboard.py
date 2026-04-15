import streamlit as st
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_data():
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        # Get the latest unique scan for each ticker
        query = """
            SELECT DISTINCT ON (symbol) * FROM quant_signals 
            ORDER BY symbol, timestamp DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df.sort_values(by="final_score", ascending=False)
    except Exception as e:
        st.error(f"❌ DB Error: {e}")
        return pd.DataFrame()

st.set_page_config(page_title="Quant Pro", layout="wide")

# Title & Refresh
st.title("📡 Alpha Intelligence Command Center")
if st.button('🔄 Force Refresh Data'):
    st.rerun()

df = get_data()

if not df.empty:
    # --- METRICS BAR ---
    m1, m2, m3, m4 = st.columns(4)
    avg_sent = df['news_sentiment'].mean()
    
    m1.metric("Live Coverage", f"{len(df)} Stocks")
    m2.metric("💎 Crystal Signals", len(df[df['final_score'] >= 70]))
    m3.metric("Avg Market Score", f"{df['final_score'].mean():.1f}")
    m4.metric("Market Sentiment", f"{avg_sent:.2f}", delta_color="normal")

    st.divider()

    # --- DATAFRAME WITH HIGHLIGHTS ---
    st.subheader("🎯 High-Conviction Radar")

    # Formatting and Styling
    def style_dataframe(df):
        # We create a copy for display
        styled_df = df[[
            'symbol', 'price', 'final_score', 'signal_label', 
            'analyst_transition', 'news_sentiment', 'volume_delta'
        ]].copy()
        
        styled_df.columns = [
            'Ticker', 'Price', 'Score', 'Signal', 
            'Rating (Prev→Now)', 'News Score', 'Vol vs Avg'
        ]
        return styled_df

    clean_df = style_dataframe(df)

    # Display with conditional formatting
    st.dataframe(
        clean_df.style.background_gradient(cmap='Greens', subset=['Score'])
        .format({
            'Price': '${:.2f}',
            'News Score': '{:.2f}',
            'Vol vs Avg': '{:.2f}x'
        }),
        use_container_width=True,
        hide_index=True
    )

    # --- VOLUME ALERT SECTION ---
    st.divider()
    vol_col, chart_col = st.columns([1, 2])
    
    with vol_col:
        st.subheader("🔥 Volume Breakouts")
        # Show stocks where volume is 50% above average
        breakouts = df[df['volume_delta'] > 1.5][['symbol', 'volume_delta']].sort_values('volume_delta', ascending=False)
        if not breakouts.empty:
            st.table(breakouts)
        else:
            st.write("No unusual volume detected yet.")

    with chart_col:
        st.subheader("📈 Score Distribution")
        st.bar_chart(df.set_index('symbol')['final_score'].head(15))
        st.caption("Top 15 Tickers by Quant Score")

else:
    st.info("Waiting for agent to finish the first clean scan...")