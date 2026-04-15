import streamlit as st
import pandas as pd
import psycopg2
import os
import plotly.express as px
from dotenv import load_dotenv

# Add this to auto-refresh every 300 seconds
from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=300 * 1000, key="datarefresh")

# Load your .env file
load_dotenv()

# --- DB CONNECTION ---
def get_data():
    try:
        # Pulls DATABASE_URL from your .env file
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        query = "SELECT * FROM quant_signals ORDER BY timestamp DESC"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"❌ Connection Error: {e}")
        return pd.DataFrame()

# --- DASHBOARD UI CONFIG ---
st.set_page_config(
    page_title="Quant Signal Command Center",
    page_icon="📡",
    layout="wide"
)

# Custom CSS for a "Trading Terminal" feel
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    div[data-testid="stMetricValue"] { color: #00ffc8; }
    </style>
    """, unsafe_allow_html=True)

st.title("📡 Alpha Intelligence Command Center")
st.caption("Live Quantitative Analysis of 50 High-Volume Equities")

# --- DATA FETCHING ---
df = get_data()

if not df.empty:
    # --- TOP LEVEL METRICS ---
    m1, m2, m3, m4 = st.columns(4)
    
    with m1:
        st.metric("Total Scans", len(df))
    with m2:
        crystal_count = len(df[df['signal_label'] == '💎 CRYSTAL'])
        st.metric("💎 Crystal Buys", crystal_count)
    with m3:
        avg_score = round(df['final_score'].mean(), 1)
        st.metric("Avg Market Score", avg_score)
    with m4:
        # Calculate most recent sentiment
        recent_sent = df['news_sentiment'].iloc[0] if not df.empty else 0
        st.metric("Latest News Bias", f"{recent_sent:.2f}")

    st.divider()

    # --- MAIN INTERFACE ---
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("🎯 Active Trading Signals")
        
        # Filters
        st.sidebar.header("Filters")
        search = st.sidebar.text_input("Search Ticker (e.g. NVDA)")
        min_score = st.sidebar.slider("Min Score", 0, 100, 40)
        
        # Filtering Logic
        display_df = df[df['final_score'] >= min_score]
        if search:
            display_df = display_df[display_df['symbol'].str.contains(search.upper())]

        # Display Dataframe
        st.dataframe(
            display_df[['timestamp', 'symbol', 'price', 'final_score', 'signal_label', 'rs_status', 'news_sentiment']],
            use_container_width=True,
            hide_index=True
        )

    with col_right:
        st.subheader("📈 Score Distribution")
        fig = px.pie(df, names='signal_label', hole=0.4, 
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(showlegend=False, height=300, margin=dict(t=0, b=0, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("🔥 Top 5 Heat")
        top_5 = df.nlargest(5, 'final_score')[['symbol', 'final_score', 'price']]
        st.table(top_5)

    # --- DETAILED ANALYSIS SECTION ---
    st.divider()
    st.header("🔍 Signal Deep Dive")
    
    selected_ticker = st.selectbox("Select a ticker for historical context:", df['symbol'].unique())
    ticker_history = df[df['symbol'] == selected_ticker]
    
    hist_col1, hist_col2 = st.columns(2)
    with hist_col1:
        st.line_chart(ticker_history.set_index('timestamp')['final_score'])
        st.caption(f"Score History for {selected_ticker}")
    with hist_col2:
        st.bar_chart(ticker_history.set_index('timestamp')['news_sentiment'])
        st.caption(f"Sentiment History for {selected_ticker}")

else:
    st.warning("The database appears to be empty. Run your `main.py` first to populate signals.")

# Footer
st.markdown("---")
st.markdown("🔒 *AI Quant Agent v1.0 - Private Deployment*")