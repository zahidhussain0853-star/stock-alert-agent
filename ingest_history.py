import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Setup
load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL").replace("postgres://", "postgresql://", 1))

st.set_page_config(page_title="Scout: Short-Cycle Trader", layout="wide")

# --- DATA LOADERS ---
@st.cache_data(ttl=600)
def get_ticker_list():
    query = "SELECT DISTINCT ticker FROM daily_metrics ORDER BY ticker"
    return pd.read_sql(query, engine)['ticker'].tolist()

def load_ticker_data(symbol):
    query = f"SELECT * FROM daily_metrics WHERE ticker = '{symbol}' ORDER BY date ASC"
    df = pd.read_sql(query, engine)
    # FIX: Explicitly convert to datetime to prevent the "flattened" axis
    df['date'] = pd.to_datetime(df['date'])
    return df

@st.cache_data(ttl=300)
def get_top_signals():
    """Finds stocks where volume is surging and rating is bullish"""
    query = """
    SELECT ticker, price, analyst_rating, 
           ROUND(CAST(volume AS DECIMAL) / NULLIF(average_volume_30d, 0), 2) as velocity
    FROM daily_metrics 
    WHERE date = (SELECT MAX(date) FROM daily_metrics)
    AND analyst_rating < 2.5
    ORDER BY velocity DESC LIMIT 5
    """
    return pd.read_sql(query, engine)

# --- SIDEBAR: SCOUT SCANNER ---
st.sidebar.title("🔭 Scout Scanner")
st.sidebar.subheader("Top Short-Cycle Signals")
signals = get_top_signals()
for _, row in signals.iterrows():
    st.sidebar.metric(row['ticker'], f"${row['price']}", f"Vol: {row['velocity']}x")

tickers = get_ticker_list()
selected_ticker = st.sidebar.selectbox("Select Asset to Inspect", tickers, index=tickers.index("NVDA") if "NVDA" in tickers else 0)

# --- MAIN DASHBOARD ---
st.title(f"Transition Map: {selected_ticker}")

df = load_ticker_data(selected_ticker)

if not df.empty:
    latest = df.iloc[-1]
    
    # 1. TOP ROW: KEY METRICS
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Current Price", f"${latest['price']}")
    col2.metric("Scout Rating", latest['analyst_rating'], help="Lower is better (1.0 = Strong Buy)")
    col3.metric("Volume Velocity", f"{round(latest['volume']/latest['average_volume_30d'], 2)}x")
    col4.metric("Short Float", f"{latest['short_float_pct']}%")

    # 2. THE CHART: PRICE & SENTIMENT
    # We use subplots to keep Price and Volume distinct but synced
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.1, subplot_titles=("Price & Analyst Trend", "Volume Surge"),
                        row_heights=[0.7, 0.3])

    # Price Line
    fig.add_trace(go.Scatter(x=df['date'], y=df['price'], name="Price", 
                             line=dict(color='#00ffcc', width=3)), row=1, col=1)

    # Analyst Rating (On a secondary Y-axis so it doesn't squash the price)
    fig.add_trace(go.Scatter(x=df['date'], y=df['analyst_rating'], name="Rating", 
                             line=dict(color='#ff9900', dash='dot')), row=1, col=1)

    # Volume Bars
    colors = ['#ff4b4b' if v > latest['average_volume_30d'] else '#31333f' for v in df['volume']]
    fig.add_trace(go.Bar(x=df['date'], y=df['volume'], name="Volume", marker_color=colors), row=2, col=1)

    fig.update_layout(height=600, template="plotly_dark", showlegend=False,
                      xaxis_rangeslider_visible=False)
    
    # Reverse the Y-axis for Rating (so 1.0 is at the top)
    fig.update_yaxes(title_text="Price / Rating", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)

    # 3. TRANSITION ALERT
    if latest['volume'] > latest['average_volume_30d'] and latest['analyst_rating'] < 2.2:
        st.success(f"🎯 **Trade Signal**: {selected_ticker} is showing high-conviction inflow. Ideal for 1-4 week hold.")
else:
    st.error("No data found for this ticker. Run ingest_history.py to backfill.")