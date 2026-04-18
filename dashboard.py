import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL").replace("postgres://", "postgresql://", 1))

def load_data(symbol):
    return pd.read_sql(f"SELECT * FROM daily_metrics WHERE ticker = '{symbol}' ORDER BY date ASC", engine)

st.set_page_config(page_title="Scout Terminal", layout="wide", initial_sidebar_state="expanded")

# --- CUSTOM CSS FOR TRADING VIBE ---
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    div[data-metric-indicator="positive"] > div { color: #00ffcc !important; }
    </style>
    """, unsafe_allow_html=True)

st.title("🎯 Scout Transition Terminal")

# --- SIDEBAR ---
tickers = pd.read_sql("SELECT DISTINCT ticker FROM daily_metrics", engine)['ticker'].tolist()
selected = st.sidebar.selectbox("Select Asset", sorted(tickers))

if selected:
    df = load_data(selected)
    latest = df.iloc[-1]
    
    # --- ROW 1: THE GAUGES ---
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        # Scout Score Gauge (Mocked logic for visualization)
        score = 65 if selected == "NFLX" else 25 
        fig_gauge = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = score,
            title = {'text': "Scout Confidence"},
            gauge = {'axis': {'range': [0, 100]},
                     'bar': {'color': "#00ffcc"},
                     'steps': [{'range': [0, 50], 'color': "#333"}, {'range': [50, 80], 'color': "#555"}]}
        ))
        fig_gauge.update_layout(height=250, margin=dict(l=20, r=20, t=50, b=20), paper_bgcolor="rgba(0,0,0,0)", font={'color': "white"})
        st.plotly_chart(fig_gauge, use_container_width=True)

    with col2:
        st.metric("RVOL (Volume Surge)", f"{(latest['volume']/latest['average_volume_30d']):.2f}x", delta_color="normal")
        st.metric("Short Interest", f"{latest['short_float_pct']}%")
        st.metric("Analyst Vibe", f"{latest['analyst_rating']}", help="1.0 is Strong Buy, 5.0 is Sell")

    with col3:
        st.subheader("🤖 Active Signals")
        # Logic to display your signal strings from main.py
        signals = ["INSTITUTIONAL_ACCUMULATION", "POSITIVE_TREND_CONFIRMED"] if score > 50 else ["STABLE_TREND"]
        for s in signals:
            st.info(f"🧬 {s.replace('_', ' ')}")

    # --- ROW 2: THE MAIN TRANSITION CHART ---
    st.markdown("### 📊 Multi-Metric Transition Map")
    
    # Create subplots: Top for Rating/Price, Bottom for Volume
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.05, row_heights=[0.7, 0.3])

    # Analyst Rating Trend (The Core Metric)
    fig.add_trace(go.Scatter(x=df['date'], y=df['analyst_rating'], 
                             name="Analyst Rating", line=dict(color='#00ffcc', width=3)), row=1, col=1)

    # Volume (The Fuel)
    fig.add_trace(go.Bar(x=df['date'], y=df['volume'], name="Volume", 
                         marker_color='rgba(100, 100, 100, 0.5)'), row=2, col=1)

    fig.update_layout(
        template="plotly_dark",
        height=600,
        yaxis=dict(title="Rating", autorange="reversed"),
        yaxis2=dict(title="Volume"),
        showlegend=False,
        margin=dict(l=0, r=0, t=0, b=0)
    )
    st.plotly_chart(fig, use_container_width=True)

# --- RANKING TABLE ---
st.markdown("---")
st.subheader("⚡ Top Momentum Captures")
rankings = pd.read_sql("SELECT ticker, analyst_rating, volume, average_volume_30d FROM daily_metrics WHERE date = (SELECT MAX(date) FROM daily_metrics) ORDER BY analyst_rating ASC LIMIT 10", engine)
st.table(rankings)