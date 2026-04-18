import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import os
from dotenv import load_dotenv

# 1. Setup
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

st.set_page_config(page_title="Ratings Dashboard", layout="wide")

@st.cache_data(ttl=60) 
def fetch_ratings():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        query = "SELECT * FROM stock_ratings"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return pd.DataFrame()

# 2. Load and Prepare Data
df = fetch_ratings()

if not df.empty:
    df['date'] = pd.to_datetime(df['date'])
    
    # --- SIDEBAR ---
    st.sidebar.header("Filters")
    ticker_list = sorted(df['ticker'].unique())
    selected_ticker = st.sidebar.selectbox("Select Ticker", ticker_list)

    # Filter for selected ticker
    ticker_df = df[df['ticker'] == selected_ticker].sort_values(by='date')

    # --- HEADER ---
    st.title(f"📈 {selected_ticker} Sentiment Analysis")

    # --- CHART SECTION ---
    # Smaller height as requested
    fig = px.line(
        ticker_df, 
        x='date', 
        y='score',
        markers=True,
        labels={'score': 'Score', 'date': 'Date'}
    )

    fig.update_layout(
        height=380,  # Compact height
        margin=dict(l=10, r=10, t=10, b=10),
        hovermode="x unified"
    )

    # Always show full 1.0 to 5.0 scale in 0.1 increments
    fig.update_yaxes(
        range=[5.0, 1.0],  # 1.0 at top (Strong Buy), 5.0 at bottom
        dtick=0.1,         # 0.1 increments
        tickformat=".1f",  # Force 1 decimal place
        showgrid=True,
        gridcolor='rgba(200, 200, 200, 0.2)'
    )

    fig.update_xaxes(
        dtick="D5",        # Label every 5 days to avoid clutter
        tickformat="%b %d",
        showgrid=True
    )

    st.plotly_chart(fig, use_container_width=True)

    # --- TABLE SECTION ---
    st.subheader("Historical Ratings")
    table_cols = ['ticker', 'date', 'score', 'sb', 'b', 'h', 's', 'ss', 'total', 'event']
    
    st.dataframe(
        ticker_df[table_cols].sort_values(by='date', ascending=False),
        use_container_width=True,
        hide_index=True
    )
else:
    st.warning("Waiting for data... Ensure the seeder script is running.")