import streamlit as st
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# ==============================
# 🔐 LOAD ENV VARIABLES
# ==============================
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# ==============================
# 🗄️ CONNECT TO DATABASE
# ==============================
conn = psycopg2.connect(DATABASE_URL)

# ==============================
# 📊 LOAD DATA
# ==============================
df = pd.read_sql("SELECT * FROM trades ORDER BY timestamp DESC", conn)

st.write("Rows in DB:", len(df))
st.write(df.head())

# ==============================
# 🎯 PAGE CONFIG
# ==============================
st.set_page_config(page_title="Stock Agent Dashboard", layout="wide")

st.title("📊 Stock Agent Dashboard")

# ==============================
# 🚫 NO DATA CASE
# ==============================
if df.empty:
    st.warning("No trades yet.")

else:
    # ==============================
    # 🧠 PREP DATA
    # ==============================
    completed = df.dropna(subset=["return_pct"])

    # ==============================
    # 📊 METRICS
    # ==============================
    total_trades = len(df)

    win_rate = (
        (completed["return_pct"] > 0).mean() * 100
        if len(completed) > 0 else 0
    )

    avg_return = (
        completed["return_pct"].mean()
        if len(completed) > 0 else 0
    )

    col1, col2, col3 = st.columns(3)

    col1.metric("Total Trades", total_trades)
    col2.metric("Win Rate (%)", f"{win_rate:.2f}")
    col3.metric("Avg Return (%)", f"{avg_return:.2f}")

    # ==============================
    # 📈 CHARTS
    # ==============================
    if not completed.empty:

        # Sort by time
        completed_sorted = completed.sort_values("timestamp")

        # ---- Equity Curve ----
        st.subheader("📈 Equity Curve")

        completed_sorted["cumulative_return"] = completed_sorted["return_pct"].cumsum()

        st.line_chart(
            completed_sorted.set_index("timestamp")["cumulative_return"]
        )

        # ---- Win vs Loss ----
        st.subheader("📊 Win vs Loss")

        wins = (completed["return_pct"] > 0).sum()
        losses = (completed["return_pct"] <= 0).sum()

        chart_data = pd.DataFrame({
            "Result": ["Wins", "Losses"],
            "Count": [wins, losses]
        })

        st.bar_chart(chart_data.set_index("Result"))

        # ---- Returns Over Time ----
        st.subheader("📅 Returns Over Time")

        st.line_chart(
            completed_sorted.set_index("timestamp")["return_pct"]
        )

    # ==============================
    # 📋 TABLE
    # ==============================
    st.subheader("Recent Trades")
    st.dataframe(df.head(20))