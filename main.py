import yfinance as yf
import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv
from ta.momentum import RSIIndicator
import psycopg2

# ==============================
# 🔐 LOAD ENV
# ==============================
load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

# ==============================
# 📲 TELEGRAM
# ==============================
def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        data = {"chat_id": CHAT_ID, "text": message}
        requests.post(url, data=data)
    except Exception as e:
        print("Telegram error:", e)

# ==============================
# 🗄️ DATABASE
# ==============================
def get_connection():
    return psycopg2.connect(DATABASE_URL)

def log_trade(symbol, change, rsi, price):
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO trades (symbol, change_pct, rsi, price)
            VALUES (%s, %s, %s, %s)
        """, (symbol, change, rsi, price))

        conn.commit()
        cur.close()
        conn.close()

        print(f"Logged trade: {symbol}")

    except Exception as e:
        print("DB error:", e)

# ==============================
# 📊 STOCK LIST
# ==============================
stocks = [
    "AAPL","MSFT","NVDA","AMZN","META",
    "GOOGL","TSLA","AMD","NFLX","INTC"
]

# ==============================
# 🔁 RETRY LOGIC
# ==============================
def get_spy_data(retries=3):
    for i in range(retries):
        try:
            spy = yf.Ticker("SPY")
            data = spy.history(period="2d")

            if len(data) >= 2:
                return data

        except Exception as e:
            print(f"SPY fetch failed (attempt {i+1}):", e)

        time.sleep(2)

    return None

# ==============================
# 🤖 AGENT
# ==============================
def run_agent():
    print("\n🚀 Running stock scan...")

    # --- Market check ---
    spy_data = get_spy_data()

    if spy_data is None:
        print("Skipping run due to SPY failure")
        return

    spy_latest = spy_data["Close"].iloc[-1]
    spy_previous = spy_data["Close"].iloc[-2]
    spy_change = ((spy_latest - spy_previous) / spy_previous) * 100

    print(f"SPY Change: {spy_change:.2f}%")

    if spy_change > -1:
        print("Market not down enough.")

        # 🔥 TEST TRADE (so dashboard fills)
        log_trade("TEST", -1.5, 25, 100)
        return

    print("Market DOWN → scanning stocks...\n")

    # --- Download data ---
    try:
        data = yf.download(stocks, period="1mo", group_by="ticker", progress=False)
    except Exception as e:
        print("Stock download error:", e)
        return

    candidates = []

    for stock in stocks:
        try:
            df = data[stock].dropna()

            if len(df) < 15:
                continue

            change = ((df["Close"].iloc[-1] - df["Close"].iloc[-2]) / df["Close"].iloc[-2]) * 100

            rsi = RSIIndicator(df["Close"]).rsi().iloc[-1]
            price = df["Close"].iloc[-1]

            print(f"{stock}: Change={change:.2f}% | RSI={rsi:.2f}")

            # 🔥 RELAXED CONDITION (for testing)
            if change < -1:
                candidates.append((stock, change, rsi, price))

        except Exception as e:
            print(f"{stock} error:", e)

    # --- Results ---
    print("\n--- TOP BUY CANDIDATES ---")

    if not candidates:
        print("No strong opportunities found.")

        # 🔥 TEST TRADE fallback
        log_trade("TEST", -1.2, 30, 100)
        return

    for stock, change, rsi, price in candidates:
        message = f"📉 BUY SIGNAL\n{stock}\nDrop: {change:.2f}%\nRSI: {rsi:.2f}\nPrice: {price:.2f}"

        send_telegram(message)
        log_trade(stock, change, rsi, price)

# ==============================
# 🔄 LOOP
# ==============================
while True:
    try:
        run_agent()
    except Exception as e:
        print("Agent error:", e)

    print("\n⏱️ Sleeping for 10 minutes...\n")
    time.sleep(600)