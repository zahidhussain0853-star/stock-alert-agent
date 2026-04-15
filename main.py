import yfinance as yf
import pandas as pd
import requests
import time
from ta.momentum import RSIIndicator

# ==============================
# 🔐 TELEGRAM CONFIG 
# ==============================
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": message
    }
    response = requests.post(url, data=data)
    print("Telegram:", response.text)


# ==============================
# 📈 STOCK LIST
# ==============================
stocks = [
    "AAPL","MSFT","NVDA","AMZN","META",
    "GOOGL","TSLA","AMD","NFLX","INTC",
    "CRM","ADBE","PYPL","CSCO","ORCL",
    "PEP","KO","MCD","NKE","SBUX",
    "JPM","BAC","WFC","GS","MS",
    "XOM","CVX","COP","SLB","BP",
    "BA","CAT","GE","MMM","HON",
    "UNH","JNJ","PFE","MRK","ABBV",
    "HD","LOW","COST","WMT","TGT"
]


def run_agent():
    print("\n🚀 Running stock scan...")

    # --- Market check ---
    spy = yf.Ticker("SPY")
    spy_data = spy.history(period="2d")

    spy_latest = spy_data["Close"].iloc[-1]
    spy_previous = spy_data["Close"].iloc[-2]

    spy_change = ((spy_latest - spy_previous) / spy_previous) * 100

    print(f"SPY Change: {spy_change:.2f}%")

    if spy_change > -1:
        print("Market not down enough.")
        return

    print("Market DOWN → scanning stocks...\n")

    # --- Download data ---
    data = yf.download(stocks, period="1mo", group_by="ticker", progress=False)

    candidates = []

    for symbol in stocks:
        try:
            df = data[symbol]

            if len(df) < 14:
                continue

            rsi_indicator = RSIIndicator(close=df["Close"])
            df["RSI"] = rsi_indicator.rsi()

            latest = df.iloc[-1]
            previous = df.iloc[-2]

            change = ((latest["Close"] - previous["Close"]) / previous["Close"]) * 100
            rsi = latest["RSI"]

            print(f"{symbol}: {change:.2f}% | RSI {rsi:.2f}")

            if change < -3 and rsi < 30:
                score = abs(change) + (30 - rsi)
                candidates.append((symbol, change, rsi, score))

        except Exception as e:
            print(f"Error with {symbol}: {e}")
            continue

    candidates.sort(key=lambda x: x[3], reverse=True)

    print("\n--- TOP BUY CANDIDATES ---")

    if not candidates:
        print("No strong opportunities found.")
    else:
        for stock in candidates:
            symbol, change, rsi, score = stock

            message = (
                f"🚀 BUY ALERT\n"
                f"{symbol}\n"
                f"Drop: {change:.2f}%\n"
                f"RSI: {rsi:.2f}"
            )

            print(message)
            send_telegram(message)


# ==============================
# 🔁 LOOP
# ==============================
while True:
    run_agent()
    print("\n⏱️ Sleeping for 10 minutes...\n")
    time.sleep(600)