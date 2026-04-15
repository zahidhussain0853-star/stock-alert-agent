import yfinance as yf
import pandas as pd
import requests
import time
import os
import feedparser
import psycopg2
from dotenv import load_dotenv
from ta.momentum import RSIIndicator

#print("🚀 NEW DEPLOY TEST")

# ==============================
# 🔐 LOAD ENV VARIABLES
# ==============================
load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

#print("DB URL:", DATABASE_URL)

# ==============================
# 🗄️ DATABASE CONNECTION
# ==============================
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    symbol TEXT,
    change FLOAT,
    rsi FLOAT,
    entry_price FLOAT,
    future_price FLOAT,
    return_pct FLOAT
)
""")
conn.commit()


# ==============================
# 📩 TELEGRAM FUNCTION
# ==============================
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": message
    }
    response = requests.post(url, data=data)
    print("Telegram:", response.text)


# ==============================
# 🧠 NEWS FILTER
# ==============================
def has_bad_news(symbol):
    url = f"https://news.google.com/rss/search?q={symbol}+stock"
    feed = feedparser.parse(url)

    bad_keywords = [
        "earnings miss", "downgrade", "lawsuit", "fraud",
        "investigation", "guidance cut", "bankruptcy",
        "layoffs", "missed expectations", "revenue miss"
    ]

    for entry in feed.entries[:5]:
        title = entry.title.lower()

        for word in bad_keywords:
            if word in title:
                print(f"⚠️ Bad news detected for {symbol}: {title}")
                return True

    return False


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


# ==============================
# 📝 LOG TRADE TO DATABASE
# ==============================
def log_trade(symbol, change, rsi, price):
    cursor.execute("""
        INSERT INTO trades (timestamp, symbol, change, rsi, entry_price)
        VALUES (NOW(), %s, %s, %s, %s)
    """, (symbol, change, rsi, price))
    conn.commit()


# ==============================
# 📊 UPDATE PERFORMANCE
# ==============================
def update_performance():
    cursor.execute("""
        SELECT id, symbol, entry_price
        FROM trades
        WHERE future_price IS NULL
    """)

    rows = cursor.fetchall()

    for row in rows:
        trade_id, symbol, entry_price = row

        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d")

            if len(data) == 0:
                continue

            current_price = data["Close"].iloc[-1]
            return_pct = ((current_price - entry_price) / entry_price) * 100

            cursor.execute("""
                UPDATE trades
                SET future_price = %s,
                    return_pct = %s
                WHERE id = %s
            """, (current_price, return_pct, trade_id))

            conn.commit()

        except Exception as e:
            print(f"Error updating {symbol}:", e)


# ==============================
# 🚀 MAIN AGENT
# ==============================
def run_agent():
    print("\n🚀 Running stock scan...")

    # Market check
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

                if has_bad_news(symbol):
                    print(f"Skipping {symbol} due to bad news")
                    continue

                score = abs(change) + (30 - rsi)
                candidates.append((symbol, change, rsi, score, latest["Close"]))

        except Exception as e:
            print(f"Error with {symbol}: {e}")
            continue

    candidates.sort(key=lambda x: x[3], reverse=True)

    print("\n--- TOP BUY CANDIDATES ---")

    if not candidates:
        print("No strong opportunities found.")
    else:
        for stock in candidates:
            symbol, change, rsi, score, price = stock

            message = (
                f"🚀 BUY ALERT\n"
                f"{symbol}\n"
                f"Drop: {change:.2f}%\n"
                f"RSI: {rsi:.2f}"
            )

            print(message)
            send_telegram(message)
            log_trade(symbol, change, rsi, price)


# ==============================
# 🔁 LOOP
# ==============================
while True:
    run_agent()
    update_performance()
    print("\n⏱️ Sleeping for 10 minutes...\n")
    time.sleep(600)