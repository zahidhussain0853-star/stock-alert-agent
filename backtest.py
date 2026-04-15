import yfinance as yf
import pandas as pd
from ta.momentum import RSIIndicator

# ==============================
# ⚙️ CONFIG
# ==============================
stocks = ["AAPL", "MSFT", "NVDA", "AMZN", "META"]
holding_days = 5

results = []

print("🚀 Running backtest...\n")

# ==============================
# 📉 LOAD SPY DATA
# ==============================
spy_df = yf.download("SPY", period="1y", progress=False)

if spy_df.empty:
    print("❌ Failed to load SPY data")
    exit()

# ✅ FIX: flatten columns if needed
if isinstance(spy_df.columns, pd.MultiIndex):
    spy_df.columns = spy_df.columns.get_level_values(0)

spy_df = spy_df.dropna()
spy_close = spy_df["Close"]
spy_change = spy_close.pct_change() * 100

# ==============================
# 🔁 LOOP STOCKS
# ==============================
for stock in stocks:
    print(f"Processing {stock}...")

    try:
        df = yf.download(stock, period="1y", progress=False)

        if df.empty:
            print(f"No data for {stock}")
            continue

        # ✅ FIX: flatten columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.dropna()

        close = df["Close"]

        # Indicators
        df["RSI"] = RSIIndicator(close).rsi()
        df["Change"] = close.pct_change() * 100

        # ==============================
        # 🔗 ALIGN WITH SPY (safe)
        # ==============================
        df["SPY_Change"] = spy_change

        df = df.dropna()

        # ==============================
        # 🔍 STRATEGY LOOP
        # ==============================
        df["SMA50"] = close.rolling(50).mean()
        for i in range(len(df) - holding_days - 1):

            change = df["Change"].iloc[i]
            rsi = df["RSI"].iloc[i]

            if change < -2 and rsi < 30 and close.iloc[i] > df["SMA50"].iloc[i]:
                
                entry_price = close.iloc[i+1]
                exit_price = close.iloc[i + 1 + holding_days]

                # 🛑 stop loss
                stop_price = entry_price * 0.95

                for j in range(i+1, i + 1 + holding_days):
                    if close.iloc[j] <= stop_price:
                        exit_price = stop_price
                        break

                # ✅ CALCULATE FIRST
                return_pct = ((exit_price - entry_price) / entry_price) * 100

                # ✅ THEN STORE
                results.append({
                    "stock": stock,
                    "date": df.index[i+1],
                    "change": change,
                    "rsi": rsi,
                    "return_pct": return_pct
                })

    except Exception as e:
        print(f"{stock} error:", e)

# ==============================
# 📊 RESULTS
# ==============================
results_df = pd.DataFrame(results)

if results_df.empty:
    print("\n❌ No trades found.")
else:
    print("\n📊 RESULTS\n")

    total_trades = len(results_df)
    win_rate = (results_df["return_pct"] > 0).mean() * 100
    avg_return = results_df["return_pct"].mean()

    print(f"Total Trades: {total_trades}")
    print(f"Win Rate: {win_rate:.2f}%")
    print(f"Avg Return: {avg_return:.2f}%")

    print("\n🔝 Top 10 Trades:")
    print(results_df.sort_values("return_pct", ascending=False).head(10))

    print("\n🔻 Worst 10 Trades:")
    print(results_df.sort_values("return_pct").head(10))