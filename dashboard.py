import streamlit as st
import pandas as pd
import psycopg2
import os
import warnings
from dotenv import load_dotenv

# --- PAGE CONFIG ---
st.set_page_config(page_title="Alpha Intelligence Command Center", layout="wide")
load_dotenv()

# Suppress the SQLAlchemy warning from Pandas (Corrected function name)
warnings.filterwarnings("ignore", category=UserWarning)

# --- DATABASE CONNECTION ---
def get_data():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    # Ensure num_analysts and raw_rating are included in the SELECT
    query = """
    SELECT DISTINCT ON (symbol) 
        symbol, price, final_score, signal_label, 
        analyst_transition, news_sentiment, volume_delta,
        insider_buying, short_float_pct, rs_status, 
        num_analysts, raw_rating, timestamp
    FROM quant_signals
    ORDER BY symbol, timestamp DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# --- RATING TRANSLATOR ---
def translate_rating(val):
    try:
        f_val = float(val)
        if f_val <= 1.5: return "Strong Buy"
        if f_val <= 2.5: return "Buy"
        if f_val <= 3.5: return "Hold"
        if f_val <= 4.5: return "Sell"
        return "Strong Sell"
    except:
        return "N/A"

# --- DETAILED RATING FORMATTER ---
def format_detailed_rating(row):
    """Handles arrow character mismatch and adds (Count, Avg)"""
    try:
        raw_text = str(row['analyst_transition'])
        
        # Split by EITHER the special arrow or the hyphen arrow
        if " → " in raw_text:
            parts = raw_text.split(" → ")
        elif " -> " in raw_text:
            parts = raw_text.split(" -> ")
        else:
            return raw_text # Fallback if no arrow found

        # Translate the numbers into text labels
        prev = translate_rating(parts[0].strip())
        now = translate_rating(parts[1].strip())
        
        # Get the extra numeric data
        count = row.get('num_analysts')
        avg = row.get('raw_rating')
        
        # If we have valid numeric data, append the brackets
        if count is not None and not pd.isna(count) and avg is not None:
            return f"{prev} → {now} ({int(count)}, {float(avg):.1f})"
        
        # If no numeric data, just return labels
        return f"{prev} → {now}"
    except Exception:
        return str(row['analyst_transition'])

# --- STYLING FUNCTION ---
def style_dataframe(df):
    # Select columns for display
    styled_df = df[[
        'symbol', 'price', 'final_score', 'signal_label', 
        'analyst_transition', 'news_sentiment', 'volume_delta',
        'insider_buying', 'short_float_pct', 'rs_status'
    ]].copy()
    
    # Apply the detailed formatting logic (uses data from the original df)
    styled_df['analyst_transition'] = df.apply(format_detailed_rating, axis=1)
    
    # Rename columns for the UI
    styled_df.columns = [
        'Ticker', 'Price', 'Score', 'Signal', 
        'Rating (Prev→Now)', 'News', 'Vol Shift',
        'Insider', 'Short %', 'Trend'
    ]
    
    # Prettify the status columns
    styled_df['Insider'] = styled_df['Insider'].apply(lambda x: "🟢 Buy" if x else "⚪ None")
    styled_df['Trend'] = styled_df['Trend'].apply(lambda x: "🚀 Leader" if x == "Leader" else "📉 Lag")
    
    # Sort by Score
    styled_df = styled_df.sort_values(by='Score', ascending=False)
    
    return styled_df

# --- MAIN DASHBOARD UI ---
def main():
    st.title("📡 Alpha Intelligence Command Center")
    
    if st.button("🔄 Force Refresh Data"):
        st.cache_data.clear()

    try:
        df = get_data()
        
        # --- METRICS BAR ---
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Live Coverage", f"{len(df)} Stocks")
        with col2:
            crystals = len(df[df['final_score'] >= 70])
            st.metric("💎 Crystal Signals", crystals)
        with col3:
            avg_score = df['final_score'].mean()
            st.metric("Avg Market Score", f"{avg_score:.1f}")
        with col4:
            mkt_sentiment = df['news_sentiment'].mean()
            st.metric("Market Sentiment", f"{mkt_sentiment:.2f}")

        st.markdown("---")

        # --- DATA TABLE ---
        st.subheader("🎯 High-Conviction Radar")
        
        st.info("""
        **Legend:** **Score:** 70+ (💎 Crystal), 45-65 (✅ Conviction) | 
        **Rating:** Transition (Analyst Count, Raw Avg) | 
        **News:** Sentiment (>0.10 = +20) | 
        **Vol Shift:** vs 10-day Avg (>1.5x = +10) | 
        **Trend:** RS vs Sector (🚀 Leader = +20) | 
        **Insider:** Purchase (🟢 Buy = +10) | 
        **Short %:** Squeeze Potential (>10% = +10)
        """)
        
        clean_df = style_dataframe(df)

        # Render the styled dataframe
        st.dataframe(
            clean_df.style.background_gradient(cmap='Greens', subset=['Score'])
            .format({
                'Price': '${:.2f}',
                'News': '{:.2f}',
                'Vol Shift': '{:.2f}x',
                'Short %': '{:.1f}%'
            }),
            use_container_width=True,
            hide_index=True,
            height=800
        )

        st.caption("Data sources: Yahoo Finance, Google News RSS. Scores updated per scan.")

    except Exception as e:
        st.error(f"Error loading dashboard: {e}")

if __name__ == "__main__":
    main()