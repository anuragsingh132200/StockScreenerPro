import streamlit as st
import pandas as pd
import time
from datetime import datetime, timedelta
import pytz
from stock_data import get_volume_data, get_market_caps, get_nse_bse_symbols
from utils import is_market_open, get_current_time_ist, format_market_cap

# Set page config
st.set_page_config(
    page_title="Indian Stock Volume Screener",
    page_icon="📈",
    layout="wide"
)

# Title and description
st.title("Indian Stock Volume Screener")
st.write("""
This application screens Indian stocks based on volume spikes and market cap criteria.
It identifies stocks with current 5-minute volumes at least 10 times higher than the previous day's average,
and with market caps greater than ₹1000 crore.
""")

# Initialize session state for storing filtered stocks
if 'filtered_stocks' not in st.session_state:
    st.session_state.filtered_stocks = pd.DataFrame()

if 'last_update_time' not in st.session_state:
    st.session_state.last_update_time = None

if 'symbols' not in st.session_state:
    st.session_state.symbols = None


def load_and_filter_stocks():
    """Load stock data, apply filters, and update the session state"""
    with st.spinner("Fetching stock symbols..."):
        if st.session_state.symbols is None:
            st.session_state.symbols = get_nse_bse_symbols()
    
    if not st.session_state.symbols:
        st.error("Failed to fetch stock symbols. Please try again later.")
        return
    
    symbols = st.session_state.symbols
    current_time = get_current_time_ist()
    
    # Status message
    status_col1, status_col2 = st.columns(2)
    with status_col1:
        st.write(f"Current time (IST): {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    with status_col2:
        market_status = "OPEN" if is_market_open(current_time) else "CLOSED"
        st.write(f"Market Status: {market_status}")
    
    # Progress bar and processing message
    progress_bar = st.progress(0)
    processing_text = st.empty()
    
    # Get volume data for all symbols
    processing_text.text(f"Fetching volume data for {len(symbols)} stocks...")
    volume_data = get_volume_data(symbols, progress_callback=lambda x: progress_bar.progress(x * 0.7))
    
    if volume_data.empty:
        st.error("Failed to fetch volume data. Please try again later.")
        return
    
    # Filter stocks with volume spike ratio >= 10
    processing_text.text("Filtering stocks based on volume spike...")
    volume_filtered = volume_data[volume_data['volume_spike_ratio'] >= 10].copy()
    
    if volume_filtered.empty:
        st.warning("No stocks meet the volume spike criteria at this time.")
        st.session_state.filtered_stocks = pd.DataFrame()
        st.session_state.last_update_time = current_time
        progress_bar.progress(1.0)
        processing_text.empty()
        return
    
    # Get market caps
    processing_text.text("Fetching market cap data...")
    market_caps = get_market_caps(volume_filtered.index.tolist(), 
                                 progress_callback=lambda x: progress_bar.progress(0.7 + x * 0.3))
    
    # Merge volume data with market caps
    volume_filtered = volume_filtered.join(market_caps)
    
    # Filter by market cap > 1000 crore
    filtered_stocks = volume_filtered[volume_filtered['market_cap_cr'] > 1000].copy()
    
    # Sort by current volume (descending) and take top 10
    filtered_stocks = filtered_stocks.sort_values('current_volume', ascending=False).head(10)
    
    # Format market cap
    filtered_stocks['market_cap_cr'] = filtered_stocks['market_cap_cr'].apply(format_market_cap)
    
    # Update session state
    st.session_state.filtered_stocks = filtered_stocks
    st.session_state.last_update_time = current_time
    
    # Clear progress indicators
    progress_bar.progress(1.0)
    processing_text.empty()


# Auto refresh settings
col1, col2 = st.columns([3, 1])
with col1:
    auto_refresh = st.checkbox("Auto-refresh data", value=True)
    
with col2:
    refresh_interval = st.selectbox("Refresh interval (minutes)", 
                                   options=[1, 5, 10, 15, 30], 
                                   index=1)

# Manual refresh button
if st.button("Refresh Now"):
    load_and_filter_stocks()

# Auto-refresh logic
if auto_refresh:
    # Check if it's time to refresh
    current_time = get_current_time_ist()
    if (st.session_state.last_update_time is None or 
        (current_time - st.session_state.last_update_time).total_seconds() >= refresh_interval * 60):
        load_and_filter_stocks()

# Display results
if not st.session_state.filtered_stocks.empty:
    st.subheader("Top 10 Stocks with Volume Spikes")
    
    if st.session_state.last_update_time:
        st.write(f"Last updated: {st.session_state.last_update_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
    
    # Rename columns for display
    display_df = st.session_state.filtered_stocks.reset_index().rename(columns={
        'index': 'Symbol',
        'name': 'Company Name',
        'current_volume': 'Current 5-Minute Volume',
        'avg_volume_prev_day': 'Avg Volume (Prev Day, 10x5min)',
        'volume_spike_ratio': 'Volume Spike Ratio',
        'market_cap_cr': 'Market Cap (₹ Cr)'
    })
    
    # Reorder columns
    display_df = display_df[['Symbol', 'Company Name', 'Current 5-Minute Volume', 
                             'Avg Volume (Prev Day, 10x5min)', 'Volume Spike Ratio', 
                             'Market Cap (₹ Cr)']]
    
    # Format the volume spike ratio to 2 decimal places
    display_df['Volume Spike Ratio'] = display_df['Volume Spike Ratio'].apply(lambda x: f"{x:.2f}x")
    
    # Display the dataframe
    st.dataframe(display_df, use_container_width=True)
else:
    if st.session_state.last_update_time:
        st.info("No stocks currently meet the filtering criteria. Try refreshing later.")
    else:
        # Initial load
        load_and_filter_stocks()

# Add information about the criteria
st.subheader("Screening Criteria")
st.write("""
- **Volume Spike**: Current 5-minute volume is at least 10 times the average of the previous day's first 10 five-minute candles
- **Market Cap**: Greater than ₹1000 crore
- **Sorting**: Top 10 stocks with highest current 5-minute volume
""")

# Footer
st.markdown("---")
st.caption("Data refreshes automatically during market hours. The NSE and BSE market hours are 9:15 AM to 3:30 PM IST, Monday to Friday.")
