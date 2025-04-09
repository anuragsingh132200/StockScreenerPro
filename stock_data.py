import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import pytz
import time
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from utils import get_current_time_ist

# Cache for stock symbols and market cap data
SYMBOL_CACHE = {
    'timestamp': None,
    'symbols': None
}

MARKET_CAP_CACHE = {
    'timestamp': None,
    'market_caps': {}
}

def get_nse_bse_symbols():
    """
    Get a list of stock symbols from NSE and BSE
    Returns a dictionary mapping symbols to company names
    """
    # Check if we have a recent cache (less than 1 day old)
    if (SYMBOL_CACHE['timestamp'] is not None and 
        (datetime.now() - SYMBOL_CACHE['timestamp']).total_seconds() < 86400 and
        SYMBOL_CACHE['symbols'] is not None):
        return SYMBOL_CACHE['symbols']
    
    try:
        # Get NSE symbols
        nse_symbols = {}
        
        # For faster performance and reliable testing, use top 50 Indian stocks
        # This significantly improves the data fetching speed
        default_symbols = [
            # NIFTY 50 Components
            'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
            'HINDUNILVR.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'ITC.NS', 'KOTAKBANK.NS',
            'LT.NS', 'BAJFINANCE.NS', 'AXISBANK.NS', 'ASIANPAINT.NS', 'MARUTI.NS',
            'TITAN.NS', 'SUNPHARMA.NS', 'ULTRACEMCO.NS', 'TATASTEEL.NS', 'NTPC.NS',
            'ADANIENT.NS', 'ADANIPORTS.NS', 'BAJAJFINSV.NS', 'BAJAJ-AUTO.NS', 'BPCL.NS',
            'BRITANNIA.NS', 'CIPLA.NS', 'COALINDIA.NS', 'DIVISLAB.NS', 'DRREDDY.NS',
            'EICHERMOT.NS', 'GRASIM.NS', 'HEROMOTOCO.NS', 'HINDALCO.NS', 'INDUSINDBK.NS',
            'JSWSTEEL.NS', 'M&M.NS', 'NESTLEIND.NS', 'ONGC.NS', 'POWERGRID.NS',
            'SBILIFE.NS', 'TATACONSUM.NS', 'TATAMOTORS.NS', 'TECHM.NS', 'UPL.NS',
            'WIPRO.NS', 'HCLTECH.NS', 'APOLLOHOSP.NS', 'HDFCLIFE.NS', 'SHREECEM.NS'
        ]
        
        # Get company names with multi-threading for speed
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            def get_company_name(symbol):
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.info
                    name = info.get('longName', symbol.replace('.NS', ''))
                    return symbol, name
                except Exception:
                    return symbol, symbol.replace('.NS', '')
            
            for symbol, name in executor.map(get_company_name, default_symbols):
                nse_symbols[symbol] = name
        
        # Cache the results
        SYMBOL_CACHE['timestamp'] = datetime.now()
        SYMBOL_CACHE['symbols'] = nse_symbols
        
        return nse_symbols
        
    except Exception as e:
        print(f"Error getting stock symbols: {e}")
        # If all else fails, return a minimal set to ensure app functionality
        return {
            'RELIANCE.NS': 'Reliance Industries', 
            'TCS.NS': 'Tata Consultancy Services',
            'HDFCBANK.NS': 'HDFC Bank', 
            'INFY.NS': 'Infosys'
        }

def fetch_volume_data_for_symbol(symbol_info):
    """Fetch volume data for a single symbol"""
    symbol, name = symbol_info
    
    try:
        # Current time in IST
        current_time_ist = get_current_time_ist()
        
        # Previous trading day (accounting for weekends)
        days_to_subtract = 1
        prev_day = current_time_ist - timedelta(days=days_to_subtract)
        while prev_day.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
            days_to_subtract += 1
            prev_day = current_time_ist - timedelta(days=days_to_subtract)
        
        # Start and end times for previous day data - convert to UTC for Yahoo Finance
        # We need to strip timezone info to avoid the tz parameter error
        prev_day_naive = prev_day.replace(tzinfo=None)
        prev_day_start_naive = prev_day_naive.replace(hour=9, minute=15, second=0, microsecond=0)
        prev_day_end_naive = prev_day_naive.replace(hour=11, minute=0, second=0, microsecond=0)
        
        # Fetch previous day's 5-minute candles
        ticker = yf.Ticker(symbol)
        prev_day_data = ticker.history(
            start=prev_day_start_naive.strftime('%Y-%m-%d'),
            end=(prev_day_naive + timedelta(days=1)).strftime('%Y-%m-%d'),
            interval="5m"
        )
        
        if prev_day_data.empty:
            return None
        
        # Handle timezone for filtering - convert dataframe index timezone to match
        if prev_day_data.index.tzinfo is None:
            prev_day_data.index = prev_day_data.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
        
        # Create time objects for filtering (without dates)
        market_open_time = datetime.strptime('09:15:00', '%H:%M:%S').time()
        market_end_time = datetime.strptime('11:00:00', '%H:%M:%S').time()
        
        # Filter to trading hours and get first 10 candles
        mask = [(t.time() >= market_open_time and t.time() <= market_end_time) for t in prev_day_data.index]
        filtered_data = prev_day_data.iloc[mask].head(10)
        
        if len(filtered_data) < 5:  # Need at least 5 candles for reasonable average
            return None
        
        # Calculate average volume
        avg_volume = filtered_data['Volume'].mean()
        
        # Get current 5-minute candle - convert to naive datetime for yfinance
        current_day_start_naive = current_time_ist.replace(hour=9, minute=15, second=0, microsecond=0).replace(tzinfo=None)
        current_time_naive = current_time_ist.replace(tzinfo=None)
        
        current_day_data = ticker.history(
            start=current_day_start_naive.strftime('%Y-%m-%d'),
            end=current_time_naive.strftime('%Y-%m-%d %H:%M:%S'),
            interval="5m"
        )
        
        if current_day_data.empty:
            return None
        
        # Get the latest 5-minute candle
        current_candle = current_day_data.iloc[-1]
        current_volume = current_candle['Volume']
        
        # Calculate volume spike ratio
        volume_spike_ratio = current_volume / avg_volume if avg_volume > 0 else 0
        
        return {
            'symbol': symbol,
            'name': name,
            'current_volume': current_volume,
            'avg_volume_prev_day': avg_volume,
            'volume_spike_ratio': volume_spike_ratio
        }
    
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None

def get_volume_data(symbols_dict, progress_callback=None):
    """
    Get volume data for all symbols and calculate volume spike ratios
    
    Args:
        symbols_dict: Dictionary mapping symbols to company names
        progress_callback: Function to call with progress (0.0 to 1.0)
        
    Returns:
        DataFrame with volume data and spike ratios
    """
    results = []
    symbols_list = list(symbols_dict.items())
    total_symbols = len(symbols_list)
    
    # Process in larger batches with more workers for better speed
    batch_size = 20
    for i in range(0, total_symbols, batch_size):
        batch = symbols_list[i:i+batch_size]
        
        # Increase max_workers for faster parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            batch_results = list(executor.map(fetch_volume_data_for_symbol, batch))
            
            for result in batch_results:
                if result is not None:
                    results.append(result)
        
        # Update progress
        if progress_callback:
            progress_callback(min(1.0, (i + batch_size) / total_symbols))
        
        # Minimal sleep to prevent rate limiting but be faster
        time.sleep(0.5)
    
    # Create DataFrame from results
    if not results:
        return pd.DataFrame()
    
    df = pd.DataFrame(results)
    df = df.set_index('symbol')
    
    return df

def get_market_cap(symbol):
    """Get market cap for a single symbol"""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        # Market cap in USD
        market_cap_usd = info.get('marketCap', 0)
        
        # Convert to INR (rough conversion)
        usd_to_inr = 83  # Approximate exchange rate, adjust as needed
        market_cap_inr = market_cap_usd * usd_to_inr
        
        # Convert to crores (1 crore = 10 million)
        market_cap_cr = market_cap_inr / 10000000
        
        return market_cap_cr
    
    except Exception as e:
        print(f"Error getting market cap for {symbol}: {e}")
        return 0

def get_market_caps(symbols, progress_callback=None):
    """
    Get market caps for all symbols
    
    Args:
        symbols: List of symbols
        progress_callback: Function to call with progress (0.0 to 1.0)
        
    Returns:
        DataFrame with market caps
    """
    # Check if we have a recent cache (less than 1 hour old)
    cache_valid = (
        MARKET_CAP_CACHE['timestamp'] is not None and 
        (datetime.now() - MARKET_CAP_CACHE['timestamp']).total_seconds() < 3600
    )
    
    # Initialize with cached data if available
    if cache_valid:
        market_caps = MARKET_CAP_CACHE['market_caps'].copy()
    else:
        market_caps = {}
    
    # Only fetch data for symbols not in cache
    symbols_to_fetch = [s for s in symbols if s not in market_caps]
    
    if not symbols_to_fetch:
        # If all symbols are already in cache, return immediately
        # This makes subsequent calls very fast
        if progress_callback:
            progress_callback(1.0)  # Indicate complete progress
        return pd.DataFrame({'market_cap_cr': {s: market_caps[s] for s in symbols}})
    
    total_symbols = len(symbols_to_fetch)
    
    # Process in larger batches with more workers for better speed
    batch_size = 20
    for i in range(0, total_symbols, batch_size):
        batch = symbols_to_fetch[i:i+batch_size]
        
        # Increase max_workers for faster parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            batch_results = list(executor.map(get_market_cap, batch))
            
            for symbol, market_cap in zip(batch, batch_results):
                market_caps[symbol] = market_cap
                # Update the cache
                MARKET_CAP_CACHE['market_caps'][symbol] = market_cap
        
        # Update progress
        if progress_callback:
            progress_callback(min(1.0, (i + batch_size) / total_symbols))
        
        # Minimal sleep to prevent rate limiting but be faster
        time.sleep(0.5)
    
    # Update cache timestamp
    if not cache_valid:
        MARKET_CAP_CACHE['timestamp'] = datetime.now()
    
    # Return only the requested symbols
    result_market_caps = {s: market_caps.get(s, 0) for s in symbols}
    
    # Create DataFrame from results
    return pd.DataFrame({'market_cap_cr': result_market_caps})
