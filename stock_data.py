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
import sample_data
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cache for stock symbols and market cap data
SYMBOL_CACHE = {
    'timestamp': None,
    'symbols': None
}

MARKET_CAP_CACHE = {
    'timestamp': None,
    'market_caps': {}
}

# Updated symbol mappings with correct Yahoo Finance tickers
SYMBOL_MAPPING = {
    # Map common incorrect symbols to correct ones
    'ULTRACEM.NS': 'ULTRACEMCO.NS',
    'NTPC.NS': 'NTPC.NS',  # Keep this as is, but we'll handle it better
    'TATASTEEL.NS': 'TATASTEEL.NS',  # Keep this as is, but we'll handle it better
}

# Define a list of reliable Indian stocks less likely to have API issues
RELIABLE_SYMBOLS = [
    # Key NIFTY stocks that are most reliable for API calls - updated with correct symbols
    'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
    'HINDUNILVR.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'ITC.NS', 'KOTAKBANK.NS',
    'LT.NS', 'BAJFINANCE.NS', 'AXISBANK.NS', 'ASIANPAINT.NS', 'MARUTI.NS',
    'TITAN.NS', 'SUNPHARMA.NS', 'ULTRACEMCO.NS', 'TATASTEEL.NS', 'NTPC.NS',
    # Additional reliable symbols
    'WIPRO.NS', 'ONGC.NS', 'POWERGRID.NS', 'M&M.NS', 'ADANIENT.NS',
    'HCLTECH.NS', 'JSWSTEEL.NS', 'TECHM.NS', 'BAJAJFINSV.NS', 'APOLLOHOSP.NS'
]

# Map of pre-defined company names to minimize API calls
PREDEFINED_NAMES = {
    'RELIANCE.NS': 'Reliance Industries',
    'TCS.NS': 'Tata Consultancy Services',
    'HDFCBANK.NS': 'HDFC Bank',
    'INFY.NS': 'Infosys',
    'ICICIBANK.NS': 'ICICI Bank',
    'HINDUNILVR.NS': 'Hindustan Unilever',
    'SBIN.NS': 'State Bank of India',
    'BHARTIARTL.NS': 'Bharti Airtel',
    'ITC.NS': 'ITC Limited',
    'KOTAKBANK.NS': 'Kotak Mahindra Bank',
    'LT.NS': 'Larsen & Toubro',
    'BAJFINANCE.NS': 'Bajaj Finance',
    'AXISBANK.NS': 'Axis Bank',
    'ASIANPAINT.NS': 'Asian Paints',
    'MARUTI.NS': 'Maruti Suzuki',
    'TITAN.NS': 'Titan Company',
    'SUNPHARMA.NS': 'Sun Pharmaceutical',
    'ULTRACEMCO.NS': 'UltraTech Cement',
    'TATASTEEL.NS': 'Tata Steel',
    'NTPC.NS': 'NTPC Limited',
    'WIPRO.NS': 'Wipro Limited',
    'ONGC.NS': 'Oil and Natural Gas Corporation',
    'POWERGRID.NS': 'Power Grid Corporation',
    'M&M.NS': 'Mahindra & Mahindra',
    'ADANIENT.NS': 'Adani Enterprises',
    'HCLTECH.NS': 'HCL Technologies',
    'JSWSTEEL.NS': 'JSW Steel',
    'TECHM.NS': 'Tech Mahindra',
    'BAJAJFINSV.NS': 'Bajaj Finserv',
    'APOLLOHOSP.NS': 'Apollo Hospitals'
}

# Pre-defined market caps for fallback (in crores)
FALLBACK_MARKET_CAPS = {
    'RELIANCE.NS': 18000,   # ~₹18,00,000 crore
    'TCS.NS': 14000,        # ~₹14,00,000 crore
    'HDFCBANK.NS': 12000,   # ~₹12,00,000 crore
    'INFY.NS': 7000,        # ~₹7,00,000 crore
    'ICICIBANK.NS': 7500,   # ~₹7,50,000 crore
    'HINDUNILVR.NS': 6000,  # ~₹6,00,000 crore
    'SBIN.NS': 6500,        # ~₹6,50,000 crore
    'BHARTIARTL.NS': 6200,  # ~₹6,20,000 crore
    'ITC.NS': 5500,         # ~₹5,50,000 crore
    'KOTAKBANK.NS': 4200,   # ~₹4,20,000 crore
    'LT.NS': 4000,          # ~₹4,00,000 crore
    'BAJFINANCE.NS': 4500,  # ~₹4,50,000 crore
    'AXISBANK.NS': 3200,    # ~₹3,20,000 crore
    'ASIANPAINT.NS': 3000,  # ~₹3,00,000 crore
    'MARUTI.NS': 3300,      # ~₹3,30,000 crore
    'TITAN.NS': 2800,       # ~₹2,80,000 crore
    'SUNPHARMA.NS': 2600,   # ~₹2,60,000 crore
    'ULTRACEMCO.NS': 2500,  # ~₹2,50,000 crore (corrected from ULTRACEM.NS)
    'TATASTEEL.NS': 2200,   # ~₹2,20,000 crore
    'NTPC.NS': 2400,        # ~₹2,40,000 crore
    'WIPRO.NS': 2100,       # ~₹2,10,000 crore
    'ONGC.NS': 2300,        # ~₹2,30,000 crore
    'POWERGRID.NS': 1800,   # ~₹1,80,000 crore
    'M&M.NS': 1900,         # ~₹1,90,000 crore
    'ADANIENT.NS': 4800,    # ~₹4,80,000 crore
    'HCLTECH.NS': 1700,     # ~₹1,70,000 crore
    'JSWSTEEL.NS': 1600,    # ~₹1,60,000 crore
    'TECHM.NS': 1200,       # ~₹1,20,000 crore
    'BAJAJFINSV.NS': 2700,  # ~₹2,70,000 crore
    'APOLLOHOSP.NS': 1100   # ~₹1,10,000 crore
}

def normalize_symbol(symbol):
    """Normalize Yahoo Finance symbols to handle common issues"""
    if symbol in SYMBOL_MAPPING:
        return SYMBOL_MAPPING[symbol]
    return symbol

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
        # Start with reliable symbols list
        nse_symbols = {}
        
        # Use the predefined list and names
        for symbol in RELIABLE_SYMBOLS:
            normalized_symbol = normalize_symbol(symbol)
            if normalized_symbol in PREDEFINED_NAMES:
                nse_symbols[normalized_symbol] = PREDEFINED_NAMES[normalized_symbol]
            else:
                nse_symbols[normalized_symbol] = normalized_symbol.replace('.NS', '')
        
        # Cache the results immediately - we'll use what we have even if partial
        SYMBOL_CACHE['timestamp'] = datetime.now()
        SYMBOL_CACHE['symbols'] = nse_symbols
        
        logger.info(f"Loaded {len(nse_symbols)} stock symbols")
        return nse_symbols
        
    except Exception as e:
        logger.error(f"Error getting stock symbols: {e}")
        # Fall back to a minimal set to ensure app functionality
        minimal_symbols = {
            'RELIANCE.NS': 'Reliance Industries', 
            'TCS.NS': 'Tata Consultancy Services',
            'HDFCBANK.NS': 'HDFC Bank', 
            'INFY.NS': 'Infosys'
        }
        SYMBOL_CACHE['timestamp'] = datetime.now()
        SYMBOL_CACHE['symbols'] = minimal_symbols
        return minimal_symbols

def get_history_with_fallback(ticker, start, end=None, interval="5m", max_retries=3):
    """Get historical data with fallback and retries"""
    retry_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            params = {
                'start': start,
                'interval': interval
            }
            if end:
                params['end'] = end
                
            data = ticker.history(**params)
            
            if not data.empty:
                return data
            
            # Empty data, try again after delay
            logger.warning(f"Empty data for {ticker.ticker} on attempt {attempt+1}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed for {ticker.ticker}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    
    # All attempts failed
    return pd.DataFrame()

def is_valid_volume_data(data):
    """Check if volume data is valid"""
    if data is None or data.empty:
        return False
        
    # Check if Volume column exists and has non-zero values
    if 'Volume' not in data.columns:
        return False
        
    # Check if there are any significant volumes
    if data['Volume'].sum() < 100:  # Arbitrary low threshold
        return False
        
    return True

def fetch_volume_data_for_symbol(symbol_info):
    """Fetch volume data for a single symbol with improved error handling"""
    symbol, name = symbol_info
    normalized_symbol = normalize_symbol(symbol)
    
    try:
        # Current time in IST
        current_time_ist = get_current_time_ist()
        
        # Previous trading day (accounting for weekends)
        days_to_subtract = 1
        prev_day = current_time_ist - timedelta(days=days_to_subtract)
        while prev_day.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
            days_to_subtract += 1
            prev_day = current_time_ist - timedelta(days=days_to_subtract)
        
        # Prepare date strings
        prev_day_str = prev_day.strftime('%Y-%m-%d')
        next_day_str = (prev_day + timedelta(days=1)).strftime('%Y-%m-%d')
        current_day_str = current_time_ist.strftime('%Y-%m-%d')
        
        # Create ticker object
        ticker = yf.Ticker(normalized_symbol)
        
        # Get previous day's data with improved fallback
        prev_day_data = get_history_with_fallback(
            ticker,
            start=prev_day_str,
            end=next_day_str,
            interval="5m"
        )
        
        if not is_valid_volume_data(prev_day_data):
            logger.warning(f"Invalid prev day data for {normalized_symbol}")
            return None
            
        # Market hours in IST for filtering
        market_open_hour, market_open_minute = 9, 15
        market_end_hour, market_end_minute = 11, 0
            
        # Try different methods to filter data between market hours
        filtered_data = pd.DataFrame()
        
        # Method 1: Try hour/minute filtering
        try:
            filtered_data = prev_day_data[
                ((prev_day_data.index.hour > market_open_hour) | 
                 ((prev_day_data.index.hour == market_open_hour) & (prev_day_data.index.minute >= market_open_minute))) &
                ((prev_day_data.index.hour < market_end_hour) | 
                 ((prev_day_data.index.hour == market_end_hour) & (prev_day_data.index.minute <= market_end_minute)))
            ]
        except Exception as e:
            logger.warning(f"Hour/minute filtering failed for {normalized_symbol}: {e}")
            
        # Method 2: If that failed, try string-based filtering
        if filtered_data.empty:
            filtered_rows = []
            filtered_indices = []
            
            for idx, row in prev_day_data.iterrows():
                try:
                    time_str = str(idx.time()) if hasattr(idx, 'time') else str(idx)
                    if ('09:' in time_str and int(time_str.split(':')[1][:2]) >= 15) or '10:' in time_str:
                        filtered_rows.append(row)
                        filtered_indices.append(idx)
                except Exception:
                    pass
            
            if filtered_rows:
                filtered_data = pd.DataFrame(filtered_rows, index=filtered_indices)
        
        # Take the first 10 candles or as many as available
        filtered_data = filtered_data.head(10)
        
        if len(filtered_data) < 3:  # Need at least 3 candles for a reasonable average
            logger.warning(f"Insufficient data points for {normalized_symbol}")
            return None
        
        # Calculate average volume
        avg_volume = filtered_data['Volume'].mean()
        if avg_volume == 0:
            logger.warning(f"Zero average volume for {normalized_symbol}")
            return None
            
        # Get current day data
        current_day_data = get_history_with_fallback(
            ticker,
            start=current_day_str,
            interval="5m"
        )
        
        if not is_valid_volume_data(current_day_data):
            logger.warning(f"Invalid current day data for {normalized_symbol}")
            return None
            
        # Get the latest 5-minute candle
        if len(current_day_data) > 0:
            current_candle = current_day_data.iloc[-1]
            current_volume = current_candle['Volume']
            
            # Ignore if volume is unrealistically low
            if current_volume <= 0:
                logger.warning(f"Zero or negative current volume for {normalized_symbol}")
                return None
                
            # Calculate volume spike ratio
            volume_spike_ratio = current_volume / avg_volume
            
            return {
                'symbol': normalized_symbol,
                'name': name,
                'current_volume': current_volume,
                'avg_volume_prev_day': avg_volume,
                'volume_spike_ratio': volume_spike_ratio
            }
        else:
            logger.warning(f"No current day candles for {normalized_symbol}")
            return None
    
    except Exception as e:
        logger.error(f"Error processing {normalized_symbol}: {e}")
        return None

def get_volume_data(symbols_dict, progress_callback=None):
    """
    Get volume data for all symbols and calculate volume spike ratios with improved reliability
    
    Args:
        symbols_dict: Dictionary mapping symbols to company names
        progress_callback: Function to call with progress (0.0 to 1.0)
        
    Returns:
        DataFrame with volume data and spike ratios
    """
    results = []
    symbols_list = list(symbols_dict.items())
    total_symbols = len(symbols_list)
    
    # Process symbols with better parallelism
    batch_size = 10  # Smaller batch size to avoid overloading
    for i in range(0, total_symbols, batch_size):
        batch = symbols_list[i:i+batch_size]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            batch_results = list(executor.map(fetch_volume_data_for_symbol, batch))
            
            for result in batch_results:
                if result is not None:
                    results.append(result)
        
        # Update progress
        if progress_callback:
            progress_callback(min(1.0, (i + batch_size) / total_symbols))
        
        # Sleep to prevent rate limiting
        time.sleep(1)
    
    # Check if we got enough real data
    if len(results) < 5:
        logger.warning(f"Only found {len(results)} valid stocks. Using sample data.")
        import streamlit as st
        if 'using_sample_data' in st.session_state:
            st.session_state.using_sample_data = True
        sample_df = sample_data.get_sample_volume_data()
        return sample_df
    
    # Create DataFrame from results
    df = pd.DataFrame(results)
    df = df.set_index('symbol')
    
    return df

def get_market_cap(symbol):
    """Get market cap for a single symbol with improved error handling"""
    normalized_symbol = normalize_symbol(symbol)
    
    try:
        # Check if we already have this in the cache
        if normalized_symbol in MARKET_CAP_CACHE['market_caps']:
            return MARKET_CAP_CACHE['market_caps'][normalized_symbol]
            
        # Try using the API first
        max_retries = 3
        retry_delay = 1  # seconds
        market_cap_cr = 0
        
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(normalized_symbol)
                info = ticker.info
                
                # Market cap in USD
                market_cap_usd = info.get('marketCap', 0)
                
                if market_cap_usd > 0:
                    # Convert to INR (rough conversion)
                    usd_to_inr = 83  # Approximate exchange rate
                    market_cap_inr = market_cap_usd * usd_to_inr
                    
                    # Convert to crores (1 crore = 10 million)
                    market_cap_cr = market_cap_inr / 10000000
                    break
                else:
                    # If we got 0 market cap, try again
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for market cap of {normalized_symbol}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                
        # If API failed or returned 0, use fallback for known symbols
        if market_cap_cr <= 0 and normalized_symbol in FALLBACK_MARKET_CAPS:
            logger.info(f"Using fallback market cap for {normalized_symbol}")
            market_cap_cr = FALLBACK_MARKET_CAPS[normalized_symbol]
            
        return market_cap_cr
    
    except Exception as e:
        logger.error(f"Error getting market cap for {normalized_symbol}: {e}")
        # Use fallback value if available
        if normalized_symbol in FALLBACK_MARKET_CAPS:
            return FALLBACK_MARKET_CAPS[normalized_symbol]
        return 0

def get_market_caps(symbols, progress_callback=None):
    """
    Get market caps for all symbols with improved caching
    
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
    
    # Normalize symbols
    normalized_symbols = [normalize_symbol(s) for s in symbols]
    
    # Only fetch data for symbols not in cache
    symbols_to_fetch = [s for s in normalized_symbols if s not in market_caps]
    
    if not symbols_to_fetch:
        # If all symbols are already in cache, return immediately
        if progress_callback:
            progress_callback(1.0)  # Indicate complete progress
        return pd.DataFrame({'market_cap_cr': {s: market_caps.get(normalize_symbol(s), 0) for s in symbols}})
    
    # Try fetching from Yahoo Finance API
    success_count = 0
    total_symbols = len(symbols_to_fetch)
    
    # Process in batches
    batch_size = 10
    for i in range(0, total_symbols, batch_size):
        batch = symbols_to_fetch[i:i+batch_size]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            batch_results = list(executor.map(get_market_cap, batch))
            
            for symbol, market_cap in zip(batch, batch_results):
                if market_cap > 0:
                    success_count += 1
                market_caps[symbol] = market_cap
                # Update the cache
                MARKET_CAP_CACHE['market_caps'][symbol] = market_cap
        
        # Update progress
        if progress_callback:
            progress_callback(min(1.0, (i + batch_size) / total_symbols))
        
        # Sleep to prevent rate limiting
        time.sleep(1)
    
    # If we got very few successful results, use sample data
    if success_count < 5 and len(symbols) > 10:
        logger.warning(f"Only got market cap data for {success_count} symbols. Using sample data.")
        import streamlit as st
        if 'using_sample_data' in st.session_state:
            st.session_state.using_sample_data = True
        sample_market_caps = sample_data.get_sample_market_caps()
        
        # Update our market caps with sample data for missing or zero values
        for symbol in symbols:
            normalized = normalize_symbol(symbol)
            if normalized in sample_market_caps.index and (normalized not in market_caps or market_caps[normalized] <= 0):
                market_cap = sample_market_caps.loc[normalized, 'market_cap_cr']
                market_caps[normalized] = market_cap
                MARKET_CAP_CACHE['market_caps'][normalized] = market_cap
    
    # Update cache timestamp
    if not cache_valid:
        MARKET_CAP_CACHE['timestamp'] = datetime.now()
    
    # Return only the requested symbols
    result_market_caps = {s: market_caps.get(normalize_symbol(s), 0) for s in symbols}
    
    # Create DataFrame from results
    return pd.DataFrame({'market_cap_cr': result_market_caps})
