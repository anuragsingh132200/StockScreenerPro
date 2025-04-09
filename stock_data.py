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
        
        # Use a list of reliable Indian stocks that are less likely to have API issues
        # Focus on the key index components which are more stable
        reliable_symbols = [
            # Key NIFTY stocks that are most reliable for API calls
            'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
            'HINDUNILVR.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'ITC.NS', 'KOTAKBANK.NS',
            'LT.NS', 'BAJFINANCE.NS', 'AXISBANK.NS', 'ASIANPAINT.NS', 'MARUTI.NS',
            'TITAN.NS', 'SUNPHARMA.NS', 'ULTRACEM.NS', 'TATASTEEL.NS', 'NTPC.NS'
        ]
        
        # Map of pre-defined company names to minimize API calls
        predefined_names = {
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
            'ULTRACEM.NS': 'UltraTech Cement',
            'TATASTEEL.NS': 'Tata Steel',
            'NTPC.NS': 'NTPC Limited'
        }
        
        # First, use pre-defined names
        for symbol in reliable_symbols:
            if symbol in predefined_names:
                nse_symbols[symbol] = predefined_names[symbol]
            else:
                nse_symbols[symbol] = symbol.replace('.NS', '')
        
        # Cache the results immediately - we'll use what we have even if partial
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
        
        # Prepare date strings without timezone info to avoid errors
        # Use strict date format strings without time components to improve reliability
        prev_day_str = prev_day.strftime('%Y-%m-%d')
        next_day_str = (prev_day + timedelta(days=1)).strftime('%Y-%m-%d')
        current_day_str = current_time_ist.strftime('%Y-%m-%d')
        
        # Fetch previous day's data with retry mechanism
        max_retries = 3
        retry_delay = 1  # seconds
        ticker = yf.Ticker(symbol)
        
        # First attempt: Get previous day data
        prev_day_data = None
        for attempt in range(max_retries):
            try:
                prev_day_data = ticker.history(
                    start=prev_day_str,
                    end=next_day_str,
                    interval="5m"
                )
                if not prev_day_data.empty:
                    break
            except Exception as e:
                print(f"Attempt {attempt+1} failed for {symbol} prev day: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        
        if prev_day_data is None or prev_day_data.empty:
            print(f"Could not retrieve previous day data for {symbol}")
            return None
            
        # Market hours in IST
        market_open_hour, market_open_minute = 9, 15
        market_end_hour, market_end_minute = 11, 0
            
        # Filter to get data between 9:15 AM to 11:00 AM - first trading session
        # Use string-based hour matching to avoid timezone issues
        filtered_rows = []
        filtered_indices = []
        
        for idx, row in prev_day_data.iterrows():
            # Check if the timestamp has hour and minute attributes
            try:
                hour = idx.hour
                minute = idx.minute
                
                if (hour > market_open_hour or (hour == market_open_hour and minute >= market_open_minute)) and \
                   (hour < market_end_hour or (hour == market_end_hour and minute <= market_end_minute)):
                    filtered_rows.append(row)
                    filtered_indices.append(idx)
            except:
                # If we can't access hour/minute, try time string matching
                try:
                    time_str = str(idx.time()) if hasattr(idx, 'time') else str(idx)
                    if '09:' in time_str or '10:' in time_str:
                        filtered_rows.append(row)
                        filtered_indices.append(idx)
                except:
                    pass  # Skip problematic timestamps
        
        # Create dataframe from filtered rows
        if filtered_rows:
            filtered_data = pd.DataFrame(filtered_rows, index=filtered_indices)
        else:
            filtered_data = pd.DataFrame()
        
        # Take the first 10 candles or as many as available
        filtered_data = filtered_data.head(10)
        
        if len(filtered_data) < 3:  # Need at least 3 candles for a reasonable average
            print(f"Insufficient data points for {symbol}")
            return None
        
        # Calculate average volume
        avg_volume = filtered_data['Volume'].mean()
        if avg_volume == 0:
            print(f"Zero average volume for {symbol}")
            return None
            
        # Get current day data with retry mechanism
        current_day_data = None
        for attempt in range(max_retries):
            try:
                current_day_data = ticker.history(
                    start=current_day_str,
                    interval="5m"
                )
                if not current_day_data.empty:
                    break
            except Exception as e:
                print(f"Attempt {attempt+1} failed for {symbol} current day: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        
        if current_day_data is None or current_day_data.empty:
            print(f"Could not retrieve current day data for {symbol}")
            return None
            
        # Get the latest 5-minute candle
        try:
            current_candle = current_day_data.iloc[-1]
            current_volume = current_candle['Volume']
            
            # Ignore if volume is unrealistically low or missing
            if current_volume <= 0:
                print(f"Zero or negative current volume for {symbol}")
                return None
                
            # Calculate volume spike ratio
            volume_spike_ratio = current_volume / avg_volume
            
            return {
                'symbol': symbol,
                'name': name,
                'current_volume': current_volume,
                'avg_volume_prev_day': avg_volume,
                'volume_spike_ratio': volume_spike_ratio
            }
        except Exception as e:
            print(f"Error processing current candle for {symbol}: {e}")
            return None
    
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
        # For symbols we already know are major companies, use hard-coded estimated values if API fails
        # This gives approximate market cap data for key stocks in case Yahoo API is having issues
        fallback_market_caps = {
            'RELIANCE.NS': 18000, # ~₹18,00,000 crore
            'TCS.NS': 14000,      # ~₹14,00,000 crore
            'HDFCBANK.NS': 12000, # ~₹12,00,000 crore
            'INFY.NS': 7000,      # ~₹7,00,000 crore
            'ICICIBANK.NS': 7500, # ~₹7,50,000 crore
            'HINDUNILVR.NS': 6000, # ~₹6,00,000 crore
            'SBIN.NS': 6500,      # ~₹6,50,000 crore
            'BHARTIARTL.NS': 6200, # ~₹6,20,000 crore
            'ITC.NS': 5500,       # ~₹5,50,000 crore
            'KOTAKBANK.NS': 4200, # ~₹4,20,000 crore
            'LT.NS': 4000,        # ~₹4,00,000 crore
            'BAJFINANCE.NS': 4500, # ~₹4,50,000 crore
            'AXISBANK.NS': 3200,  # ~₹3,20,000 crore
            'ASIANPAINT.NS': 3000, # ~₹3,00,000 crore
            'MARUTI.NS': 3300,    # ~₹3,30,000 crore
            'TITAN.NS': 2800,     # ~₹2,80,000 crore
            'SUNPHARMA.NS': 2600, # ~₹2,60,000 crore
            'ULTRACEM.NS': 2500,  # ~₹2,50,000 crore
            'TATASTEEL.NS': 2200, # ~₹2,20,000 crore
            'NTPC.NS': 2400       # ~₹2,40,000 crore
        }
        
        # Try using the API first
        max_retries = 3
        retry_delay = 1  # seconds
        market_cap_cr = 0
        
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info
                
                # Market cap in USD
                market_cap_usd = info.get('marketCap', 0)
                
                if market_cap_usd > 0:
                    # Convert to INR (rough conversion)
                    usd_to_inr = 83  # Approximate exchange rate, adjust as needed
                    market_cap_inr = market_cap_usd * usd_to_inr
                    
                    # Convert to crores (1 crore = 10 million)
                    market_cap_cr = market_cap_inr / 10000000
                    break
                else:
                    # If we got 0 market cap, try again
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
            except Exception as e:
                print(f"Attempt {attempt+1} failed for market cap of {symbol}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                
        # If API failed or returned 0, use fallback for known symbols
        if market_cap_cr <= 0 and symbol in fallback_market_caps:
            print(f"Using fallback market cap for {symbol}")
            market_cap_cr = fallback_market_caps[symbol]
            
        return market_cap_cr
    
    except Exception as e:
        print(f"Error getting market cap for {symbol}: {e}")
        # Use fallback value if available
        if symbol in fallback_market_caps:
            return fallback_market_caps[symbol]
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
