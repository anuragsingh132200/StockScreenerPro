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

# Cache for stock symbols
SYMBOL_CACHE = {
    'timestamp': None,
    'symbols': None
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
        
        # Try to fetch NSE listed companies
        try:
            nse_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
            nse_data = pd.read_csv(nse_url)
            for _, row in nse_data.iterrows():
                symbol = row['SYMBOL']
                name = row['NAME OF COMPANY']
                nse_symbols[symbol + '.NS'] = name
        except Exception as e:
            print(f"Error fetching NSE symbols: {e}")
        
        # If NSE fetch failed, use a smaller sample for testing
        if not nse_symbols:
            # List of top Indian stocks as a fallback
            default_symbols = [
                'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
                'HINDUNILVR.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'ITC.NS', 'KOTAKBANK.NS',
                'LT.NS', 'BAJFINANCE.NS', 'AXISBANK.NS', 'ASIANPAINT.NS', 'MARUTI.NS',
                'TITAN.NS', 'SUNPHARMA.NS', 'ULTRACEMCO.NS', 'TATASTEEL.NS', 'NTPC.NS'
            ]
            
            # Get company names for default symbols
            for symbol in default_symbols:
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.info
                    name = info.get('longName', symbol.replace('.NS', ''))
                    nse_symbols[symbol] = name
                except Exception:
                    nse_symbols[symbol] = symbol.replace('.NS', '')
        
        # For simplicity, we're focusing on NSE stocks only as they cover most of the Indian market
        SYMBOL_CACHE['timestamp'] = datetime.now()
        SYMBOL_CACHE['symbols'] = nse_symbols
        
        return nse_symbols
        
    except Exception as e:
        print(f"Error getting stock symbols: {e}")
        return {}

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
        
        # Start and end times for previous day data
        prev_day_start = prev_day.replace(hour=9, minute=15, second=0, microsecond=0)
        prev_day_end = prev_day.replace(hour=11, minute=0, second=0, microsecond=0)
        
        # Fetch previous day's 5-minute candles
        ticker = yf.Ticker(symbol)
        prev_day_data = ticker.history(
            start=prev_day_start.strftime('%Y-%m-%d'),
            end=(prev_day + timedelta(days=1)).strftime('%Y-%m-%d'),
            interval="5m"
        )
        
        if prev_day_data.empty:
            return None
        
        # Get the first 10 candles of the previous day
        trading_start_time = pd.Timestamp(prev_day_start, tz='Asia/Kolkata')
        trading_end_time = pd.Timestamp(prev_day_end, tz='Asia/Kolkata')
        
        # Filter to trading hours and get first 10 candles
        prev_day_data = prev_day_data.between_time(
            trading_start_time.time(),
            trading_end_time.time()
        ).head(10)
        
        if len(prev_day_data) < 5:  # Need at least 5 candles for reasonable average
            return None
        
        # Calculate average volume
        avg_volume = prev_day_data['Volume'].mean()
        
        # Get current 5-minute candle
        current_day_start = current_time_ist.replace(hour=9, minute=15, second=0, microsecond=0)
        current_day_data = ticker.history(
            start=current_day_start.strftime('%Y-%m-%d'),
            end=current_time_ist.strftime('%Y-%m-%d %H:%M:%S'),
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
    
    # Process in smaller batches to avoid rate limiting
    batch_size = 10
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
        
        # Sleep to avoid rate limiting
        time.sleep(1)
    
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
    market_caps = {}
    total_symbols = len(symbols)
    
    # Process in smaller batches to avoid rate limiting
    batch_size = 10
    for i in range(0, total_symbols, batch_size):
        batch = symbols[i:i+batch_size]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            batch_results = list(executor.map(get_market_cap, batch))
            
            for symbol, market_cap in zip(batch, batch_results):
                market_caps[symbol] = market_cap
        
        # Update progress
        if progress_callback:
            progress_callback(min(1.0, (i + batch_size) / total_symbols))
        
        # Sleep to avoid rate limiting
        time.sleep(1)
    
    # Create DataFrame from results
    return pd.DataFrame({'market_cap_cr': market_caps})
