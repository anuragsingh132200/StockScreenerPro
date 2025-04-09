"""
Sample stock data to use when Yahoo Finance API is unavailable.
This ensures the application can demonstrate its functionality even when
external data sources are down or unavailable.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

def get_sample_symbols():
    """Return a dictionary of sample symbols and company names."""
    return {
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
        'ULTRACEM.NS': 'UltraTech Cement',  # Both ticker variations
        'TATASTEEL.NS': 'Tata Steel',
        'NTPC.NS': 'NTPC Limited'
    }

def get_sample_volume_data():
    """
    Generate sample volume data to demonstrate volume spike filtering.
    
    Returns:
        DataFrame with synthetic volume data showing some stocks with volume spikes.
    """
    symbols = get_sample_symbols()
    
    # Create synthetic data with some volume spikes
    data = []
    np.random.seed(42)  # For reproducible results
    
    for symbol, name in symbols.items():
        # Generate random volume data
        avg_volume = np.random.randint(10000, 1000000)
        
        # Create some volume spikes (20% of stocks will have spikes > 10x)
        if np.random.random() < 0.2:  
            spike_ratio = np.random.uniform(10, 20)  # Between 10x and 20x
            current_volume = int(avg_volume * spike_ratio)
        else:
            spike_ratio = np.random.uniform(0.5, 9.5)  # Less than 10x
            current_volume = int(avg_volume * spike_ratio)
            
        data.append({
            'symbol': symbol,
            'name': name,
            'current_volume': current_volume,
            'avg_volume_prev_day': avg_volume,
            'volume_spike_ratio': spike_ratio
        })
    
    df = pd.DataFrame(data)
    df = df.set_index('symbol')
    
    return df

def get_sample_market_caps():
    """
    Generate sample market cap data.
    
    Returns:
        DataFrame with market cap data for sample symbols.
    """
    market_caps = {
        'RELIANCE.NS': 18000,  # ~₹18,00,000 crore
        'TCS.NS': 14000,       # ~₹14,00,000 crore
        'HDFCBANK.NS': 12000,  # ~₹12,00,000 crore
        'INFY.NS': 7000,       # ~₹7,00,000 crore
        'ICICIBANK.NS': 7500,  # ~₹7,50,000 crore
        'HINDUNILVR.NS': 6000, # ~₹6,00,000 crore
        'SBIN.NS': 6500,       # ~₹6,50,000 crore
        'BHARTIARTL.NS': 6200, # ~₹6,20,000 crore
        'ITC.NS': 5500,        # ~₹5,50,000 crore
        'KOTAKBANK.NS': 4200,  # ~₹4,20,000 crore
        'LT.NS': 4000,         # ~₹4,00,000 crore
        'BAJFINANCE.NS': 4500, # ~₹4,50,000 crore
        'AXISBANK.NS': 3200,   # ~₹3,20,000 crore
        'ASIANPAINT.NS': 3000, # ~₹3,00,000 crore
        'MARUTI.NS': 3300,     # ~₹3,30,000 crore
        'TITAN.NS': 2800,      # ~₹2,80,000 crore
        'SUNPHARMA.NS': 2600,  # ~₹2,60,000 crore
        'ULTRACEMCO.NS': 2500, # ~₹2,50,000 crore
        'ULTRACEM.NS': 2500,   # ~₹2,50,000 crore (alternative symbol)
        'TATASTEEL.NS': 2200,  # ~₹2,20,000 crore
        'NTPC.NS': 2400        # ~₹2,40,000 crore
    }
    
    return pd.DataFrame({'market_cap_cr': market_caps})