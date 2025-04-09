from datetime import datetime, time as dt_time
import pytz

def get_current_time_ist():
    """Get current time in Indian Standard Time (IST)"""
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

def is_market_open(current_time=None):
    """
    Check if the Indian stock market is open
    
    Args:
        current_time: datetime object in IST timezone, or None to use current time
        
    Returns:
        Boolean indicating if market is open
    """
    if current_time is None:
        current_time = get_current_time_ist()
    
    # Market is closed on weekends
    if current_time.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        return False
    
    # Market hours: 9:15 AM to 3:30 PM
    market_open_time = dt_time(9, 15, 0)
    market_close_time = dt_time(15, 30, 0)
    
    current_time_only = current_time.time()
    
    return market_open_time <= current_time_only <= market_close_time

def format_market_cap(market_cap):
    """Format market cap for display"""
    if market_cap >= 1000:
        return f"{market_cap/1000:.2f}K"
    else:
        return f"{market_cap:.2f}"
