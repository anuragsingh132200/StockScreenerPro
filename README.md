# Indian Stock Volume Screener

## Overview

This application screens Indian stocks (NSE/BSE) based on specific volume and market capitalization criteria. It identifies stocks with significant trading activity that may indicate potential market movements.

## Features

- **Volume Spike Detection**: Identifies stocks where the current 5-minute volume is at least 10× the average of the previous day's first 10 five-minute candles
- **Market Cap Filter**: Only includes stocks with market caps greater than ₹1000 crore
- **Auto-refresh**: Configurable refresh intervals to keep data current during market hours
- **Real-time Data**: Uses financial API data for near real-time stock information
- **Market Awareness**: Displays current market status (OPEN/CLOSED) and operates according to Indian market hours

## Screening Criteria

1. **Volume Spike Detection**:
   - For each company, fetches the first 10 five-minute volume candles from the previous trading day
   - Calculates the average volume of these 10 candles
   - Compares the current 5-minute volume candle to this average
   - Includes stocks only if the current volume is at least 10 times higher than the average

2. **Market Cap Filter**:
   - Company's market capitalization must be greater than ₹1000 crore

3. **Result Sorting**:
   - The top 10 stocks meeting the criteria are displayed, sorted by highest current 5-minute volume

## Usage

To run the application locally:

```bash
streamlit run app.py
```

## Data Sources

- Stock symbols and basic information from NSE/BSE
- Historical and current price/volume data retrieved via financial APIs
- Market capitalization data calculated from current price and outstanding shares

## Market Hours

The Indian stock market (NSE and BSE) operates from 9:15 AM to 3:30 PM IST, Monday to Friday, excluding market holidays.

## Dependencies

See the `dependencies.md` file for a detailed list of project dependencies.