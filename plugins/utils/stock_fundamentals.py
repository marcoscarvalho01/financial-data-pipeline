import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dateutil.relativedelta import relativedelta

def get_last_5_years_quarters() -> List[Tuple[str, str]]:
    """
    Returns a list of quarter start/end dates for the past 5 years
    Returns list of tuples (quarter_start, quarter_end)
    """
    quarters = []
    now = datetime.now()
    
    # Get 5 years of quarters
    for i in range(20):  # 5 years * 4 quarters
        # Calculate the quarter end date
        year = now.year - (i // 4)
        quarter = 4 - (i % 4)
        
        if quarter == 1:
            quarter_end = datetime(year, 3, 31)
            quarter_start = datetime(year, 1, 1)
        elif quarter == 2:
            quarter_end = datetime(year, 6, 30)
            quarter_start = datetime(year, 4, 1)
        elif quarter == 3:
            quarter_end = datetime(year, 9, 30)
            quarter_start = datetime(year, 7, 1)
        else:  # quarter == 4
            quarter_end = datetime(year, 12, 31)
            quarter_start = datetime(year, 10, 1)
            
        # Skip future quarters
        if quarter_end > now:
            continue
            
        quarters.append((
            quarter_start.strftime('%Y-%m-%d'),
            quarter_end.strftime('%Y-%m-%d')
        ))
    
    return quarters

def fetch_quarterly_fundamentals(symbol: str, report_type: str, quarters: List[Tuple[str, str]]) -> List[Dict[str, Any]]:
    """
    Fetch quarterly fundamental data for a given symbol
    
    Args:
        symbol: Stock ticker symbol
        report_type: One of 'income_statement', 'balance_sheet', 'cash_flow'
        quarters: List of quarter tuples (start_date, end_date)
        
    Returns:
        List of records for loading into BigQuery
    """
    ticker = yf.Ticker(symbol)
    
    # Get the data based on report type
    if report_type == 'income_statement':
        data = ticker.quarterly_financials
    elif report_type == 'balance_sheet':
        data = ticker.quarterly_balance_sheet
    elif report_type == 'cash_flow':
        data = ticker.quarterly_cashflow
    else:
        raise ValueError(f"Invalid report type: {report_type}")
    
    # Transform the data for BigQuery
    records = []
    
    if data is not None and not data.empty:
        for date, column in data.items():
            # Get the quarter end date in string format
            quarter_end = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
            
            # Find the matching quarter
            quarter_start = None
            for start, end in quarters:
                if end == quarter_end:
                    quarter_start = start
                    break
            
            # Skip if we can't match the quarter
            if quarter_start is None:
                continue
            
            record = {
                'symbol': symbol,
                'report_type': report_type,
                'quarter_start': quarter_start,
                'quarter_end': quarter_end,
                'extraction_date': datetime.now().strftime('%Y-%m-%d'),
            }
            
            # Add all the financial values
            for index, value in column.items():
                if pd.notnull(value):
                    # Clean field name
                    field_name = str(index).lower().replace(' ', '_').replace('-', '_')
                    record[field_name] = float(value)
            
            records.append(record)
    
    return records

def fetch_daily_price_data(symbol: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
    """
    Fetch daily price and volume data for a given symbol
    
    Args:
        symbol: Stock ticker symbol
        start_date: Start date for data retrieval
        end_date: End date for data retrieval
        
    Returns:
        List of records for loading into BigQuery
    """
    # Format dates for yfinance
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Get historical data from yfinance
    ticker = yf.Ticker(symbol)
    data = ticker.history(start=start_str, end=end_str, interval="1d")
    
    # Transform the data for BigQuery
    records = []
    
    if data is not None and not data.empty:
        for index, row in data.iterrows():
            record = {
                'symbol': symbol,
                'date': index.strftime('%Y-%m-%d'),
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': int(row['Volume']),
                'extraction_date': datetime.now().strftime('%Y-%m-%d'),
            }
            
            # Add adjusted close if available
            if 'Adj Close' in row:
                record['adj_close'] = float(row['Adj Close'])