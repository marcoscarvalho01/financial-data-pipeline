from datetime import datetime
import json
import pandas as pd
import tempfile
import yfinance as yf
from typing import List
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from utils.shared_configs import GCS_BUCKET

def fetch_brazilian_tickers() -> List[str]:
    """
    Download Brazilian tickers from GCS.
    Assumes the file 'tickers/brazil_tickers.json' exists in the bucket and contains a JSON list.
    """
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    file_content = hook.download(
        bucket_name=GCS_BUCKET, 
        object_name='tickers/brazil_tickers.json'
    )
    tickers = json.loads(file_content)
    return tickers

def process_stock_data(symbols: List[str], data, output_file: str):
    """Process stock data and write to JSON file"""
    with open(output_file, 'w') as f:
        # Process each symbol's data
        for symbol in symbols:
            # Extract data for this symbol from the multi-index DataFrame
            symbol_data = data.xs(symbol, level=1, axis=1) if len(symbols) > 1 else data
            
            if symbol_data is not None and not symbol_data.empty:
                for index, row in symbol_data.iterrows():
                    record = {
                        'symbol': symbol,
                        'date': index.strftime('%Y-%m-%d'),
                        'open': float(row['Open']) if pd.notnull(row['Open']) else 0.0,
                        'high': float(row['High']) if pd.notnull(row['High']) else 0.0,
                        'low': float(row['Low']) if pd.notnull(row['Low']) else 0.0,
                        'close': float(row['Close']) if pd.notnull(row['Close']) else 0.0,
                        'volume': int(row['Volume']) if pd.notnull(row['Volume']) else 0,
                        'extraction_date': datetime.now().strftime('%Y-%m-%d'),
                    }
                    
                    if 'Adj Close' in row and pd.notnull(row['Adj Close']):
                        record['adj_close'] = float(row['Adj Close'])
                    
                    f.write(json.dumps(record) + '\n')