from datetime import datetime, timedelta
import os
import json
import pandas as pd
import tempfile
import yfinance as yf
from typing import List
from airflow.providers.google.cloud.hooks.gcs import GCSHook  # <-- New import

from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Configuration
GCS_BUCKET = "daily-prices-01"
PROJECT_ID = "portfolio-airflow"
BQ_DATASET = "stock_market"

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='initial_daily_stock_prices',
    default_args=default_args,
    description='Initial run to fetch 30 last days of daily stock price and volume pipeline',
    schedule_interval='@once',
    start_date=datetime(2025, 3, 18),
    catchup=False,
    tags=['stocks', 'prices', 'daily'],
)
def initial_daily_prices_dag():

    @task
    def get_brazilian_tickers() -> List[str]:
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

    @task
    def fetch_all_stock_price_data(ds: str, brazil_tickers: List[str] = None) -> str:
        """Fetch daily price and volume data for the last 30 days for all symbols (US + Brazilian)"""
        end_date = datetime.strptime(ds, '%Y-%m-%d').date()
        start_date = end_date - timedelta(days=30)
        
        start_str = start_date.strftime('%Y-%m-%d')
        # Merge US and Brazilian tickers
        all_symbols = brazil_tickers
        
        # Get historical data for all tickers at once
        tickers = yf.Tickers(' '.join(all_symbols))
        all_data = tickers.history(start=start_str, end=ds, interval="1d")
        
        # Create a single temp file for all records
        temp_file = f"{tempfile.gettempdir()}/all_stock_prices.json"
        
        with open(temp_file, 'w') as f:
            # Process each symbol's data
            for symbol in all_symbols:
                # Extract data for this symbol from the multi-index DataFrame
                symbol_data = all_data.xs(symbol, level=1, axis=1) if len(all_symbols) > 1 else all_data
                
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
        
        return temp_file

    price_schema = [
        {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "open", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "high", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "low", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "close", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "adj_close", "type": "FLOAT"},
        {"name": "volume", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "extraction_date", "type": "DATE", "mode": "REQUIRED"},
    ]
    
    # 1. Load Brazilian tickers and then fetch all stock data
    brazil_tickers = get_brazilian_tickers()
    data_file = fetch_all_stock_price_data(brazil_tickers=brazil_tickers)
    
    # 2. Upload data to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_all_prices_to_gcs',
        src=data_file,
        dst=f'daily_prices/all_stock_prices_{{{{ ds }}}}.json',
        bucket=GCS_BUCKET,
        gcp_conn_id='google_cloud_default',
    )
    
    # 3. Load data to BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id='load_all_prices_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[f'daily_prices/all_stock_prices_{{{{ ds }}}}.json'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.daily_stock_prices',
        schema_fields=price_schema,
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=0,
        gcp_conn_id='google_cloud_default',
    )
    
    # Setting task dependencies
    brazil_tickers >> data_file >> upload_to_gcs >> load_to_bq

# Instantiate the DAG
initial_daily_dag = initial_daily_prices_dag()