from datetime import datetime, timedelta
import tempfile
import yfinance as yf
from typing import List

from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Import from your existing utils
from utils.schema_definitions import PRICE_SCHEMA
from utils.stock_price import fetch_brazilian_tickers, process_stock_data
from utils.shared_configs import GCS_BUCKET, PROJECT_ID, BQ_DATASET

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='daily_stock_prices_update',
    default_args=default_args,
    description='Daily update of stock price and volume data for the previous day',
    schedule_interval='0 6 * * *',  # Run at 6 AM daily
    start_date=datetime(2025, 3, 19),  # Start the day after initial load
    catchup=False,
    tags=['stocks', 'prices', 'daily', 'update'],
)
def daily_prices_dag():

    @task
    def get_brazilian_tickers() -> List[str]:
        """Task wrapper for the helper function"""
        return fetch_brazilian_tickers()

    @task
    def fetch_previous_day_stock_data(ds: str, symbols: List[str] = None) -> str:
        """Fetch daily price and volume data only for the previous day for all symbols"""
        # Calculate dates (ds is the execution date, we want the previous day's data)
        execution_date = datetime.strptime(ds, '%Y-%m-%d').date()
        previous_date = execution_date - timedelta(days=1)
        previous_date_str = previous_date.strftime('%Y-%m-%d')
        
        # We'll fetch just 2 days of data and filter to ensure we get the exact day
        # (this adds some resilience if the previous day has no data)
        start_date = previous_date - timedelta(days=1)
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        # Get historical data for all tickers at once
        tickers = yf.Tickers(' '.join(symbols))
        all_data = tickers.history(start=start_date_str, end=ds, interval="1d")
        
        # Create a temp file for records
        temp_file = f"{tempfile.gettempdir()}/daily_stock_prices_{previous_date_str}.json"
        
        # Filter data to only include the previous day
        filtered_data = all_data[all_data.index.strftime('%Y-%m-%d') == previous_date_str]
        
        # Process the data using the shared helper function
        process_stock_data(symbols, filtered_data, temp_file)
        
        return temp_file
    
    # 1. Load Brazilian tickers and then fetch previous day's stock data
    brazil_tickers = get_brazilian_tickers()
    data_file = fetch_previous_day_stock_data(symbols=brazil_tickers)
    
    # 2. Upload data to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_daily_prices_to_gcs',
        src=data_file,
        dst=f'daily_prices/daily_stock_prices_{{{{ ds }}}}.json',
        bucket=GCS_BUCKET,
        gcp_conn_id='google_cloud_default',
    )
    
    # 3. Load data to BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id='load_daily_prices_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[f'daily_prices/daily_stock_prices_{{{{ ds }}}}.json'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.daily_stock_prices',
        schema_fields=PRICE_SCHEMA,  # Use the imported schema
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND',  # Append to the existing table
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=0,
        gcp_conn_id='google_cloud_default',
    )
    
    # Setting task dependencies
    brazil_tickers >> data_file >> upload_to_gcs >> load_to_bq

# Instantiate the DAG
daily_prices = daily_prices_dag()