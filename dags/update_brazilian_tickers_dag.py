from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta
import json
import tempfile
from airflow.models import Variable

import requests

FMP_API_KEY = Variable.get("FMP_API_KEY")

# Replace with your FMP API key
GCS_BUCKET = "daily-prices-01"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='update_brazil_tickers',
    default_args=default_args,
    description='Update Brazilian tickers once a month',
    schedule_interval='@monthly',
    start_date=datetime(2025, 3, 1),
    catchup=True,
    tags=['tickers', 'brazil', 'monthly'],
)
def update_tickers_dag():

    @task
    def fetch_brazilian_tickers() -> str:
        """
        Fetch the Brazilian tickers from the Brazil exchange (B3) using the FMP API and write them to a JSON file.
        """

        url = "https://financialmodelingprep.com/api/v3/stock-screener"
        params = {
            "exchange": "SAO",
            "apikey": FMP_API_KEY,
            "isEtf": "false",
            "isFund": "false",
            "isActivelyTrading": "true",
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        tickers = [item["symbol"] for item in data]
        
        # Save tickers to a temp file as JSON
        temp_file = f"{tempfile.gettempdir()}/brazil_tickers.json"
        with open(temp_file, 'w') as f:
            json.dump(tickers, f)
        return temp_file

    tickers_file = fetch_brazilian_tickers()

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_brazil_tickers_to_gcs',
        src=tickers_file,
        dst='tickers/brazil_tickers.json',
        bucket=GCS_BUCKET,
        gcp_conn_id='google_cloud_default',
    )

    tickers_file >> upload_to_gcs

# Instantiate the DAG
update_tickers = update_tickers_dag()