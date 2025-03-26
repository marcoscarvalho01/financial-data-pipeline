# Financial Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Airflow](https://img.shields.io/badge/Airflow-2.10.5-red.svg)
![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)
![Google Cloud](https://img.shields.io/badge/GCP-Enabled-blue.svg)

A robust data engineering pipeline for extracting, transforming, and loading financial market data, focusing on Brazilian stocks.

This project implements a modern data pipeline using Apache Airflow to orchestrate the extraction of financial data from various sources, store it in Google Cloud Storage, and load it into BigQuery for analysis.

## Features

- **Automated Stock Data Collection**: Daily collection of stock prices, volumes, and fundamental data
- **Brazilian Market Focus**: Specialized in extracting data from the B3 (Brazilian Stock Exchange)
- **Scalable Architecture**: Containerized with Docker and designed to handle multiple data sources
- **Cloud-Native**: Leverages Google Cloud for storage and analytics
- **Scheduled Pipelines**: Various ETL jobs running on different schedules (daily, monthly, quarterly)

## Technologies Used

- **Apache Airflow**: Workflow orchestration
- **Docker & Docker Compose**: Containerization and local development
- **Google Cloud Platform**:
  - Google Cloud Storage (GCS): Data lake storage
  - BigQuery: Data warehouse
- **Python**: ETL processing and data manipulation
- **yfinance**: Financial data extraction
- **Pandas**: Data transformation

## Setup and Deployment

### Prerequisites

- Docker and Docker Compose
- Google Cloud account with GCS and BigQuery enabled
- GCP service account credentials with appropriate permissions

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/financial-data-pipeline.git
   cd financial-data-pipeline
   ```

2. Set up environment variables:
   ```bash
   cp .env.example .env
   # Edit .env file with your GCP details and API keys
   ```

3. Place your GCP service account JSON file in the project root:
   ```bash
   # Rename your key file to service_account_credentials.json
   ```

4. Start the Airflow services:
   ```bash
   docker-compose up -d
   ```

5. Access the Airflow UI at http://localhost:8080 (default credentials: airflow/airflow)

## Project Structure

```
financial-data-pipeline/
├── dags/                  # Airflow DAG definitions
│   ├── daily_prices_dag.py            # Daily stock price updates
│   ├── initial_daily_prices_dag.py    # Initial historical data load
│   ├── quarterly_fundamentals_dag.py  # Quarterly fundamental data
│   └── update_brazilian_tickers_dag.py# Updates list of Brazilian stocks
├── plugins/              
│   └── utils/             # Utility functions and shared code
│       ├── schema_definitions.py      # BigQuery schema definitions
│       ├── shared_configs.py          # Shared configuration values
│       ├── stock_fundamentals.py      # Functions for fundamental data
│       └── stock_price.py             # Functions for price data
├── logs/                  # Airflow logs
├── Dockerfile             # Custom Airflow image definition
├── docker-compose.yaml    # Docker Compose configuration
├── requirements.txt       # Python dependencies
└── README.md              # Project documentation
```

## How It Works

This pipeline handles several types of financial data:

1. **Daily Stock Prices**: 
   - Collects end-of-day pricing data for all Brazilian stocks
   - Stores raw data in GCS and processed data in BigQuery
   - Runs daily at 6:00 AM

2. **Stock Tickers Management**:
   - Updates the list of active Brazilian stock tickers monthly
   - Uses Financial Modeling Prep API to get the latest listed companies

3. **Fundamental Data** (Quarterly):
   - Collects balance sheets, income statements, and cash flow data
   - Maintains historical records for financial analysis
   - Processes and transforms data into analytics-ready format

## Data Flow

1. **Extraction**: Data is pulled from financial APIs using yfinance and other providers
2. **Transformation**: Raw data is cleaned, normalized, and enriched using Python
3. **Storage**: Processed data is stored in GCS as JSON files
4. **Loading**: Data is loaded into BigQuery tables with predefined schemas
5. **Orchestration**: All workflows are managed by Airflow with appropriate scheduling and error handling

## Future Improvements

- Add support for additional financial markets (US, Europe, Asia)
- Implement data quality checks and monitoring
- Create data visualization dashboards using Looker or other BI tools
- Add sentiment analysis from financial news sources
- Implement machine learning models for price prediction

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

Marcos Carvalho - marcos01.dev@gmail.com

---

*This project is for educational and portfolio purposes. Financial data is sourced from publicly available APIs.*