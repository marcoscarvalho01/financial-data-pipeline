from google.cloud import bigquery

PRICE_SCHEMA = [
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

# Define schemas for different yfinance data types
FUNDAMENTALS_SCHEMA = {
    'info': [
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),  # Logical date of the pipeline run
        bigquery.SchemaField("extraction_timestamp", "TIMESTAMP", mode="REQUIRED"),  # Actual extraction time
        bigquery.SchemaField("shortName", "STRING"),
        bigquery.SchemaField("longName", "STRING"),
        bigquery.SchemaField("sector", "STRING"),
        bigquery.SchemaField("industry", "STRING"),
        bigquery.SchemaField("marketCap", "FLOAT"),
        bigquery.SchemaField("currentPrice", "FLOAT"),
        bigquery.SchemaField("targetHighPrice", "FLOAT"),
        bigquery.SchemaField("targetLowPrice", "FLOAT"),
        bigquery.SchemaField("targetMeanPrice", "FLOAT"),
        bigquery.SchemaField("recommendationMean", "FLOAT"),
        bigquery.SchemaField("recommendationKey", "STRING"),
        bigquery.SchemaField("trailingPE", "FLOAT"),
        bigquery.SchemaField("forwardPE", "FLOAT"),
        bigquery.SchemaField("dividendYield", "FLOAT"),
        bigquery.SchemaField("payoutRatio", "FLOAT"),
        bigquery.SchemaField("beta", "FLOAT"),
        bigquery.SchemaField("enterpriseValue", "FLOAT"),
        bigquery.SchemaField("profitMargins", "FLOAT"),
        # Add more fields as needed
    ],
    
    'financials': [
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),  # Logical date of the pipeline run
        bigquery.SchemaField("extraction_timestamp", "TIMESTAMP", mode="REQUIRED"),  # Actual extraction time
        bigquery.SchemaField("statement_date", "STRING", mode="REQUIRED"),  # Date on the financial statement
        bigquery.SchemaField("total_revenue", "FLOAT"),
        bigquery.SchemaField("cost_of_revenue", "FLOAT"),
        bigquery.SchemaField("gross_profit", "FLOAT"),
        bigquery.SchemaField("research_development", "FLOAT"),
        bigquery.SchemaField("operating_expense", "FLOAT"),
        bigquery.SchemaField("operating_income", "FLOAT"),
        bigquery.SchemaField("net_income", "FLOAT"),
        # Add more fields based on what yfinance returns
    ],
    
    'balance_sheet': [
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),  # Logical date of the pipeline run
        bigquery.SchemaField("extraction_timestamp", "TIMESTAMP", mode="REQUIRED"),  # Actual extraction time
        bigquery.SchemaField("statement_date", "STRING", mode="REQUIRED"),  # Date on the balance sheet
        bigquery.SchemaField("total_assets", "FLOAT"),
        bigquery.SchemaField("total_liabilities", "FLOAT"),
        bigquery.SchemaField("total_stockholder_equity", "FLOAT"),
        bigquery.SchemaField("cash", "FLOAT"),
        bigquery.SchemaField("total_cash", "FLOAT"),
        bigquery.SchemaField("total_debt", "FLOAT"),
        bigquery.SchemaField("current_debt", "FLOAT"),
        # Add more fields based on what yfinance returns
    ]
}