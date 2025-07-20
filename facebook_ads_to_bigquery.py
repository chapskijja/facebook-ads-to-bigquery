from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from google.cloud import bigquery
import os
import dotenv 
from datetime import datetime, timedelta
import time

# Load environment variables from .env file
dotenv.load_dotenv()

# Facebook API setup
app_id = os.getenv('FACEBOOK_APP_ID')
app_secret = os.getenv('FACEBOOK_APP_SECRET')
access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
ad_account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')

FacebookAdsApi.init(app_id, app_secret, access_token, api_version='v17.0')
account = AdAccount(ad_account_id)

# BigQuery setup
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
client = bigquery.Client()
dataset_id = 'feettech_crm'
table_id = 'facebook_ads'

# Define the schema for BigQuery table
schema = [
    bigquery.SchemaField("account_name", "STRING"),
    bigquery.SchemaField("campaign", "STRING"),
    bigquery.SchemaField("adset_name", "STRING"),
    bigquery.SchemaField("ad_name", "STRING"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("impressions", "INTEGER"),
    bigquery.SchemaField("clicks", "INTEGER"),
    bigquery.SchemaField("spend", "FLOAT"),
    bigquery.SchemaField("cpc", "FLOAT"),
    bigquery.SchemaField("cpm", "FLOAT"),
    bigquery.SchemaField("ctr", "FLOAT"),
    bigquery.SchemaField("frequency", "FLOAT"),
    bigquery.SchemaField("unique_ctr", "FLOAT"),
    bigquery.SchemaField("conversions", "INTEGER"),
    bigquery.SchemaField("cost_per_conversion", "FLOAT"),
    bigquery.SchemaField("unique_conversions", "INTEGER"),
]

# Define the fields we want to fetch
fields = [
    AdsInsights.Field.account_name,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.adset_name,
    AdsInsights.Field.ad_name,
    AdsInsights.Field.date_start,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.spend,
    AdsInsights.Field.cpc,
    AdsInsights.Field.cpm,
    AdsInsights.Field.ctr,
    AdsInsights.Field.unique_ctr,
    AdsInsights.Field.frequency,
    AdsInsights.Field.conversions,
    AdsInsights.Field.cost_per_conversion,
    AdsInsights.Field.unique_conversions,
]

def get_conversion_value(conversions_data, action_type):
    """
    Extract conversion value for specific action type from conversions list
    """
    if not conversions_data or not isinstance(conversions_data, list):
        return 0
    
    for conversion in conversions_data:
        if isinstance(conversion, dict) and conversion.get('action_type') == action_type:
            try:
                return int(conversion.get('value', 0))
            except (ValueError, TypeError):
                return 0
    return 0

def get_cost_per_conversion_value(cost_data, action_type):
    """
    Extract cost per conversion value for specific action type
    """
    if not cost_data or not isinstance(cost_data, list):
        return 0.0
    
    for cost in cost_data:
        if isinstance(cost, dict) and cost.get('action_type') == action_type:
            try:
                return float(cost.get('value', 0))
            except (ValueError, TypeError):
                return 0.0
    return 0.0

def fetch_and_load_data(start_date, end_date):
    params = {
        'time_range': {'since': start_date.strftime('%Y-%m-%d'), 'until': end_date.strftime('%Y-%m-%d')},
        'level': 'ad',
        'time_increment': 1,  # This will give daily breakdown
    }

    try:
        print(f"Fetching insights for ad account: {ad_account_id}")
        print(f"Date range: {start_date} to {end_date}")
        insights = account.get_insights(fields=fields, params=params)

        # Transform data
        rows_to_insert = []
        for insight in insights:
            row = {
                'account_name': insight.get('account_name'),
                'campaign': insight.get('campaign_name'),
                'adset_name': insight.get('adset_name'),
                'ad_name': insight.get('ad_name'),
                'date': insight.get('date_start'),
                'impressions': int(insight.get('impressions', 0)),
                'clicks': int(insight.get('clicks', 0)),
                'spend': float(insight.get('spend', 0)),
                'cpc': float(insight.get('cpc', 0)),
                'cpm': float(insight.get('cpm', 0)),
                'ctr': float(insight.get('ctr', 0)),
                'frequency': float(insight.get('frequency', 0)),
                'unique_ctr': float(insight.get('unique_ctr', 0)),
                'conversions': get_conversion_value(insight.get('conversions'), 'schedule_total'),
                'cost_per_conversion': get_cost_per_conversion_value(insight.get('cost_per_conversion'), 'schedule_total'),
                'unique_conversions': int(insight.get('unique_conversions', 0)),
            }
            rows_to_insert.append(row)

        # Insert data into BigQuery
        table_ref = client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.schema = schema
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        )

        job = client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete

        if job.errors:
            print(f"Errors occurred: {job.errors}")
        else:
            print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Fetch data for the last 365 days in 30-day chunks
end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
start_date = end_date - timedelta(days=365)

current_start = start_date
while current_start < end_date:
    current_end = min(current_start + timedelta(days=30), end_date)
    fetch_and_load_data(current_start, current_end)
    current_start = current_end + timedelta(days=1)
    time.sleep(3)  # Wait for 60 seconds between requests to avoid rate limiting

print("365-day backfill completed.")