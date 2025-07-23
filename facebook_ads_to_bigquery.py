import signal
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from google.cloud import bigquery
import os
import dotenv 
from datetime import datetime, timedelta
import time
from typing import List, Set
from config import ETLConfig

# Load environment variables from .env file
dotenv.load_dotenv()

# Add timeout handler for API requests
class APITimeoutError(Exception):
    pass

def timeout_handler(signum, frame):
    raise APITimeoutError("API request timed out")

# Facebook API setup
app_id = os.getenv('FACEBOOK_APP_ID')
app_secret = os.getenv('FACEBOOK_APP_SECRET')
access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
ad_account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')

FacebookAdsApi.init(app_id, app_secret, access_token, api_version=ETLConfig.API_VERSION)
account = AdAccount(ad_account_id)

# BigQuery setup
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
client = bigquery.Client()
dataset_id = ETLConfig.DATASET_ID
table_id = ETLConfig.TABLE_ID

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
    bigquery.SchemaField("leads", "INTEGER"),
    bigquery.SchemaField("cost_per_lead", "FLOAT"),
    bigquery.SchemaField("landing_page_views", "INTEGER"),
    bigquery.SchemaField("cost_per_landing_page_view", "FLOAT"),
    bigquery.SchemaField("add_to_cart", "INTEGER"),
    bigquery.SchemaField("cost_per_add_to_cart", "FLOAT"),
    bigquery.SchemaField('initiate_checkout', 'INTEGER'),
    bigquery.SchemaField('cost_per_initiate_checkout', 'FLOAT'),
    bigquery.SchemaField('add_payment_info', 'INTEGER'),
    bigquery.SchemaField('cost_per_add_payment_info', 'FLOAT'),
    bigquery.SchemaField('subscribe', 'INTEGER'),
    bigquery.SchemaField('cost_per_subscribe', 'FLOAT'),
    bigquery.SchemaField('complete_registration', 'INTEGER'),
    bigquery.SchemaField('cost_per_complete_registration', 'FLOAT'),
    bigquery.SchemaField('purchase', 'INTEGER'),
    bigquery.SchemaField('cost_per_purchase', 'FLOAT'),
]

# Priority actions to extract from Facebook API
action_priority = [
    'lead', 'landing_page_view', 'add_to_cart', 'initiate_checkout',
    'add_payment_info', 'subscribe', 'complete_registration', 'purchase'
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
    AdsInsights.Field.actions,
    AdsInsights.Field.action_values,
]

def create_table_if_not_exists():
    """Create BigQuery table if it doesn't exist"""
    table_ref = client.dataset(dataset_id).table(table_id)
    
    try:
        client.get_table(table_ref)
        print(f"Table {dataset_id}.{table_id} already exists")
    except Exception:
        print(f"Creating table {dataset_id}.{table_id}")
        table = bigquery.Table(table_ref, schema=schema)
        
        # Configure time partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        )
        
        client.create_table(table)
        print(f"Created table {dataset_id}.{table_id}")

def get_existing_dates(last_n_days=10) -> Set[datetime.date]:
    """Get the last N dates from BigQuery table for efficiency"""
    query = f"""
    SELECT DISTINCT date
    FROM `{dataset_id}.{table_id}`
    ORDER BY date DESC
    LIMIT {last_n_days}
    """
    
    try:
        results = client.query(query)
        existing_dates = {row.date for row in results}
        print(f"Found {len(existing_dates)} recent dates in BigQuery (last {last_n_days} days)")
        if existing_dates:
            min_date = min(existing_dates)
            max_date = max(existing_dates)
            print(f"Recent date range: {min_date} to {max_date}")
        return existing_dates
    except Exception as e:
        print(f"Error querying existing dates (table might not exist): {e}")
        return set()

def get_existing_dates_in_range(start_date: datetime.date, end_date: datetime.date) -> Set[datetime.date]:
    """Get all existing dates in a specific date range"""
    query = f"""
    SELECT DISTINCT date
    FROM `{dataset_id}.{table_id}`
    WHERE date >= '{start_date}' AND date <= '{end_date}'
    ORDER BY date
    """
    
    try:
        results = client.query(query)
        existing_dates = {row.date for row in results}
        total_possible_days = (end_date - start_date).days + 1
        print(f"Found {len(existing_dates)} existing dates in range {start_date} to {end_date}")
        print(f"Coverage: {len(existing_dates)}/{total_possible_days} days ({len(existing_dates)/total_possible_days*100:.1f}%)")
        
        if existing_dates:
            min_date = min(existing_dates)
            max_date = max(existing_dates)
            print(f"Existing data range: {min_date} to {max_date}")
        else:
            print("No existing data found in this range")
            
        return existing_dates
    except Exception as e:
        print(f"Error querying existing dates in range (table might not exist): {e}")
        return set()

def get_latest_date_in_bq() -> datetime.date:
    """Get the most recent date in BigQuery table"""
    query = f"""
    SELECT MAX(date) as max_date
    FROM `{dataset_id}.{table_id}`
    """
    
    try:
        results = client.query(query)
        for row in results:
            if row.max_date:
                return row.max_date
        return None
    except Exception as e:
        print(f"Error getting latest date: {e}")
        return None

def get_missing_date_ranges_for_backfill(start_date: datetime.date, end_date: datetime.date, 
                                       existing_dates: Set[datetime.date]) -> List[tuple]:
    """
    Simple backfill logic: find ALL missing dates in the requested range
    """
    print(f"\n=== Backfill Date Range Analysis ===")
    print(f"Requested range: {start_date} to {end_date}")
    
    # Generate all dates in the range
    all_dates = set()
    current = start_date
    while current <= end_date:
        all_dates.add(current)
        current += timedelta(days=1)
    
    # Find missing dates
    missing_dates = all_dates - existing_dates
    
    if not missing_dates:
        print("✅ No missing dates found!")
        return []
    
    # Convert to sorted list
    missing_dates_sorted = sorted(missing_dates)
    print(f"Found {len(missing_dates)} missing dates out of {len(all_dates)} total")
    print(f"Missing date range: {missing_dates_sorted[0]} to {missing_dates_sorted[-1]}")
    
    # Group consecutive dates into ranges
    ranges = []
    if missing_dates_sorted:
        range_start = missing_dates_sorted[0]
        range_end = missing_dates_sorted[0]
        
        for i in range(1, len(missing_dates_sorted)):
            current_date = missing_dates_sorted[i]
            if current_date == range_end + timedelta(days=1):
                # Consecutive date, extend current range
                range_end = current_date
            else:
                # Gap found, save current range and start new one
                ranges.append((range_start, range_end))
                range_start = current_date
                range_end = current_date
        
        # Add the last range
        ranges.append((range_start, range_end))
    
    print(f"Grouped into {len(ranges)} consecutive range(s):")
    for i, (start, end) in enumerate(ranges, 1):
        days = (end - start).days + 1
        print(f"  Range {i}: {start} to {end} ({days} days)")
    
    return ranges

def get_date_ranges_to_fetch(start_date: datetime.date, end_date: datetime.date, 
                           existing_dates: Set[datetime.date], 
                           rewrite_last_n_days: int = 0,
                           monitoring_window_days: int = 10) -> List[tuple]:
    """
    Smart date range fetching focused on recent data integrity
    
    Logic:
    1. Find latest date in BigQuery  
    2. Focus on monitoring window (last N days from latest date)
    3. Rewrite latest date + fill any gaps in monitoring window
    4. If latest date is old, fill gap from latest to yesterday
    """
    print(f"\n=== Smart Date Range Analysis ===")
    print(f"Requested range: {start_date} to {end_date}")
    print(f"Monitoring window: {monitoring_window_days} days")
    
    # Get the latest date from BigQuery
    latest_date_in_bq = get_latest_date_in_bq()
    yesterday = datetime.now().date() - timedelta(days=1)
    
    if not latest_date_in_bq:
        print("No existing data found - will fetch entire requested range")
        return [(start_date, end_date)]
    
    # Calculate monitoring window
    monitoring_start = latest_date_in_bq - timedelta(days=monitoring_window_days - 1)
    
    print(f"Latest date in BQ: {latest_date_in_bq}")
    print(f"Yesterday: {yesterday}")
    print(f"Monitoring window: {monitoring_start} to {latest_date_in_bq}")
    
    # Determine what we need to fetch
    dates_to_fetch = set()
    
    # Always rewrite the latest date for completeness
    if rewrite_last_n_days > 0 and latest_date_in_bq:
        rewrite_from = max(
            latest_date_in_bq - timedelta(days=rewrite_last_n_days - 1),
            start_date
        )
        rewrite_to = min(latest_date_in_bq, end_date)
        
        print(f"Will rewrite last {rewrite_last_n_days} day(s): {rewrite_from} to {rewrite_to}")
        
        current = rewrite_from
        while current <= rewrite_to:
            dates_to_fetch.add(current)
            current += timedelta(days=1)
    
    # Check for gaps in monitoring window
    current = max(monitoring_start, start_date)
    monitoring_end = min(latest_date_in_bq, end_date)
    
    gaps_in_monitoring = []
    while current <= monitoring_end:
        if current not in existing_dates:
            gaps_in_monitoring.append(current)
            dates_to_fetch.add(current)
        current += timedelta(days=1)
    
    if gaps_in_monitoring:
        print(f"Found {len(gaps_in_monitoring)} gaps in monitoring window: {gaps_in_monitoring}")
    
    # Add missing dates from latest_date + 1 to yesterday
    if latest_date_in_bq < yesterday:
        gap_start = latest_date_in_bq + timedelta(days=1)
        gap_end = min(yesterday, end_date)
        
        print(f"Gap from latest to yesterday: {gap_start} to {gap_end}")
        
        current = gap_start
        while current <= gap_end:
            dates_to_fetch.add(current)
            current += timedelta(days=1)
    
    # Also include any requested dates beyond our latest date
    if end_date > latest_date_in_bq:
        future_start = max(latest_date_in_bq + timedelta(days=1), start_date)
        future_end = end_date
        
        current = future_start
        while current <= future_end:
            dates_to_fetch.add(current)
            current += timedelta(days=1)
    
    if not dates_to_fetch:
        print("No missing dates found!")
        return []
    
    # Convert to sorted list and group into consecutive ranges
    dates_sorted = sorted(dates_to_fetch)
    print(f"Total dates to fetch: {len(dates_sorted)}")
    print(f"Date range: {dates_sorted[0]} to {dates_sorted[-1]}")
    
    # Group consecutive dates into ranges
    ranges = []
    if dates_sorted:
        range_start = dates_sorted[0]
        range_end = dates_sorted[0]
        
        for i in range(1, len(dates_sorted)):
            current_date = dates_sorted[i]
            if current_date == range_end + timedelta(days=1):
                # Consecutive date, extend current range
                range_end = current_date
            else:
                # Gap found, save current range and start new one
                ranges.append((range_start, range_end))
                range_start = current_date
                range_end = current_date
        
        # Add the last range
        ranges.append((range_start, range_end))
    
    print(f"Optimized into {len(ranges)} range(s):")
    for i, (start, end) in enumerate(ranges, 1):
        days = (end - start).days + 1
        print(f"  Range {i}: {start} to {end} ({days} days)")
    
    return ranges

def delete_existing_data_for_date_range(start_date: datetime.date, end_date: datetime.date):
    """Delete existing data for a date range to avoid duplicates"""
    
    # First check if there's any data in the date range
    check_query = f"""
    SELECT COUNT(*) as row_count
    FROM `{dataset_id}.{table_id}`
    WHERE date >= '{start_date}' AND date <= '{end_date}'
    """
    
    try:
        # Check if there are any rows to delete
        check_job = client.query(check_query)
        result = check_job.result()
        row_count = next(iter(result)).row_count
        
        if row_count == 0:
            print(f"No existing data found for {start_date} to {end_date}, skipping deletion")
            return
        
        print(f"Found {row_count} existing rows for {start_date} to {end_date}, deleting...")
        
        # Delete existing data
        delete_query = f"""
        DELETE FROM `{dataset_id}.{table_id}`
        WHERE date >= '{start_date}' AND date <= '{end_date}'
        """
        
        job = client.query(delete_query)
        job.result()
        print(f"Deleted existing data for {start_date} to {end_date}")
        
    except Exception as e:
        print(f"Error checking/deleting existing data: {e}")

def get_action_value(actions_data, action_type):
    """
    Extract action value for specific action type from actions list
    """
    if not actions_data or not isinstance(actions_data, list):
        return 0
    
    for action in actions_data:
        if isinstance(action, dict) and action.get('action_type') == action_type:
            try:
                return int(action.get('value', 0))
            except (ValueError, TypeError):
                return 0
    return 0

def get_action_cost_value(action_values_data, action_type):
    """
    Extract cost per action value for specific action type
    """
    if not action_values_data or not isinstance(action_values_data, list):
        return 0.0
    
    for cost in action_values_data:
        if isinstance(cost, dict) and cost.get('action_type') == action_type:
            try:
                return float(cost.get('value', 0))
            except (ValueError, TypeError):
                return 0.0
    return 0.0

def fetch_and_load_data(start_date: datetime.date, end_date: datetime.date, delete_existing: bool = True):
    """Fetch data from Facebook API and load to BigQuery"""
    
    # Check request size - if more than 30 days, split into chunks
    days_diff = (end_date - start_date).days + 1
    if days_diff > 30:
        print(f"⚠️  Request for {days_diff} days is too large, splitting into chunks...")
        
        current_start = start_date
        while current_start <= end_date:
            chunk_end = min(current_start + timedelta(days=29), end_date)
            print(f"📅 Processing chunk: {current_start} to {chunk_end}")
            fetch_and_load_data(current_start, chunk_end, delete_existing)
            current_start = chunk_end + timedelta(days=1)
            
            # Pause between requests
            if current_start <= end_date:
                print(f"⏳ Waiting {ETLConfig.RATE_LIMIT_DELAY} seconds between requests...")
                time.sleep(ETLConfig.RATE_LIMIT_DELAY)
        return
    
    params = {
        'time_range': {'since': start_date.strftime('%Y-%m-%d'), 'until': end_date.strftime('%Y-%m-%d')},
        'level': 'ad',
        'time_increment': 1,  # Daily breakdown
    }

    try:
        print(f"\n--- Fetching data ---")
        print(f"Date range: {start_date} to {end_date}")
        print(f"Ad account: {ad_account_id}")
        print(f"Days in request: {days_diff}")
        
        # Delete existing data for this range to avoid duplicates
        if delete_existing:
            delete_existing_data_for_date_range(start_date, end_date)
        
        # Set 2 minute timeout for API request
        print("🔄 Sending request to Facebook API...")
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(120)  # 2 minute timeout
        
        start_time = time.time()
        try:
            insights = account.get_insights(fields=fields, params=params)
            api_time = time.time() - start_time
            print(f"✅ API responded in {api_time:.2f} seconds")
        except APITimeoutError:
            print("⏰ API request exceeded 2 minute timeout!")
            print("💡 Try reducing the period or check connection")
            return
        finally:
            signal.alarm(0)  # Disable timeout

        # Transform data
        rows_to_insert = []
        total_rows_processed = 0
        filtered_out_count = 0
        
        for insight in insights:
            total_rows_processed += 1
            # Get actions and action values data
            actions = insight.get('actions', [])
            action_values = insight.get('action_values', [])
            
            # Base row data
            spend = float(insight.get('spend', 0))
            
            # Filter out rows with spend below threshold
            if spend < ETLConfig.MIN_SPEND_THRESHOLD:
                filtered_out_count += 1
                continue
            
            row = {
                'account_name': insight.get('account_name'),
                'campaign': insight.get('campaign_name'),
                'adset_name': insight.get('adset_name'),
                'ad_name': insight.get('ad_name'),
                'date': insight.get('date_start'),
                'impressions': int(insight.get('impressions', 0)),
                'clicks': int(insight.get('clicks', 0)),
                'spend': spend,
                'cpc': float(insight.get('cpc', 0)),
                'cpm': float(insight.get('cpm', 0)),
                'ctr': float(insight.get('ctr', 0)),
                'frequency': float(insight.get('frequency', 0)),
                'unique_ctr': float(insight.get('unique_ctr', 0)),
            }
            
            # Add conversion/action data for each priority action
            for action_type in action_priority:
                # Get action count
                action_count = get_action_value(actions, action_type)
                
                # Map action types to schema field names
                if action_type == 'landing_page_view':
                    count_field = 'landing_page_views'
                    cost_field = 'cost_per_landing_page_view'
                elif action_type == 'lead':
                    count_field = 'leads'
                    cost_field = 'cost_per_lead'
                else:
                    count_field = action_type
                    cost_field = f'cost_per_{action_type}'
                
                row[count_field] = action_count
                
                # Get cost per action (if action count > 0)
                if action_count > 0:
                    cost_per_action = get_action_cost_value(action_values, action_type)
                    if cost_per_action == 0.0 and row['spend'] > 0:
                        # Fallback: calculate cost per action from spend
                        cost_per_action = row['spend'] / action_count
                    row[cost_field] = cost_per_action
                else:
                    row[cost_field] = 0.0
            
            rows_to_insert.append(row)

        # Show filtering statistics
        print(f"📊 Data filtering results:")
        print(f"   Total rows from API: {total_rows_processed}")
        print(f"   Filtered out (spend < ${ETLConfig.MIN_SPEND_THRESHOLD}): {filtered_out_count}")
        print(f"   Rows to insert: {len(rows_to_insert)}")
        
        if not rows_to_insert:
            print("⚠️  No active campaigns/ads found (all below spend threshold)")
            return

        # Insert data into BigQuery
        table_ref = client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.schema = schema
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        job = client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete

        if job.errors:
            print(f"Errors occurred: {job.errors}")
        else:
            print(f"✅ Loaded {job.output_rows} rows into {dataset_id}:{table_id}")

    except Exception as e:
        print(f"❌ An error occurred: {str(e)}")

def main():
    """Main execution function"""
    print("🚀 Starting Facebook Ads to BigQuery ETL with incremental loading")
    
    # Create table if it doesn't exist
    create_table_if_not_exists()
    
    # Get existing dates from BigQuery
    existing_dates = get_existing_dates()
    
    # Define date range (you can modify this as needed)
    start_date, end_date = ETLConfig.get_default_date_range()
    
    # Get date ranges to fetch
    date_ranges = get_date_ranges_to_fetch(
        start_date=start_date,
        end_date=end_date,
        existing_dates=existing_dates,
        rewrite_last_n_days=ETLConfig.REWRITE_LAST_N_DAYS,
        monitoring_window_days=ETLConfig.MONITORING_WINDOW_DAYS
    )
    
    if not date_ranges:
        print("🎉 All data is up to date!")
        return
    
    # Process each date range
    total_ranges = len(date_ranges)
    for i, (range_start, range_end) in enumerate(date_ranges, 1):
        print(f"\n📊 Processing range {i}/{total_ranges}")
        
        # Split large ranges into smaller chunks for API limits
        current_start = range_start
        while current_start <= range_end:
            chunk_end = min(current_start + timedelta(days=ETLConfig.MAX_CHUNK_DAYS - 1), range_end)
            
            fetch_and_load_data(current_start, chunk_end)
            
            # Move to next chunk
            current_start = chunk_end + timedelta(days=1)
            
            # Rate limiting
            if current_start <= range_end:
                print(f"⏳ Waiting {ETLConfig.RATE_LIMIT_DELAY} seconds...")
                time.sleep(ETLConfig.RATE_LIMIT_DELAY)
    
    print("🎉 ETL process completed successfully!")

if __name__ == "__main__":
    main()