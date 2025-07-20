#!/usr/bin/env python3
"""
Command-line interface for Facebook Ads to BigQuery ETL
"""

import argparse
import sys
from datetime import datetime, timedelta
from config import ETLConfig

# Import the ETL functions
from facebook_ads_to_bigquery import (
    create_table_if_not_exists,
    get_existing_dates,
    get_date_ranges_to_fetch,
    fetch_and_load_data,
    client,
    account
)

def run_daily_sync():
    """Run daily sync - check for missing data in recent days"""
    print("🔄 Running daily sync...")
    
    # Create table if needed
    create_table_if_not_exists()
    
    # Get existing dates (only last N days for efficiency)
    existing_dates = get_existing_dates(ETLConfig.MONITORING_WINDOW_DAYS)
    
    # Use default date range
    start_date, end_date = ETLConfig.get_default_date_range()
    
    print(f"Checking date range: {start_date} to {end_date}")
    
    # Get missing date ranges
    date_ranges = get_date_ranges_to_fetch(
        start_date=start_date,
        end_date=end_date,
        existing_dates=existing_dates,
        rewrite_last_n_days=ETLConfig.REWRITE_LAST_N_DAYS,
        monitoring_window_days=ETLConfig.MONITORING_WINDOW_DAYS
    )
    
    if not date_ranges:
        print("✅ All data is up to date!")
        return
    
    # Process ranges
    for range_start, range_end in date_ranges:
        fetch_and_load_data(range_start, range_end)
    
    print("✅ Daily sync completed!")

def run_backfill(days_back=365):
    """Run historical backfill"""
    print(f"⏳ Running backfill for last {days_back} days...")
    
    # Create table if needed
    create_table_if_not_exists()
    
    # Get existing dates (only last N days for efficiency)
    existing_dates = get_existing_dates(ETLConfig.MONITORING_WINDOW_DAYS)
    
    # Get backfill date range
    start_date, end_date = ETLConfig.get_backfill_date_range(days_back)
    
    print(f"Backfill date range: {start_date} to {end_date}")
    
    # Get missing date ranges (no rewrite for backfill)
    date_ranges = get_date_ranges_to_fetch(
        start_date=start_date,
        end_date=end_date,
        existing_dates=existing_dates,
        rewrite_last_n_days=0,  # No rewrite for backfill
        monitoring_window_days=ETLConfig.MONITORING_WINDOW_DAYS
    )
    
    if not date_ranges:
        print("✅ All historical data is already loaded!")
        return
    
    # Process ranges with chunking
    import time
    total_ranges = len(date_ranges)
    
    for i, (range_start, range_end) in enumerate(date_ranges, 1):
        print(f"\n📊 Processing range {i}/{total_ranges}: {range_start} to {range_end}")
        
        # Split large ranges into chunks
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
    
    print("✅ Backfill completed!")

def run_custom_range(start_date_str, end_date_str, force_rewrite=False):
    """Run ETL for custom date range"""
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        print("❌ Invalid date format. Use YYYY-MM-DD")
        return
    
    if start_date > end_date:
        print("❌ Start date must be before end date")
        return
    
    print(f"📅 Running ETL for custom range: {start_date} to {end_date}")
    
    # Create table if needed
    create_table_if_not_exists()
    
    if force_rewrite:
        print("🔥 Force rewrite enabled - will overwrite existing data")
        # Split into chunks and process
        current_start = start_date
        while current_start <= end_date:
            chunk_end = min(current_start + timedelta(days=ETLConfig.MAX_CHUNK_DAYS - 1), end_date)
            fetch_and_load_data(current_start, chunk_end, delete_existing=True)
            current_start = chunk_end + timedelta(days=1)
    else:
        # Get existing dates and find missing ranges
        existing_dates = get_existing_dates(ETLConfig.MONITORING_WINDOW_DAYS)
        
        date_ranges = get_date_ranges_to_fetch(
            start_date=start_date,
            end_date=end_date,
            existing_dates=existing_dates,
            rewrite_last_n_days=0,
            monitoring_window_days=ETLConfig.MONITORING_WINDOW_DAYS
        )
        
        if not date_ranges:
            print("✅ All data for this range already exists!")
            return
        
        # Process missing ranges
        for range_start, range_end in date_ranges:
            # Split into chunks
            current_start = range_start
            while current_start <= range_end:
                chunk_end = min(current_start + timedelta(days=ETLConfig.MAX_CHUNK_DAYS - 1), range_end)
                fetch_and_load_data(current_start, chunk_end)
                current_start = chunk_end + timedelta(days=1)
    
    print("✅ Custom range ETL completed!")

def show_status():
    """Show current status of data in BigQuery"""
    print("📊 Data Status Report")
    print("=" * 50)
    
    try:
        # Get basic table info
        query = f"""
        SELECT 
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(DISTINCT date) as total_days,
            COUNT(*) as total_rows,
            SUM(spend) as total_spend,
            SUM(impressions) as total_impressions,
            SUM(clicks) as total_clicks
        FROM `{ETLConfig.DATASET_ID}.{ETLConfig.TABLE_ID}`
        """
        
        results = client.query(query)
        for row in results:
            print(f"Date Range: {row.earliest_date} to {row.latest_date}")
            print(f"Total Days: {row.total_days}")
            print(f"Total Rows: {row.total_rows:,}")
            print(f"Total Spend: ${row.total_spend:,.2f}")
            print(f"Total Impressions: {row.total_impressions:,}")
            print(f"Total Clicks: {row.total_clicks:,}")
        
        # Check for missing dates in recent monitoring window
        end_date = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=ETLConfig.MONITORING_WINDOW_DAYS)
        
        existing_dates = get_existing_dates(ETLConfig.MONITORING_WINDOW_DAYS)
        date_ranges = get_date_ranges_to_fetch(
            start_date, 
            end_date, 
            existing_dates, 
            rewrite_last_n_days=0,
            monitoring_window_days=ETLConfig.MONITORING_WINDOW_DAYS
        )
        
        if date_ranges:
            print(f"\n⚠️  Missing dates in last {ETLConfig.MONITORING_WINDOW_DAYS} days:")
            for range_start, range_end in date_ranges:
                if range_start == range_end:
                    print(f"  - {range_start}")
                else:
                    print(f"  - {range_start} to {range_end}")
        else:
            print(f"\n✅ No missing dates in last {ETLConfig.MONITORING_WINDOW_DAYS} days")
            
    except Exception as e:
        print(f"❌ Error getting status: {e}")

def main():
    parser = argparse.ArgumentParser(description='Facebook Ads to BigQuery ETL')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Daily sync command
    daily_parser = subparsers.add_parser('daily', help='Run daily sync (default)')
    
    # Backfill command
    backfill_parser = subparsers.add_parser('backfill', help='Run historical backfill')
    backfill_parser.add_argument('--days', type=int, default=365, 
                                help='Number of days to backfill (default: 365)')
    
    # Custom range command
    custom_parser = subparsers.add_parser('custom', help='Run ETL for custom date range')
    custom_parser.add_argument('start_date', help='Start date (YYYY-MM-DD)')
    custom_parser.add_argument('end_date', help='End date (YYYY-MM-DD)')
    custom_parser.add_argument('--force', action='store_true', 
                              help='Force rewrite existing data')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show data status')
    
    args = parser.parse_args()
    
    if args.command == 'daily' or args.command is None:
        run_daily_sync()
    elif args.command == 'backfill':
        run_backfill(args.days)
    elif args.command == 'custom':
        run_custom_range(args.start_date, args.end_date, args.force)
    elif args.command == 'status':
        show_status()
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 