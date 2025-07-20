#!/usr/bin/env python3
"""
Demo script to show how incremental loading works
This script simulates the date range logic without making actual API calls
"""

from datetime import datetime, timedelta
from facebook_ads_to_bigquery import get_date_ranges_to_fetch

def demo_incremental_loading():
    """Demo the incremental loading logic"""
    
    print("🎭 Demo: Facebook Ads ETL Incremental Loading")
    print("=" * 60)
    
    # Simulate existing dates in BigQuery (some dates are missing)
    existing_dates = set()
    
    # Add dates from Jan 1-15, 2024
    for i in range(15):
        existing_dates.add(datetime(2024, 1, 1).date() + timedelta(days=i))
    
    # Add dates from Jan 20-25, 2024 (missing 16-19)
    for i in range(20, 26):
        existing_dates.add(datetime(2024, 1, 1).date() + timedelta(days=i-1))
    
    # Add recent dates (missing last 2 days)
    for i in range(27, 29):
        existing_dates.add(datetime(2024, 1, 1).date() + timedelta(days=i-1))
    
    print(f"📅 Simulated existing dates in BigQuery: {len(existing_dates)} days")
    print(f"   Range: {min(existing_dates)} to {max(existing_dates)}")
    
    # Show some missing dates
    missing_in_jan = []
    for i in range(31):
        check_date = datetime(2024, 1, 1).date() + timedelta(days=i)
        if check_date not in existing_dates:
            missing_in_jan.append(check_date)
    
    print(f"   Missing dates in January: {missing_in_jan}")
    
    # Test different scenarios
    scenarios = [
        {
            "name": "Daily Sync (last 7 days)",
            "start": datetime(2024, 1, 25).date(),
            "end": datetime(2024, 1, 31).date(),
            "rewrite": 1
        },
        {
            "name": "Backfill (whole month)",
            "start": datetime(2024, 1, 1).date(),
            "end": datetime(2024, 1, 31).date(),
            "rewrite": 0
        },
        {
            "name": "Custom range with rewrite",
            "start": datetime(2024, 1, 20).date(),
            "end": datetime(2024, 1, 28).date(),
            "rewrite": 2
        }
    ]
    
    for scenario in scenarios:
        print(f"\n🔍 Scenario: {scenario['name']}")
        print(f"   Requested: {scenario['start']} to {scenario['end']}")
        print(f"   Rewrite last {scenario['rewrite']} days: {'Yes' if scenario['rewrite'] > 0 else 'No'}")
        
        # Get date ranges to fetch
        date_ranges = get_date_ranges_to_fetch(
            start_date=scenario['start'],
            end_date=scenario['end'],
            existing_dates=existing_dates,
            rewrite_last_n_days=scenario['rewrite']
        )
        
        if not date_ranges:
            print("   📊 Result: No data to fetch (all up to date)")
        else:
            total_days = sum((end - start).days + 1 for start, end in date_ranges)
            print(f"   📊 Result: {len(date_ranges)} date range(s), {total_days} total days to fetch")
            for i, (start, end) in enumerate(date_ranges, 1):
                days = (end - start).days + 1
                print(f"      Range {i}: {start} to {end} ({days} days)")

def demo_api_chunking():
    """Demo how large date ranges get chunked for API calls"""
    
    print(f"\n🧩 Demo: API Chunking Logic")
    print("=" * 40)
    
    # Simulate a large date range that needs chunking
    start_date = datetime(2024, 1, 1).date()
    end_date = datetime(2024, 3, 31).date()  # 90 days
    
    MAX_CHUNK_DAYS = 30  # From config
    
    print(f"📅 Large date range: {start_date} to {end_date}")
    total_days = (end_date - start_date).days + 1
    print(f"   Total days: {total_days}")
    print(f"   Max chunk size: {MAX_CHUNK_DAYS} days")
    
    # Show how it would be chunked
    chunks = []
    current_start = start_date
    chunk_num = 1
    
    print(f"\n📦 API call chunks:")
    while current_start <= end_date:
        chunk_end = min(current_start + timedelta(days=MAX_CHUNK_DAYS - 1), end_date)
        chunk_days = (chunk_end - current_start).days + 1
        chunks.append((current_start, chunk_end))
        
        print(f"   Chunk {chunk_num}: {current_start} to {chunk_end} ({chunk_days} days)")
        
        current_start = chunk_end + timedelta(days=1)
        chunk_num += 1
    
    print(f"\n✅ Total API calls needed: {len(chunks)}")

if __name__ == "__main__":
    demo_incremental_loading()
    demo_api_chunking()
    
    print(f"\n🎉 Demo completed!")
    print(f"💡 Run 'python run_etl.py --help' to see actual CLI commands") 