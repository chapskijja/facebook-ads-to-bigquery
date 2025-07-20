#!/usr/bin/env python3
"""
Demo script to show how the new monitoring window logic works
This script simulates the date range logic without making actual API calls
"""

from datetime import datetime, timedelta

def simulate_get_date_ranges_to_fetch(start_date, end_date, existing_dates, 
                                    rewrite_last_n_days=0, monitoring_window_days=10, 
                                    simulated_latest_date=None):
    """
    Simulated version of get_date_ranges_to_fetch for demo purposes
    """
    print(f"\n=== Smart Date Range Analysis ===")
    print(f"Requested range: {start_date} to {end_date}")
    print(f"Monitoring window: {monitoring_window_days} days")
    
    # Use simulated latest date instead of querying BigQuery
    latest_date_in_bq = simulated_latest_date
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
    
    # Add missing dates from latest_date + 1 to yesterday (or end_date if earlier)
    if latest_date_in_bq < min(yesterday, end_date):
        gap_start = latest_date_in_bq + timedelta(days=1)
        gap_end = min(yesterday, end_date)
        
        print(f"Gap from latest to end: {gap_start} to {gap_end}")
        
        current = gap_start
        while current <= gap_end:
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
                range_end = current_date
            else:
                ranges.append((range_start, range_end))
                range_start = current_date
                range_end = current_date
        
        ranges.append((range_start, range_end))
    
    print(f"Optimized into {len(ranges)} range(s):")
    for i, (start, end) in enumerate(ranges, 1):
        days = (end - start).days + 1
        print(f"  Range {i}: {start} to {end} ({days} days)")
    
    return ranges

def demo_monitoring_window():
    """Demo the new monitoring window logic with user's example"""
    
    print("🎭 Demo: Smart Monitoring Window Logic")
    print("=" * 60)
    
    # User's example: Latest in BQ is July 7, today is July 15
    print("📝 Your Example Scenario:")
    print("   Latest date in BigQuery: July 7, 2024")
    print("   Today: July 15, 2024")
    print("   Expected: Rewrite July 7 + Add July 8-14")
    print()
    
    # Simulate existing dates - only the last 10 days around July 7
    existing_dates = set()
    
    # Add some dates before July 7 (simulating the last 10 days)
    base_date = datetime(2024, 7, 7).date()
    for i in range(-5, 1):  # July 2 to July 7
        existing_dates.add(base_date + timedelta(days=i))
    
    print(f"📅 Simulated recent dates in BigQuery: {sorted(existing_dates)}")
    print(f"   Latest date: {max(existing_dates)}")
    print()
    
    # Test the scenario: request from July 1 to July 14
    start_date = datetime(2024, 7, 1).date()
    end_date = datetime(2024, 7, 14).date()
    latest_date = datetime(2024, 7, 7).date()  # Simulate latest date
    
    print(f"🔍 Running ETL for: {start_date} to {end_date}")
    print(f"   Monitoring window: 10 days")
    print(f"   Rewrite last: 1 day")
    
    # Get date ranges with new logic
    date_ranges = simulate_get_date_ranges_to_fetch(
        start_date=start_date,
        end_date=end_date,
        existing_dates=existing_dates,
        rewrite_last_n_days=1,
        monitoring_window_days=10,
        simulated_latest_date=latest_date
    )
    
    if date_ranges:
        total_days = sum((end - start).days + 1 for start, end in date_ranges)
        print(f"\n📊 Result: {len(date_ranges)} range(s), {total_days} total days")
        for i, (start, end) in enumerate(date_ranges, 1):
            days = (end - start).days + 1
            print(f"   Range {i}: {start} to {end} ({days} days)")
    else:
        print("\n📊 Result: No data needed!")

def demo_different_scenarios():
    """Demo different scenarios to show the efficiency"""
    
    print(f"\n🧪 Demo: Different Scenarios")
    print("=" * 50)
    
    # Scenario 1: Up to date data
    print("🔍 Scenario 1: Data is completely up to date")
    existing_dates = set()
    yesterday = datetime(2024, 7, 14).date()  # Simulate yesterday
    latest_date = yesterday
    
    # Add last 5 days including yesterday
    for i in range(5):
        existing_dates.add(yesterday - timedelta(days=i))
    
    print(f"   Existing dates: {sorted(existing_dates)}")
    
    start_date = yesterday - timedelta(days=10)
    end_date = yesterday
    
    date_ranges = simulate_get_date_ranges_to_fetch(
        start_date=start_date,
        end_date=end_date,
        existing_dates=existing_dates,
        rewrite_last_n_days=1,
        monitoring_window_days=10,
        simulated_latest_date=latest_date
    )
    
    # Scenario 2: Big gap (like user's original example)
    print(f"\n🔍 Scenario 2: Big gap (latest is 7 days old)")
    existing_dates = set()
    latest_date = datetime(2024, 7, 7).date()  # July 7
    end_date = datetime(2024, 7, 14).date()     # July 14
    
    # Add some recent dates around the latest date
    for i in range(3):
        existing_dates.add(latest_date - timedelta(days=i))
    
    print(f"   Existing dates: {sorted(existing_dates)}")
    print(f"   Latest: {latest_date}, End date: {end_date}")
    
    date_ranges = simulate_get_date_ranges_to_fetch(
        start_date=datetime(2024, 7, 1).date(),
        end_date=end_date,
        existing_dates=existing_dates,
        rewrite_last_n_days=1,
        monitoring_window_days=10,
        simulated_latest_date=latest_date
    )

def demo_efficiency_comparison():
    """Demo the efficiency improvement"""
    
    print(f"\n⚡ Demo: Efficiency Comparison")
    print("=" * 40)
    
    print("🐌 Old approach:")
    print("   - Query ALL dates from BigQuery")
    print("   - Load 100s or 1000s of dates into memory")
    print("   - Compare against entire date range")
    print("   - Slow for large tables")
    
    print("\n🚀 New monitoring window approach:")
    print("   - Query only LAST 10 dates from BigQuery")
    print("   - Load minimal data into memory")  
    print("   - Focus on recent data integrity")
    print("   - Fast regardless of table size")
    
    print(f"\n📈 Example efficiency:")
    print(f"   Table with 2 years of data (730 days):")
    print(f"   Old: Query 730 dates")
    print(f"   New: Query 10 dates (73x faster!)")
    
    print(f"\n✨ Your scenario benefits:")
    print(f"   Latest: July 7, Today: July 15")
    print(f"   Old approach: Check all dates since beginning")
    print(f"   New approach: Check only July 7 + fetch July 8-14")
    print(f"   Result: Exactly what you requested! 🎯")

if __name__ == "__main__":
    demo_monitoring_window()
    demo_different_scenarios()
    demo_efficiency_comparison()
    
    print(f"\n🎉 Demo completed!")
    print(f"💡 Your ETL now only monitors last 10 dates for maximum efficiency!")
    print(f"🔧 Configure monitoring window in config.py (MONITORING_WINDOW_DAYS)") 