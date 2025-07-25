# Configuration file for Facebook Ads to BigQuery ETL

from datetime import datetime, timedelta

class ETLConfig:
    """Configuration settings for the ETL process"""
    
    # BigQuery settings
    DATASET_ID = 'feettech_crm'
    TABLE_ID = 'facebook_ads'
    
    # Date range settings
    DEFAULT_LOOKBACK_DAYS = 30  # How many days back to check by default
    REWRITE_LAST_N_DAYS = 1     # Rewrite last N days to ensure complete data
    MONITORING_WINDOW_DAYS = 10 # Only monitor last N dates in BigQuery for efficiency
    
    # API and processing settings
    MAX_CHUNK_DAYS = 7         # Maximum days per Facebook API request
    RATE_LIMIT_DELAY = 30       # Seconds between API calls
    
    # Filtering settings
    MIN_SPEND_THRESHOLD = 0.01  # Minimum spend ($0.01) to include campaign/ad in results
    
    # Facebook API settings
    API_VERSION = 'v17.0'
    
    @staticmethod
    def get_default_date_range():
        """Get default date range for processing"""
        end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=ETLConfig.DEFAULT_LOOKBACK_DAYS)
        return start_date, end_date
    
    @staticmethod
    def get_backfill_date_range(days_back=365):
        """Get date range for historical backfill"""
        end_date = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=days_back)
        return start_date, end_date 
