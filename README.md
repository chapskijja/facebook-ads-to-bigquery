# Facebook Ads to BigQuery ETL Pipeline

An intelligent ETL pipeline that extracts Facebook Ads data and loads it into Google BigQuery with **incremental data loading** and **date range control**.

## ✨ Key Features

- **🔄 Incremental Loading**: Only fetches missing data, avoiding duplicates
- **📅 Smart Date Management**: Automatically detects existing dates in BigQuery
- **🔄 Last Day Rewrite**: Optionally rewrites recent data to ensure completeness
- **⚡ Chunked Processing**: Handles large date ranges efficiently
- **🛡️ Rate Limiting**: Respects Facebook API limits
- **📊 Status Reporting**: View data coverage and identify gaps
- **🖥️ CLI Interface**: Easy-to-use command-line tools

## 🏗️ Architecture

The pipeline consists of:
- `facebook_ads_to_bigquery.py`: Core ETL logic
- `config.py`: Configuration settings
- `run_etl.py`: Command-line interface
- `.env`: Environment variables (not tracked in git)

## 🚀 Quick Start

### Prerequisites

1. **Facebook App & Access Token**
2. **Google Cloud Project with BigQuery API enabled**
3. **Service Account JSON file**

### Installation

1. **Clone and setup environment:**
```bash
git clone <your-repo>
cd facebook-ads-to-bigquery
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. **Create `.env` file:**
```env
FACEBOOK_APP_ID=your_app_id
FACEBOOK_APP_SECRET=your_app_secret
FACEBOOK_ACCESS_TOKEN=your_access_token
FACEBOOK_AD_ACCOUNT_ID=act_your_account_id
GOOGLE_APPLICATION_CREDENTIALS=secret/your-service-account.json
```

3. **Place your Google Cloud service account JSON** in the `secret/` folder.

## 📖 Usage

### CLI Commands

#### 1. Daily Sync (Default)
```bash
python run_etl.py daily
# or simply
python run_etl.py
```
- Checks last 30 days for missing data
- Rewrites last 1 day to ensure completeness
- Perfect for daily cron jobs

#### 2. Historical Backfill
```bash
# Backfill last 365 days
python run_etl.py backfill

# Backfill custom number of days
python run_etl.py backfill --days 90
```
- Loads historical data
- Only fetches missing dates
- Processes in chunks to respect API limits

#### 3. Custom Date Range
```bash
# Fetch specific date range
python run_etl.py custom 2024-01-01 2024-01-31

# Force rewrite existing data
python run_etl.py custom 2024-01-01 2024-01-31 --force
```

#### 4. Data Status Report
```bash
python run_etl.py status
```
Shows:
- Date coverage
- Total records and spend
- Missing dates in last 30 days

### Direct Script Usage

You can also run the main script directly:
```bash
python facebook_ads_to_bigquery.py
```

## ⚙️ Configuration

Edit `config.py` to customize:

```python
class ETLConfig:
    # How many days back to check by default
    DEFAULT_LOOKBACK_DAYS = 30
    
    # Rewrite last N days to ensure complete data
    REWRITE_LAST_N_DAYS = 1
    
    # Maximum days per Facebook API request
    MAX_CHUNK_DAYS = 30
    
    # Seconds between API calls
    RATE_LIMIT_DELAY = 3
```

## 🗃️ BigQuery Schema

The pipeline creates a table with these fields:

| Field | Type | Description |
|-------|------|-------------|
| account_name | STRING | Facebook Ad Account Name |
| campaign | STRING | Campaign Name |
| adset_name | STRING | Ad Set Name |
| ad_name | STRING | Ad Name |
| date | DATE | Report Date |
| impressions | INTEGER | Number of Impressions |
| clicks | INTEGER | Number of Clicks |
| spend | FLOAT | Amount Spent |
| cpc | FLOAT | Cost Per Click |
| cpm | FLOAT | Cost Per Mille |
| ctr | FLOAT | Click Through Rate |
| frequency | FLOAT | Frequency |
| unique_ctr | FLOAT | Unique Click Through Rate |
| conversions | INTEGER | Total Conversions |
| cost_per_conversion | FLOAT | Cost Per Conversion |
| unique_conversions | INTEGER | Unique Conversions |

## 🔄 How Incremental Loading Works

1. **Check Existing Data**: Query BigQuery for existing dates
2. **Identify Gaps**: Compare requested range with existing dates
3. **Smart Rewrite**: Optionally rewrite recent data for completeness
4. **Chunked Fetching**: Process large ranges in smaller chunks
5. **Duplicate Prevention**: Delete existing data before inserting new

### Example Workflow

```
Requested: 2024-01-01 to 2024-01-31
Existing:  2024-01-01 to 2024-01-25
Missing:   2024-01-26 to 2024-01-31
Rewrite:   2024-01-25 (last day)
Action:    Fetch 2024-01-25 to 2024-01-31
```

## 🔧 Troubleshooting

### Common Issues

1. **"Table not found"**: First run creates the table automatically
2. **Facebook API rate limits**: Increase `RATE_LIMIT_DELAY` in config
3. **Large date ranges**: Use backfill mode for historical data
4. **Duplicate data**: The pipeline handles this automatically

### Monitoring

Use the status command to monitor your data:
```bash
python run_etl.py status
```

## 📅 Scheduling

For production use, schedule daily sync with cron:
```bash
# Daily at 6 AM
0 6 * * * cd /path/to/project && /path/to/venv/bin/python run_etl.py daily
```

## 🛡️ Security

- **Never commit `.env` files**
- **Keep service account JSON files secure**
- **Use IAM roles with minimal required permissions**
- **Rotate access tokens regularly**

## 📊 Sample Output

```
🚀 Starting Facebook Ads to BigQuery ETL with incremental loading
Table feettech_crm.facebook_ads already exists
Found 25 existing dates in BigQuery
Date range: 2024-01-01 to 2024-01-25

=== Date Range Analysis ===
Requested range: 2024-01-20 to 2024-01-31
Will rewrite last 1 days from 2024-01-25
Missing dates: 7 total
First missing: 2024-01-25
Last missing: 2024-01-31

📊 Processing range 1/1: 2024-01-25 to 2024-01-31
✅ Loaded 156 rows into feettech_crm:facebook_ads
🎉 ETL process completed successfully!
```

## 🤝 Contributing

1. Create feature branch from `dev`
2. Make changes
3. Test thoroughly
4. Submit pull request

## 📝 License

MIT License - see LICENSE file for details.
