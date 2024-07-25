# Facebook Ads to BigQuery Data Connector

This project provides a robust data connector that fetches Facebook Ads data and loads it into Google BigQuery. It automates the process of retrieving detailed ad performance metrics and storing them in a structured, queryable format for further analysis.

## Features

- Fetches Facebook Ads data using the Facebook Business API
- Loads data into a partitioned BigQuery table
- Supports backfilling up to 365 days of historical data
- Handles API rate limiting with built-in delays
- Partitions BigQuery table by date for efficient querying

## Prerequisites

- Facebook Business Manager account with access to the desired Ad Account
- Google Cloud Platform account with BigQuery enabled
- Python 3.7+

## Setup

1. **Create a BigQuery table using the following schema:**

   ```sql
   CREATE TABLE `your_project.marketing_data.facebook_ads`
   (
     account_name STRING,
     campaign STRING,
     adset_name STRING,
     ad_name STRING,
     date DATE,
     impressions INT64,
     clicks INT64,
     spend FLOAT64,
     cpc FLOAT64,
     cpm FLOAT64,
     ctr FLOAT64,
     frequency FLOAT64,
     unique_ctr FLOAT64,
     conversions INT64,
     cost_per_conversion FLOAT64,
     unique_conversions INT64
   )
   PARTITION BY date;
   ```

2. **Set up Facebook API credentials:**

   - Create a Facebook App in the [Facebook Developers console](https://developers.facebook.com/)
   - Obtain the App ID, App Secret, and generate an Access Token with `ads_read` permission
   - Note your Ad Account ID

3. **Set up Google Cloud credentials:**

   - Create a service account in your [Google Cloud Console](https://console.cloud.google.com/)
   - Download the JSON key file and save it as `etl-pipeline-430312.json` in the project directory

4. **Install required Python packages:**

   ```bash
   pip install facebook_business google-cloud-bigquery
   ```

5. **Update the script with your Facebook API credentials and BigQuery project details.**

## Usage

Run the script to start the data transfer:

```bash
python facebook_ads_to_bigquery.py
```

The script will fetch data for the last 365 days in 30-day chunks and load it into the specified BigQuery table.

## Customization

You can modify the date range, fields to fetch, or add additional transformations by editing the Python script.

## Security Note

Be cautious with your API credentials. Never commit them directly to your repository. Consider using environment variables or a secure secrets management system in a production environment.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
