# 🐳 Docker Setup for Facebook Ads to BigQuery ETL

## 📦 What's included

- **Dockerfile** - image with Python and all dependencies
- **docker-compose.yml** - convenient startup with configurations
- **.dockerignore** - exclude unnecessary files

## 🚀 Quick start

### 1. Prepare files

```bash
# Copy project to new computer
git clone <your-repo> facebook-ads-etl
cd facebook-ads-etl
```

### 2. Setup secrets

```bash
# Create .env file
cp .env.example .env  # and fill with your data

# Place Google Cloud credentials
mkdir -p secret/
cp /path/to/your-service-account.json secret/
```

**Example .env file:**
```env
FACEBOOK_APP_ID=your_app_id
FACEBOOK_APP_SECRET=your_app_secret
FACEBOOK_ACCESS_TOKEN=your_access_token
FACEBOOK_AD_ACCOUNT_ID=act_your_account_id
GOOGLE_APPLICATION_CREDENTIALS=/app/secret/your-service-account.json
```

### 3. Launch

```bash
# Build image
docker-compose build

# Run daily sync
docker-compose up facebook-ads-etl

# Run backfill (one-time)
docker-compose --profile backfill up facebook-ads-backfill

# Run in background
docker-compose up -d facebook-ads-etl
```

## 🛠️ Management commands

### Build
```bash
# Rebuild image
docker-compose build --no-cache
```

### Run different tasks
```bash
# Daily sync
docker-compose run --rm facebook-ads-etl python run_etl.py daily

# Backfill for 90 days
docker-compose run --rm facebook-ads-etl python run_etl.py backfill --days 90

# Check status
docker-compose run --rm facebook-ads-etl python run_etl.py status
```

### Logs and debugging
```bash
# View logs
docker-compose logs facebook-ads-etl

# Enter container for debugging
docker-compose run --rm facebook-ads-etl bash

# Test connection
docker-compose run --rm facebook-ads-etl python test_conversions.py
```

### Cleanup
```bash
# Stop and remove containers
docker-compose down

# Remove image
docker rmi facebook-ads-to-bigquery_facebook-ads-etl
```

## 📁 Project structure for Docker

```
facebook-ads-etl/
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── .env                    # Your secrets
├── secret/
│   └── service-account.json # Google Cloud credentials
├── logs/                   # Logs (optional)
├── *.py                   # Python scripts
└── requirements.txt       # Dependencies
```

## 🔧 Setup cron for regular execution

On new server you can setup automatic execution:

```bash
# Add to crontab
0 6 * * * cd /path/to/facebook-ads-etl && docker-compose up facebook-ads-etl
```

## 🚨 Troubleshooting

### Permission issues
```bash
# If there are file permission issues
sudo chown -R $USER:$USER secret/ logs/
```

### Network issues
```bash
# Check API availability
docker-compose run --rm facebook-ads-etl ping graph.facebook.com
```

### Code updates
```bash
# After code changes
docker-compose down
docker-compose build
docker-compose up facebook-ads-etl
```

## 📋 System requirements

- **Docker** 20.0+
- **Docker Compose** 2.0+
- **RAM**: minimum 1GB
- **Disk**: ~500MB for image

## ✅ Ready!

Now the project is fully portable and easily deployable on any computer with Docker! 🎉 