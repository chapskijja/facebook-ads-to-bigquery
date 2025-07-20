# 🚀 ETL Automation via GitHub Actions

## 📋 Step-by-Step Setup

### 1. Adding Secrets to GitHub

Go to **Settings** → **Secrets and variables** → **Actions** in your repository and add the following secrets:

#### 🔑 Facebook API Secrets:
- `FACEBOOK_APP_ID` - your App ID from Facebook Developers
- `FACEBOOK_APP_SECRET` - your App Secret from Facebook Developers  
- `FACEBOOK_ACCESS_TOKEN` - your Access Token
- `FACEBOOK_AD_ACCOUNT_ID` - Ad Account ID (format: act_123456789)

#### 🔐 Google Cloud Secrets:
- `GOOGLE_SERVICE_ACCOUNT_JSON` - content of Google Cloud service account JSON file

### 2. Activating GitHub Actions

```bash
# Add file to git
git add .github/workflows/daily-etl.yml
git commit -m "Add GitHub Actions automation"
git push origin main
```

### 3. Testing the Setup

1. Open the **Actions** tab in your GitHub repository
2. Run the workflow manually for testing:
   - Find "Daily Facebook Ads ETL"
   - Click "Run workflow"
   - Click the green "Run workflow" button

## ⏰ Schedule

ETL runs automatically **every day at 4:00 UTC**.

To change the time, edit the line in `.github/workflows/daily-etl.yml`:
```yaml
- cron: '0 4 * * *'  # 4:00 UTC
- cron: '0 6 * * *'  # 6:00 UTC (example)
```

## 📊 Monitoring and Logs

### Viewing Logs:
1. Go to **Actions** → **Daily Facebook Ads ETL**
2. Select the desired run
3. Click on the "etl" job to view detailed logs

### Error Notifications:
GitHub automatically sends email notifications when workflows fail.

## 🔧 Local Testing

Before setting up GitHub Actions, test locally:

```bash
# Check status
python run_etl.py status

# Run test ETL
python run_etl.py daily
```

## 💰 Cost

- **Public repositories:** FREE
- **Private repositories:** 2000 minutes/month free
- **Estimated ETL execution time:** 2-5 minutes
- **Sufficient for:** ~400-1000 runs per month

## 🛠️ Troubleshooting

### "Invalid credentials" Error
- Check that API keys in secrets are correct
- Ensure Access Token hasn't expired

### "Permission denied" Error in BigQuery  
- Check Google Cloud service account permissions
- Ensure JSON file is copied correctly (without line breaks)

### Workflow doesn't start automatically
- Make sure the file is in the `main` or `master` branch
- Check YAML file syntax

## 📈 Optimization

To save GitHub Actions minutes:
- ETL runs only when new data is available
- Uses smart date checking logic
- Average execution time: 2-3 minutes

## 🎯 Result

After setup, your ETL will automatically:
- ✅ Run every day at 4:00 UTC
- ✅ Check for new data availability
- ✅ Load only missing dates  
- ✅ Send notifications on errors
- ✅ Log all operations 