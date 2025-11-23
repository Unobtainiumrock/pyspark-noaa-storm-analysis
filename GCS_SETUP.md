# Google Cloud Storage Setup for PySpark

## Overview
To access GCS files from PySpark, you need:
1. ✅ GCS connector JAR file (already in Dockerfile)
2. Authentication credentials (service account key)
3. Spark configuration in your notebook

## Step-by-Step Setup

### Step 1: Get Service Account Key
John mentioned a service account is already set up for the bucket. You need to:

1. **Ask John for the service account JSON key file**, OR
2. **Get it from Google Cloud Console:**
   - Go to: IAM & Admin → Service Accounts
   - Find the service account with access to `msds-694-cohort-14-group12`
   - Create/download a JSON key

### Step 2: Place Key File in Project
Once you have the JSON key file:
```bash
# Place it in your project root (will be mounted to container)
cp /path/to/service-account-key.json /home/unobtainium/Desktop/github/pyspark-noaa-storm-analysis/gcs-key.json
```

**Important:** Add `gcs-key.json` to `.gitignore` (already there for `.env` files)

### Step 3: Update Your Notebook

In your PySpark notebook, configure SparkSession with GCS support:

```python
from pyspark.sql import SparkSession

# Configure Spark with GCS connector
spark = SparkSession.builder \
    .appName("HumanHarmAnalysis") \
    .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-2.2.11.jar") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/sparkdev/app/gcs-key.json") \
    .getOrCreate()

sc = spark.sparkContext

# Now you can read from GCS
csv_path = "gs://msds-694-cohort-14-group12/storm_data.csv"
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .csv(csv_path)
```

### Step 4: Alternative - Environment Variable

If you prefer using environment variables instead of hardcoding the path:

1. Create `.env` file (or add to existing):
   ```
   GOOGLE_APPLICATION_CREDENTIALS=/home/sparkdev/app/gcs-key.json
   ```

2. In notebook, use:
   ```python
   import os
   
   spark = SparkSession.builder \
       .appName("HumanHarmAnalysis") \
       .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-2.2.11.jar") \
       .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
       .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", 
               os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/home/sparkdev/app/gcs-key.json")) \
       .getOrCreate()
   ```

## Current GCS Bucket Info
- **Bucket:** `msds-694-cohort-14-group12`
- **File:** `storm_data.csv`
- **Size:** 1.6 GB
- **Rows:** ~1,974,189 (from get_storm_data.ipynb)
- **Full Path:** `gs://msds-694-cohort-14-group12/storm_data.csv`

## Testing Connection

Once set up, test with:
```python
# Quick test - just count rows
df = spark.read.option("header", "true").csv("gs://msds-694-cohort-14-group12/storm_data.csv")
print(f"Total rows: {df.count()}")
print(f"Columns: {len(df.columns)}")
```

## Troubleshooting

**Error: "File does not exist"**
- Check bucket name: `msds-694-cohort-14-group12`
- Verify service account has read permissions

**Error: "Authentication failed"**
- Verify JSON key file path is correct
- Check key file has proper permissions (readable by container user)

**Error: "Class not found"**
- GCS connector JAR should be in `/opt/spark/jars/`
- Rebuild Docker image: `make build`
