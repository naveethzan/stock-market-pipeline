# DBT Execution Guide - Stock Market Pipeline
## Simple, Structured Commands from Start to End

---

## 📋 Prerequisites

### Environment Variables (Required)
```bash
export REDSHIFT_ENDPOINT="your-workgroup.region.redshift-serverless.amazonaws.com"
export REDSHIFT_USER="admin"
export REDSHIFT_PASSWORD="your-password"
```

### Directory Setup
```bash
cd /path/to/Stock-market-pipeline/src/streaming_pipeline/dbt/stock_market_dbt
```

---

## 🚀 Step 1: Initial Validation

### Test DBT Connection
```bash
dbt debug
```
**Expected**: "All checks passed!" and "Connection test: [OK connection ok]"

### Verify Project Structure
```bash
dbt list
```
**Expected**: Shows all staging, snapshot, and marts models

---

## 🏗️ Step 2: One-Time Setup (Run Once)

### 2.1 Initialize Static Dimensions
```bash
# Create date dimension (2020-2030, ~11 years)
dbt run --models dim_date

# Create time dimension (1440 minutes)
dbt run --models dim_time
```

### 2.2 Initialize Company Snapshot (SCD Type 2)
```bash
# Creates initial company snapshot table
dbt snapshot --select company_snapshot
```

### 2.3 Verify Setup
```bash
# Check if dimensions were created
dbt run --models dim_company  # Should create view over snapshot
```

---

## 🔄 Step 3: Regular Operations (Every 5 Minutes)

### 3.1 Basic Run Command
```bash
# Run snapshots first (SCD Type 2)
dbt snapshot --select company_snapshot

# Run all models (staging + marts)
dbt run

# Run tests
dbt test
```

### 3.2 Single Command Version
```bash
# Combined command for regular runs
dbt snapshot --select company_snapshot && dbt run && dbt test
```

---

## 🎯 Step 4: Targeted Runs

### Run Only Staging Layer
```bash
dbt run --models staging
```

### Run Only Marts Layer  
```bash
dbt run --models marts
```

### Run Specific Model
```bash
dbt run --models fact_stock_prices
dbt run --models dim_company
```

### Run with Dependencies
```bash
# Run model and all its upstream dependencies
dbt run --models +fact_stock_prices

# Run model and all downstream dependents
dbt run --models fact_stock_prices+
```

---

## 🧪 Step 5: Testing & Validation

### Run All Tests
```bash
dbt test
```

### Run Specific Tests
```bash
dbt test --models staging
dbt test --models marts
```

### Run Data Quality Checks
```bash
# Test only unique/not_null constraints
dbt test --select test_type:generic
```

---

## 📊 Step 6: Development & Debugging

### Compile Models (No Execution)
```bash
dbt compile
```

### Preview Model Results (Limit 10 rows)
```bash
dbt show --models stg_processed_stock --limit 10
```

### Fresh Check (Data Freshness)
```bash
dbt source freshness
```

### Generate Documentation
```bash
dbt docs generate
dbt docs serve  # Opens in browser at localhost:8080
```

---

## 🔧 Step 7: Troubleshooting Commands

### Debug Failed Model
```bash
# Run single model with verbose output
dbt run --models fact_stock_prices --debug
```

### Check Model Dependencies
```bash
dbt list --models +fact_stock_prices --output name
```

### Clear Target Directory
```bash
dbt clean
```

### Full Refresh (Recreate Incremental Tables)
```bash
dbt run --full-refresh
```

---

## ⚡ Step 8: Simple Automation Options

### Option A: Basic Cron (Simple)
```bash
# Edit crontab
crontab -e

# Add this line for every 5 minutes
*/5 * * * * cd /path/to/dbt/project && dbt snapshot --select company_snapshot && dbt run && dbt test
```

### Option B: Simple Script
```bash
#!/bin/bash
# File: run-dbt-simple.sh

cd /path/to/Stock-market-pipeline/src/streaming_pipeline/dbt/stock_market_dbt

# Set environment
export REDSHIFT_ENDPOINT="your-endpoint"
export REDSHIFT_USER="admin"  
export REDSHIFT_PASSWORD="your-password"

# Run DBT
echo "$(date): Starting DBT run"
dbt snapshot --select company_snapshot
dbt run
dbt test
echo "$(date): DBT run completed"
```

### Option C: Manual Schedule
```bash
# Run every 5 minutes manually during active development
watch -n 300 'dbt snapshot --select company_snapshot && dbt run'
```

---

## 📈 Step 9: Monitoring & Health Checks

### Check Row Counts
```bash
# Use dbt to query row counts
dbt run-operation --args "{'models': ['staging', 'marts']}" row_count_check
```

### Manual Health Check
```bash
# Connect to Redshift and check
psql -h $REDSHIFT_ENDPOINT -U $REDSHIFT_USER -d stockmarket -c "
SELECT 
    'staging.stg_processed_stock' as table_name, 
    COUNT(*) as row_count,
    MAX(dbt_loaded_at) as latest_load
FROM staging.stg_processed_stock
UNION ALL
SELECT 
    'marts.fact_stock_prices' as table_name, 
    COUNT(*) as row_count,
    MAX(created_at) as latest_load  
FROM marts.fact_stock_prices;
"
```

---

## 🎯 Summary: Essential Command Flow

### Daily Workflow
```bash
# 1. Morning setup check
dbt debug && dbt list

# 2. Regular runs (every 5 minutes)
dbt snapshot --select company_snapshot && dbt run && dbt test

# 3. Evening validation
dbt docs generate
# Check row counts, review any test failures
```

### Development Workflow  
```bash
# 1. Make model changes
# 2. Test compilation
dbt compile

# 3. Test single model
dbt run --models your_model_name

# 4. Run tests
dbt test --models your_model_name

# 5. Full run when ready
dbt snapshot --select company_snapshot && dbt run
```

---

## ✅ Key Benefits of This Approach

- ✅ **Simple**: No complex bash scripts or error handling
- ✅ **Flexible**: Run manually or with basic automation  
- ✅ **Debuggable**: Easy to see what failed and why
- ✅ **Scalable**: Add complexity only when needed
- ✅ **Clear**: Structured progression from setup to production

---

## 🔄 Migration from Complex Phase 6

Instead of the complex Phase 6 automation, use:

1. **Replace** complex bash script → Simple command combinations
2. **Replace** cron with complex logging → Basic cron or manual runs
3. **Replace** email alerts → Manual monitoring or simple logging
4. **Replace** psql validations → DBT built-in tests and docs

**Result**: Much simpler, easier to maintain, and just as effective!
