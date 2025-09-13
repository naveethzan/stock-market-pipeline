# DBT Implementation Fixes - Systematic Phase-wise Plan
## Critical Issues Identified & Resolution Strategy

---

## 🎯 Overview

During comprehensive code analysis, we identified **5 critical issues** that must be fixed before DBT can run successfully. This document provides a systematic, phase-wise approach to implement all fixes.

### **Issues Summary:**
- 🔥 **Critical (Must Fix)**: 3 issues that prevent DBT from running
- ⚠️ **High Priority**: 1 issue affecting data accuracy  
- 📋 **Medium Priority**: 1 issue affecting maintainability

---

## 📋 Phase 1: Core Configuration Fixes (CRITICAL)

### **1.1 Fix dbt_project.yml Configuration Errors**

**File:** `/src/streaming_pipeline/dbt/stock_market_dbt/dbt_project.yml`

#### **Issue A: Remove Intermediate Schema References**
**Problem:** Lines 32-34 reference `intermediate` schema that doesn't exist
```yaml
# ❌ CURRENT (Lines 32-34):
intermediate:
  +materialized: view
  +schema: intermediate
```

**Solution:** Remove these lines entirely

#### **Issue B: Fix dim_company Materialization Conflict**
**Problem:** Lines 42-45 configure `dim_company` as `incremental` but actual model is `view`
```yaml
# ❌ CURRENT (Lines 42-45):
dim_company:
  +materialized: incremental
  +unique_key: company_key
  +on_schema_change: 'append_new_columns'
```

**Solution:** Remove this configuration (model-level config takes precedence)

#### **Issue C: Fix Invalid unique_key Array**
**Problem:** Line 48 has array with different keys for different models
```yaml
# ❌ CURRENT (Line 48):
+unique_key: ['stock_price_key', 'trading_volume_key']
```

**Solution:** Remove this line (each model should define its own unique_key)

#### **Complete Fix for dbt_project.yml:**
```yaml
models:
  stock_market_dbt:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
      dimensions:
        +materialized: table
        +schema: marts
      facts:
        +materialized: incremental
        +schema: marts
        +on_schema_change: 'append_new_columns'
```

**Expected Result:** ✅ DBT project configuration is clean and consistent

---

## 📋 Phase 2: Critical SQL Logic Fixes

### **2.1 Fix fact_stock_prices.sql Incremental Logic**

**File:** `/models/marts/facts/fact_stock_prices.sql`

#### **Issue:** Incremental WHERE clause in wrong CTE location
**Problem:** Lines 22-24 filter data before JOIN, affecting JOIN completeness

```sql
-- ❌ CURRENT (Lines 10-25):
WITH stock_data AS (
    SELECT 
        s.*,
        t.rsi,
        t.macd,
        t.sma_20,
        t.sma_50,
        t.vwap
    FROM {{ ref('stg_processed_stock') }} s
    LEFT JOIN {{ ref('stg_technical_indicators') }} t
        ON s.symbol = t.symbol 
        AND s.timestamp = t.timestamp
    {% if is_incremental() %}
        WHERE s.dbt_loaded_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)
```

#### **Solution:** Move incremental filter to individual source CTEs
```sql
-- ✅ FIXED:
WITH stock_source AS (
    SELECT *
    FROM {{ ref('stg_processed_stock') }}
    {% if is_incremental() %}
        WHERE dbt_loaded_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

indicators_source AS (
    SELECT *
    FROM {{ ref('stg_technical_indicators') }}
    {% if is_incremental() %}
        WHERE dbt_loaded_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

stock_data AS (
    SELECT 
        s.*,
        t.rsi,
        t.macd,
        t.sma_20,
        t.sma_50,
        t.vwap
    FROM stock_source s
    LEFT JOIN indicators_source t
        ON s.symbol = t.symbol 
        AND s.timestamp = t.timestamp
)
```

**Expected Result:** ✅ Incremental loads include complete data with proper JOINs

### **2.2 Fix fact_trading_volume.sql (Same Pattern)**

**File:** `/models/marts/facts/fact_trading_volume.sql`

#### **Issue:** Ensure consistency with fact_stock_prices pattern

**Current Check:** Lines 13-16 look correct, but verify same pattern:
```sql
WITH volume_data AS (
    SELECT *
    FROM {{ ref('stg_aggregated_data') }}
    {% if is_incremental() %}
        WHERE dbt_loaded_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)
```

**Expected Result:** ✅ Consistent incremental pattern across all fact tables

---

## 📋 Phase 3: Data Logic Corrections

### **3.1 Fix dim_date.sql Trading Day Logic**

**File:** `/models/marts/dimensions/dim_date.sql`

#### **Issue:** Lines 34-37 have incorrect is_trading_day logic
```sql
-- ❌ CURRENT (Lines 30-37):
CASE 
    WHEN EXTRACT(DOW FROM date_actual) IN (0, 6) THEN FALSE
    ELSE TRUE
END AS is_weekday,
CASE 
    WHEN EXTRACT(DOW FROM date_actual) NOT IN (0, 6) THEN TRUE
    ELSE FALSE
END AS is_trading_day  -- This is identical to is_weekday!
```

#### **Solution:** Make is_trading_day more restrictive (exclude holidays)
```sql
-- ✅ FIXED:
CASE 
    WHEN EXTRACT(DOW FROM date_actual) IN (0, 6) THEN FALSE
    ELSE TRUE
END AS is_weekday,
CASE 
    WHEN EXTRACT(DOW FROM date_actual) IN (0, 6) THEN FALSE
    WHEN date_actual IN (
        '2024-01-01', '2024-07-04', '2024-12-25',  -- Major holidays
        '2025-01-01', '2025-07-04', '2025-12-25'   -- Add more as needed
    ) THEN FALSE
    ELSE TRUE
END AS is_trading_day  -- Weekdays minus major holidays
```

**Expected Result:** ✅ Trading day logic excludes weekends and major holidays

---

## 📋 Phase 4: Configuration Cleanup

### **4.1 Clean Up _staging.yml Model Configurations**

**File:** `/models/staging/_staging.yml`

#### **Issue:** Lines 12-32 have model configs that may conflict with SQL configs

**Problem:** YAML model configs can override individual model SQL configs
```yaml
# ❌ CURRENT (Lines 12-32):
models:
  - name: stg_processed_stock
    description: "Parsed processed stock data"
    config:
      materialized: incremental
      unique_key: record_id
      on_schema_change: append_new_columns
  # ... similar for other models
```

#### **Solution:** Remove individual model configs, rely on SQL configs
```yaml
# ✅ FIXED:
models:
  - name: stg_processed_stock
    description: "Parsed processed stock data"
    columns:
      - name: record_id
        description: "Unique record identifier"
        tests:
          - not_null
          - unique
      - name: symbol
        description: "Stock ticker symbol"
        tests:
          - not_null
  # ... add column documentation for other models
```

**Expected Result:** ✅ No config conflicts, better documentation

---

## 📋 Phase 5: Enhanced Documentation

### **5.1 Add Source Table Column Documentation**

**File:** `/models/staging/_staging.yml`

#### **Issue:** Source tables lack column documentation

**Current:** Basic table definitions only
```yaml
sources:
  - name: streaming
    database: stockmarket
    schema: streaming
    tables:
      - name: processed_stock_stream
      # No column documentation
```

#### **Solution:** Add comprehensive source documentation
```yaml
sources:
  - name: streaming
    database: stockmarket
    schema: streaming
    tables:
      - name: processed_stock_stream
        description: "Raw processed stock data from Kafka Connect"
        columns:
          - name: kafka_key
            description: "Kafka message key (typically stock symbol)"
            tests:
              - not_null
          - name: kafka_value
            description: "JSON payload containing stock data"
            tests:
              - not_null
          - name: kafka_partition
            description: "Kafka partition number"
          - name: kafka_offset
            description: "Kafka message offset"
          - name: kafka_timestamp
            description: "Kafka message timestamp"
            tests:
              - not_null
          - name: refresh_time
            description: "When record was written to Redshift"
```

**Expected Result:** ✅ Complete source documentation for better understanding

---

## 🚀 Implementation Execution Plan

### **Phase Execution Order:**

#### **Step 1: Pre-Implementation**
```bash
# 1. Backup current configuration
cp dbt_project.yml dbt_project.yml.backup
cp models/staging/_staging.yml models/staging/_staging.yml.backup
cp models/marts/facts/fact_stock_prices.sql models/marts/facts/fact_stock_prices.sql.backup
cp models/marts/dimensions/dim_date.sql models/marts/dimensions/dim_date.sql.backup

# 2. Test current state (expect failures)
dbt debug  # Should work
dbt compile  # May have config conflicts
```

#### **Step 2: Phase 1 Implementation**
```bash
# Fix core configuration issues
# Edit dbt_project.yml (remove intermediate, dim_company config, unique_key array)
# Test compilation
dbt compile
```

#### **Step 3: Phase 2 Implementation** 
```bash
# Fix fact table SQL logic
# Edit fact_stock_prices.sql (move incremental WHERE)
# Verify fact_trading_volume.sql pattern
dbt compile --models facts
```

#### **Step 4: Phase 3 Implementation**
```bash  
# Fix dimension logic
# Edit dim_date.sql (correct is_trading_day)
dbt compile --models dimensions
```

#### **Step 5: Phase 4 & 5 Implementation**
```bash
# Clean up documentation and configs
# Edit _staging.yml (remove model configs, add source docs)
dbt compile
```

#### **Step 6: Final Validation**
```bash
# Full project compilation test
dbt compile

# Verify all models are valid
dbt list

# Check dependencies
dbt deps  # If using packages
```

---

## ✅ Success Criteria

### **After All Fixes:**
- ✅ `dbt debug` passes without errors
- ✅ `dbt compile` compiles all models successfully
- ✅ `dbt list` shows all expected models
- ✅ No configuration conflicts or warnings
- ✅ SQL logic is correct for incremental processing
- ✅ Dimension logic accurately reflects business rules
- ✅ Complete documentation for sources and models

### **Ready for Testing:**
- ✅ All critical issues resolved
- ✅ High priority issues addressed  
- ✅ Documentation improved
- ✅ Project ready for `dbt run` with actual data

---

## 📝 Implementation Notes

### **Risk Mitigation:**
1. **Backup all files** before making changes
2. **Test each phase** individually with `dbt compile`
3. **Verify dependencies** between models remain intact
4. **Document any deviations** from this plan

### **Testing Strategy:**
1. **Compilation testing** after each phase
2. **Individual model testing** with `dbt compile --models model_name`  
3. **Dependency validation** with `dbt list --models +model_name`
4. **Full project validation** before proceeding to data testing

### **Rollback Plan:**
- Keep all `.backup` files until successful completion
- Each phase can be rolled back independently
- `git` commits after each successful phase (if using version control)

---

## 🎯 Next Steps After Implementation

1. **Test with actual Redshift connection** (when streaming tables available)
2. **Validate incremental processing** with sample data
3. **Test snapshot functionality** with changing company data  
4. **Performance optimization** based on actual data volumes
5. **Set up monitoring** and alerting for production use

This systematic approach ensures all critical issues are resolved while maintaining project integrity and enabling successful DBT execution.
