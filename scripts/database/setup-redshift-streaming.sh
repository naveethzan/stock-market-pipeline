#!/bin/bash

# ===============================================
# REDSHIFT STREAMING TABLES SETUP SCRIPT
# ===============================================
# Creates the streaming schema and tables required for Kafka Connect
# Run this BEFORE deploying Kafka Connect connectors
# ===============================================

set -e

echo "🔧 REDSHIFT STREAMING TABLES SETUP"
echo "=================================="
echo ""

# Load environment variables
echo "📁 Loading environment variables..."
if [ -f "config/.env" ]; then
    set -a
    source config/.env
    set +a
    echo "✅ Environment loaded from config/.env"
else
    echo "❌ config/.env not found!"
    exit 1
fi

# Validate required Redshift variables
echo ""
echo "🔍 Validating Redshift configuration..."
required_vars=("REDSHIFT_ENDPOINT" "REDSHIFT_DATABASE" "REDSHIFT_USER" "REDSHIFT_PASSWORD" "REDSHIFT_PORT")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo "❌ Missing required Redshift variables:"
    printf '   %s\n' "${missing_vars[@]}"
    exit 1
fi

echo "✅ Redshift configuration validated"
echo "   Endpoint: $REDSHIFT_ENDPOINT"
echo "   Database: $REDSHIFT_DATABASE"
echo "   User: $REDSHIFT_USER"

# Test Redshift connection
echo ""
echo "🔌 Testing Redshift connection..."
export PGPASSWORD="$REDSHIFT_PASSWORD"

if command -v psql >/dev/null 2>&1; then
    if psql -h "$REDSHIFT_ENDPOINT" -p "$REDSHIFT_PORT" -U "$REDSHIFT_USER" -d "$REDSHIFT_DATABASE" -c "SELECT version();" >/dev/null 2>&1; then
        echo "✅ Redshift connection successful"
    else
        echo "❌ Failed to connect to Redshift"
        echo "   Please verify your credentials and network connectivity"
        exit 1
    fi
else
    echo "⚠️  psql not found - skipping connection test"
    echo "   Install PostgreSQL client to test connection"
fi

# Execute schema creation script
echo ""
echo "📊 Creating streaming schemas and tables..."
schema_script="scripts/create_redshift_schemas.sql"

if [ ! -f "$schema_script" ]; then
    echo "❌ Schema script not found: $schema_script"
    exit 1
fi

echo "   Executing: $schema_script"

if command -v psql >/dev/null 2>&1; then
    if psql -h "$REDSHIFT_ENDPOINT" -p "$REDSHIFT_PORT" -U "$REDSHIFT_USER" -d "$REDSHIFT_DATABASE" -f "$schema_script"; then
        echo "✅ Schema creation completed successfully"
    else
        echo "❌ Schema creation failed"
        exit 1
    fi
else
    echo "⚠️  Cannot execute schema script - psql not available"
    echo "   Please run the following manually in Redshift:"
    echo "   cat $schema_script"
fi

# Verify tables were created
echo ""
echo "🔍 Verifying streaming tables..."
if command -v psql >/dev/null 2>&1; then
    echo "   Streaming tables found:"
    psql -h "$REDSHIFT_ENDPOINT" -p "$REDSHIFT_PORT" -U "$REDSHIFT_USER" -d "$REDSHIFT_DATABASE" \
        -c "SELECT '  ✓ ' || schemaname || '.' || tablename as table_name FROM pg_tables WHERE schemaname = 'streaming' ORDER BY tablename;" \
        -t -A 2>/dev/null || true
fi

echo ""
echo "🎉 REDSHIFT STREAMING SETUP COMPLETE!"
echo "===================================="
echo ""
echo "📋 Next steps:"
echo "1. Deploy Kafka Connect connectors: ./scripts/deploy-connectors.sh"
echo "2. Start Phase 3 producers to generate data"
echo "3. Run DBT to transform data: cd dbt && dbt run"
echo ""
echo "📊 Created streaming tables:"
echo "  • streaming.processed_stock_prices_stream"
echo "  • streaming.processed_technical_indicators_stream"
echo "  • streaming.processed_trading_volume_stream"