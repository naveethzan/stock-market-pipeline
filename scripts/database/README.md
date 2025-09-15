# Database Scripts

Scripts for database setup and management (Redshift, DBT).

## Scripts

### `create_redshift_schemas.sql`
Creates streaming schemas and tables in Redshift.
- Creates `streaming`, `staging`, and `marts` schemas
- Creates streaming tables for Kafka Connect:
  - `processed_stock_prices_stream`
  - `processed_technical_indicators_stream`
  - `processed_trading_volume_stream`
- Sets up proper permissions and distribution keys

**Usage:** Run this in Redshift BEFORE deploying Kafka Connect connectors

### `setup-redshift-streaming.sh`
Redshift setup wrapper script.
- Validates Redshift configuration from `config/.env`
- Tests Redshift connection
- Executes the schema creation SQL
- Verifies tables were created successfully

**Usage:** `./scripts/database/setup-redshift-streaming.sh`
