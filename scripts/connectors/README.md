# Connector Management

Scripts and configurations for managing Kafka Connect connectors in the medallion architecture.

## 📁 Directory Structure

```
scripts/connectors/
├── README.md                           # This file
├── scripts/                            # Management scripts
│   ├── deploy-connectors.sh            # Deploy all connectors
│   └── kafka-connect-manager.py        # Connector management utility
├── configs/                            # Connector configurations
│   ├── bronze/                         # Bronze layer (raw data)
│   │   ├── bronze-s3-connector.json   # S3 connector config
│   │   └── README.md                   # Bronze documentation
│   ├── silver/                         # Silver layer (processed data)
│   │   ├── silver-s3-connector.json   # S3 connector config
│   │   └── README.md                   # Silver documentation
│   └── gold/                           # Gold layer (analytics data)
│       ├── redshift-streaming-connector.json  # Redshift connector config
│       └── README.md                   # Gold documentation
└── docs/                               # Additional documentation
    ├── README-bronze.md                # Detailed Bronze docs
    ├── README-silver.md                # Detailed Silver docs
    └── README-gold.md                  # Detailed Gold docs
```

## 🚀 Quick Start

### Deploy All Connectors
```bash
# Deploy all 3 medallion connectors
./scripts/connectors/scripts/deploy-connectors.sh

# Or use Makefile
make deploy-connectors
```

### Manage Individual Connectors
```bash
# List all connectors
python3 scripts/connectors/scripts/kafka-connect-manager.py list

# Check connector status
python3 scripts/connectors/scripts/kafka-connect-manager.py status bronze-s3-sink-connector

# Restart a connector
python3 scripts/connectors/scripts/kafka-connect-manager.py restart bronze-s3-sink-connector
```

## 📊 Medallion Architecture

### Bronze Layer (Raw Data)
- **Purpose:** Store raw, unprocessed data
- **Format:** Avro with schema registry
- **Storage:** S3 with time-based partitioning
- **Topics:** `stock-quotes-realtime`, `stock-intraday-data`

### Silver Layer (Processed Data)
- **Purpose:** Store cleaned and validated data
- **Format:** Parquet for analytics
- **Storage:** S3 with time-based partitioning
- **Topics:** `processed-stock-prices`, `processed-trading-volume`, `processed-technical-indicators`

### Gold Layer (Analytics Data)
- **Purpose:** Store analytics-ready data
- **Format:** JSON for flexibility
- **Storage:** Redshift streaming tables
- **Topics:** `processed-stock-prices`, `processed-trading-volume`, `processed-technical-indicators`

## 🔧 Configuration

Each connector layer has its own configuration directory with:
- **JSON Configuration:** Connector-specific settings
- **README.md:** Detailed documentation for that layer
- **Environment Variables:** Required for deployment

## 📚 Documentation

- **Layer-specific docs:** Each `configs/*/README.md` contains detailed information
- **Management scripts:** See `scripts/` directory for deployment and management
- **Additional docs:** See `docs/` directory for comprehensive documentation
