# Scripts Directory

This directory contains all automation scripts for the Stock Market Pipeline, organized by functionality.

## 📁 Directory Structure

### `infrastructure/`
- **start-cluster.sh** - Starts the complete streaming pipeline with Spark cluster mode
- **ultra-fast-build.sh** - Docker build optimization with Spark base caching

### `database/`
- **create_redshift_schemas.sql** - Creates streaming schemas and tables in Redshift
- **setup-redshift-streaming.sh** - Redshift setup wrapper script

### `connectors/`
- **deploy-connectors.sh** - Deploys all Kafka Connect connectors (Bronze/Silver/Redshift)
- **kafka-connect-manager.py** - Kafka Connect management utility

### `schemas/`
- **init-schema-registry.py** - Initializes Schema Registry with Avro schemas

## 🚀 Quick Start

Most scripts are called automatically by the Makefile commands:
- `make setup-dev` - Uses infrastructure scripts
- `make start-dev` - Uses infrastructure and connector scripts
- `make deploy-connectors` - Uses connector scripts

## 📝 Usage

Each script can be run independently, but they're designed to work together as part of the complete pipeline orchestration.
