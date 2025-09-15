-- ===============================================
-- REDSHIFT STREAMING TABLES CREATION SCRIPT
-- ===============================================
-- Creates the streaming schema and tables that Kafka Connect will populate
-- These tables store raw JSON data from Kafka topics
-- 
-- Usage: Execute this in Redshift BEFORE deploying Kafka Connect connectors
-- ===============================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS streaming;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Grant permissions
GRANT USAGE ON SCHEMA streaming TO PUBLIC;
GRANT USAGE ON SCHEMA staging TO PUBLIC;
GRANT USAGE ON SCHEMA marts TO PUBLIC;

-- ===============================================
-- STREAMING LAYER: Raw Kafka data (3 topics → 3 tables)
-- Purpose: Store raw JSON messages from Kafka Connect
-- Structure: Standard Kafka Connect JDBC sink format
-- ===============================================

-- 1. Processed Stock Prices Stream
DROP TABLE IF EXISTS streaming.processed_stock_prices_stream;
CREATE TABLE streaming.processed_stock_prices_stream (
    kafka_key VARCHAR(256),
    kafka_value SUPER,  -- JSON from Phase 3 producers
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    refresh_time TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(kafka_key)
SORTKEY(kafka_timestamp)
ENCODE AUTO;

-- 2. Technical Indicators Stream
DROP TABLE IF EXISTS streaming.processed_technical_indicators_stream;
CREATE TABLE streaming.processed_technical_indicators_stream (
    kafka_key VARCHAR(256),
    kafka_value SUPER,  -- JSON from Phase 3 producers
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    refresh_time TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(kafka_key)
SORTKEY(kafka_timestamp)
ENCODE AUTO;

-- 3. Trading Volume Stream (NEW - was missing!)
DROP TABLE IF EXISTS streaming.processed_trading_volume_stream;
CREATE TABLE streaming.processed_trading_volume_stream (
    kafka_key VARCHAR(256),
    kafka_value SUPER,  -- JSON from Phase 3 producers
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    refresh_time TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(kafka_key)
SORTKEY(kafka_timestamp)
ENCODE AUTO;

-- ===============================================
-- TABLE PERMISSIONS
-- ===============================================
GRANT SELECT ON streaming.processed_stock_prices_stream TO PUBLIC;
GRANT SELECT ON streaming.processed_technical_indicators_stream TO PUBLIC;
GRANT SELECT ON streaming.processed_trading_volume_stream TO PUBLIC;

-- ===============================================
-- VERIFICATION QUERIES
-- ===============================================
-- Run these to verify tables were created correctly:
-- 
-- SELECT schemaname, tablename 
-- FROM pg_tables 
-- WHERE schemaname = 'streaming';
--
-- DESCRIBE streaming.processed_stock_prices_stream;
-- DESCRIBE streaming.processed_technical_indicators_stream;  
-- DESCRIBE streaming.processed_trading_volume_stream;
-- ===============================================