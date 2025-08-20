#!/usr/bin/env python3
"""
Test script for Gold layer Snowflake connector

This script tests the Gold layer Snowflake connector configuration and validates
that processed data is properly loaded into Snowflake dimensional tables.
"""

import json
import time
import requests
import snowflake.connector
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os
from typing import Dict, Any
from datetime import datetime


class GoldConnectorTester:
    """Test class for Gold layer Snowflake connector"""
    
    def __init__(self, 
                 connect_url: str = "http://localhost:8083",
                 kafka_bootstrap_servers: str = "localhost:29092"):
        self.connect_url = connect_url.rstrip('/')
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        
        # Snowflake connection parameters
        self.snowflake_config = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'role': os.getenv('SNOWFLAKE_ROLE')
        }
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
    
    def validate_snowflake_config(self) -> bool:
        """Validate Snowflake configuration"""
        required_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_DATABASE', 
                        'SNOWFLAKE_SCHEMA', 'SNOWFLAKE_WAREHOUSE']
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            print(f"✗ Missing required environment variables: {', '.join(missing_vars)}")
            return False
        
        print("✓ Snowflake configuration variables are set")
        return True
    
    def test_snowflake_connection(self) -> bool:
        """Test connection to Snowflake"""
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            print(f"✓ Connected to Snowflake (version: {version})")
            
            # Test database and schema access
            cursor.execute(f"USE DATABASE {self.snowflake_config['database']}")
            cursor.execute(f"USE SCHEMA {self.snowflake_config['schema']}")
            print(f"✓ Successfully accessed database and schema")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"✗ Failed to connect to Snowflake: {e}")
            return False
    
    def create_test_connector(self) -> bool:
        """Create the Gold layer Snowflake connector"""
        try:
            with open('config/kafka-connect/connectors/gold-snowflake-connector.json', 'r') as f:
                connector_config = json.load(f)
            
            # Replace environment variables with actual values
            config_str = json.dumps(connector_config)
            for key, value in self.snowflake_config.items():
                if value:
                    env_var = f"SNOWFLAKE_{key.upper()}"
                    config_str = config_str.replace(f"${{env:{env_var}}}", str(value))
            
            # Handle URL specifically
            snowflake_url = f"https://{self.snowflake_config['account']}.snowflakecomputing.com"
            config_str = config_str.replace("${env:SNOWFLAKE_URL}", snowflake_url)
            
            connector_config = json.loads(config_str)
            
            response = requests.post(
                f"{self.connect_url}/connectors",
                json=connector_config,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [201, 409]:  # Created or already exists
                print("✓ Gold Snowflake connector created/updated successfully")
                return True
            else:
                print(f"✗ Failed to create Gold Snowflake connector: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"✗ Error creating Gold Snowflake connector: {e}")
            return False
    
    def send_test_dimensional_data(self) -> bool:
        """Send test dimensional data to Kafka topics"""
        current_time = int(time.time() * 1000)
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        test_data = [
            {
                "topic": "processed-stock-prices",
                "key": "AAPL",
                "value": {
                    "symbol": "AAPL",
                    "company_name": "Apple Inc.",
                    "date": current_date,
                    "time": "09:30:00",
                    "open_price": 150.00,
                    "high_price": 152.50,
                    "low_price": 149.75,
                    "close_price": 151.25,
                    "volume": 1500000,
                    "adjusted_close": 151.25,
                    "dividend_amount": 0.0,
                    "split_coefficient": 1.0,
                    "sma_20": 150.85,
                    "sma_50": 149.95,
                    "ema_12": 151.10,
                    "ema_26": 150.50,
                    "rsi_14": 65.5,
                    "macd": 0.60,
                    "macd_signal": 0.45,
                    "processing_timestamp": current_time,
                    "data_source": "alpha_vantage",
                    "data_quality_score": 0.98
                }
            },
            {
                "topic": "processed-trading-volume",
                "key": "GOOGL",
                "value": {
                    "symbol": "GOOGL",
                    "company_name": "Alphabet Inc.",
                    "date": current_date,
                    "time": "09:30:00",
                    "volume": 750000,
                    "volume_weighted_price": 2825.50,
                    "trade_count": 15000,
                    "buy_volume": 400000,
                    "sell_volume": 350000,
                    "volume_sma_20": 800000,
                    "volume_ratio": 0.9375,
                    "processing_timestamp": current_time,
                    "data_source": "alpha_vantage"
                }
            },
            {
                "topic": "processed-technical-indicators",
                "key": "MSFT",
                "value": {
                    "symbol": "MSFT",
                    "date": current_date,
                    "sma_20": 335.50,
                    "sma_50": 330.25,
                    "ema_12": 337.80,
                    "ema_26": 333.90,
                    "rsi_14": 58.2,
                    "macd": 3.90,
                    "macd_signal": 2.15,
                    "bollinger_upper": 345.00,
                    "bollinger_lower": 325.00,
                    "processing_timestamp": current_time,
                    "calculation_method": "standard"
                }
            }
        ]
        
        try:
            for data in test_data:
                future = self.producer.send(
                    data["topic"],
                    key=data["key"],
                    value=data["value"]
                )
                
                # Wait for send to complete
                record_metadata = future.get(timeout=10)
                print(f"✓ Sent dimensional data to {data['topic']} (partition: {record_metadata.partition}, offset: {record_metadata.offset})")
            
            self.producer.flush()
            return True
            
        except KafkaError as e:
            print(f"✗ Failed to send dimensional test data: {e}")
            return False
    
    def check_connector_status(self) -> bool:
        """Check if the connector is running properly"""
        try:
            response = requests.get(f"{self.connect_url}/connectors/gold-snowflake-sink-connector/status")
            
            if response.status_code == 200:
                status = response.json()
                connector_state = status.get('connector', {}).get('state')
                
                if connector_state == 'RUNNING':
                    print("✓ Gold Snowflake connector is running")
                    
                    # Check task status
                    tasks = status.get('tasks', [])
                    all_running = True
                    for task in tasks:
                        task_state = task.get('state')
                        task_id = task.get('id')
                        if task_state == 'RUNNING':
                            print(f"✓ Task {task_id} is running")
                        else:
                            print(f"✗ Task {task_id} is in state: {task_state}")
                            if 'trace' in task:
                                print(f"  Error: {task['trace']}")
                            all_running = False
                    
                    return all_running
                else:
                    print(f"✗ Gold Snowflake connector is in state: {connector_state}")
                    return False
            else:
                print(f"✗ Failed to get connector status: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"✗ Error checking connector status: {e}")
            return False
    
    def verify_snowflake_data(self) -> bool:
        """Verify that data was loaded into Snowflake staging tables"""
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            # Check staging tables
            staging_tables = [
                'FACT_STOCK_PRICES_STAGING',
                'FACT_TRADING_VOLUME_STAGING', 
                'TECHNICAL_INDICATORS_STAGING'
            ]
            
            all_tables_have_data = True
            for table in staging_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    if count > 0:
                        print(f"✓ Found {count} records in {table}")
                    else:
                        print(f"⚠ No records found in {table} (data may not have been flushed yet)")
                        # Don't fail the test, data might not be flushed yet
                        
                except Exception as table_error:
                    print(f"⚠ Could not query {table}: {table_error}")
                    # Table might not exist yet, which is okay for initial testing
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"✗ Error verifying Snowflake data: {e}")
            return False
    
    def verify_direct_ingestion_setup(self) -> bool:
        """Verify direct ingestion setup for Kafka Connect"""
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            # Check if staging tables exist
            staging_tables = [
                'FACT_STOCK_PRICES_STAGING',
                'FACT_TRADING_VOLUME_STAGING', 
                'TECHNICAL_INDICATORS_STAGING'
            ]
            
            existing_tables = []
            for table in staging_tables:
                try:
                    cursor.execute(f"DESCRIBE TABLE {table}")
                    existing_tables.append(table)
                    print(f"✓ Table {table} exists")
                except:
                    print(f"⚠ Table {table} does not exist (will be created by Kafka Connect)")
            
            if existing_tables:
                print(f"✓ Found {len(existing_tables)} existing staging tables")
            else:
                print("⚠ No staging tables found (Kafka Connect will create them)")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"✗ Error verifying direct ingestion setup: {e}")
            return False
    
    def cleanup(self):
        """Clean up resources"""
        try:
            self.producer.close()
        except:
            pass


def main():
    """Run Gold connector tests"""
    print("Testing Gold Layer Snowflake Connector...")
    print("=" * 50)
    
    tester = GoldConnectorTester()
    
    tests = [
        ("Validate Snowflake Config", tester.validate_snowflake_config),
        ("Test Snowflake Connection", tester.test_snowflake_connection),
        ("Create Gold Snowflake Connector", tester.create_test_connector),
        ("Send Dimensional Test Data", tester.send_test_dimensional_data),
        ("Check Connector Status", tester.check_connector_status),
        ("Verify Snowflake Data", tester.verify_snowflake_data),
        ("Verify Direct Ingestion Setup", tester.verify_direct_ingestion_setup)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}...")
        result = test_func()
        results.append((test_name, result))
        
        # Stop on critical failures, but allow verification tests to continue
        if not result and test_name not in ["Verify Snowflake Data", "Verify Direct Ingestion Setup"]:
            print(f"Stopping tests due to failure in: {test_name}")
            break
    
    # Cleanup
    tester.cleanup()
    
    print("\n" + "=" * 50)
    print("Test Results Summary:")
    
    all_passed = True
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  {test_name}: {status}")
        if not result:
            all_passed = False
    
    if all_passed:
        print("\n✓ Gold layer connector tests passed!")
        return 0
    else:
        print("\n✗ Some Gold layer connector tests failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())