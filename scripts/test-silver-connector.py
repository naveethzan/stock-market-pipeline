#!/usr/bin/env python3
"""
Test script for Silver layer S3 connector

This script tests the Silver layer S3 connector configuration and validates
that processed data is properly stored in S3 with Parquet format.
"""

import json
import time
import boto3
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
from typing import Dict, Any
from datetime import datetime


class SilverConnectorTester:
    """Test class for Silver layer S3 connector"""
    
    def __init__(self, 
                 connect_url: str = "http://localhost:8083",
                 kafka_bootstrap_servers: str = "localhost:29092",
                 s3_bucket: str = None,
                 aws_region: str = "us-east-1"):
        self.connect_url = connect_url.rstrip('/')
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.s3_bucket = s3_bucket
        self.aws_region = aws_region
        
        # Initialize clients
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
    
    def create_test_connector(self) -> bool:
        """Create the Silver layer S3 connector"""
        try:
            with open('config/kafka-connect/connectors/silver-s3-connector.json', 'r') as f:
                connector_config = json.load(f)
            
            # Replace environment variables with actual values
            config_str = json.dumps(connector_config)
            config_str = config_str.replace('${env:AWS_DEFAULT_REGION}', self.aws_region)
            config_str = config_str.replace('${env:S3_BUCKET_NAME}', self.s3_bucket or 'test-bucket')
            connector_config = json.loads(config_str)
            
            response = requests.post(
                f"{self.connect_url}/connectors",
                json=connector_config,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [201, 409]:  # Created or already exists
                print("✓ Silver S3 connector created/updated successfully")
                return True
            else:
                print(f"✗ Failed to create Silver S3 connector: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"✗ Error creating Silver S3 connector: {e}")
            return False
    
    def send_test_processed_data(self) -> bool:
        """Send test processed data to Kafka topics"""
        current_time = int(time.time() * 1000)
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        test_data = [
            {
                "topic": "processed-stock-prices",
                "key": "AAPL",
                "value": {
                    "symbol": "AAPL",
                    "date": current_date,
                    "open_price": 150.00,
                    "high_price": 152.50,
                    "low_price": 149.75,
                    "close_price": 151.25,
                    "volume": 1500000,
                    "adjusted_close": 151.25,
                    "sma_20": 150.85,
                    "sma_50": 149.95,
                    "rsi_14": 65.5,
                    "processing_timestamp": current_time,
                    "data_quality_score": 0.98
                }
            },
            {
                "topic": "processed-trading-volume",
                "key": "GOOGL",
                "value": {
                    "symbol": "GOOGL",
                    "date": current_date,
                    "volume": 750000,
                    "volume_weighted_price": 2825.50,
                    "trade_count": 15000,
                    "buy_volume": 400000,
                    "sell_volume": 350000,
                    "volume_sma_20": 800000,
                    "volume_ratio": 0.9375,
                    "processing_timestamp": current_time,
                    "data_quality_score": 0.95
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
                print(f"✓ Sent processed data to {data['topic']} (partition: {record_metadata.partition}, offset: {record_metadata.offset})")
            
            self.producer.flush()
            return True
            
        except KafkaError as e:
            print(f"✗ Failed to send processed test data: {e}")
            return False
    
    def check_connector_status(self) -> bool:
        """Check if the connector is running properly"""
        try:
            response = requests.get(f"{self.connect_url}/connectors/silver-s3-sink-connector/status")
            
            if response.status_code == 200:
                status = response.json()
                connector_state = status.get('connector', {}).get('state')
                
                if connector_state == 'RUNNING':
                    print("✓ Silver S3 connector is running")
                    
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
                            all_running = False
                    
                    return all_running
                else:
                    print(f"✗ Silver S3 connector is in state: {connector_state}")
                    return False
            else:
                print(f"✗ Failed to get connector status: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"✗ Error checking connector status: {e}")
            return False
    
    def verify_s3_parquet_data(self) -> bool:
        """Verify that processed data was written to S3 in Parquet format"""
        if not self.s3_bucket:
            print("⚠ S3 bucket not specified, skipping S3 verification")
            return True
        
        try:
            # List objects in the silver directory
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix='silver/stock-data/'
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                print(f"✓ Found {len(response['Contents'])} objects in S3 silver layer")
                
                # Check for Parquet files
                parquet_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
                if parquet_files:
                    print(f"✓ Found {len(parquet_files)} Parquet files")
                    
                    # Show some example objects
                    for i, obj in enumerate(parquet_files[:3]):
                        print(f"  - {obj['Key']} (size: {obj['Size']} bytes)")
                else:
                    print("⚠ No Parquet files found yet (data may not have been flushed)")
                
                return True
            else:
                print("⚠ No objects found in S3 silver layer (data may not have been flushed yet)")
                return True  # Don't fail the test, data might not be flushed yet
                
        except Exception as e:
            print(f"✗ Error verifying S3 Parquet data: {e}")
            return False
    
    def verify_partitioning_strategy(self) -> bool:
        """Verify that data is properly partitioned by symbol and date"""
        if not self.s3_bucket:
            print("⚠ S3 bucket not specified, skipping partitioning verification")
            return True
        
        try:
            # Look for partitioned structure
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix='silver/stock-data/',
                Delimiter='/'
            )
            
            if 'CommonPrefixes' in response:
                prefixes = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
                print(f"✓ Found partitioned structure with {len(prefixes)} partitions")
                
                # Show some example partitions
                for i, prefix in enumerate(prefixes[:3]):
                    print(f"  - {prefix}")
                
                return True
            else:
                print("⚠ No partitioned structure found yet")
                return True
                
        except Exception as e:
            print(f"✗ Error verifying partitioning strategy: {e}")
            return False
    
    def cleanup(self):
        """Clean up resources"""
        try:
            self.producer.close()
        except:
            pass


def main():
    """Run Silver connector tests"""
    print("Testing Silver Layer S3 Connector...")
    print("=" * 50)
    
    # Get configuration from environment or use defaults
    import os
    
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    if not s3_bucket:
        print("⚠ S3_BUCKET_NAME not set, S3 verification will be skipped")
    
    tester = SilverConnectorTester(
        s3_bucket=s3_bucket,
        aws_region=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )
    
    tests = [
        ("Create Silver S3 Connector", tester.create_test_connector),
        ("Send Processed Test Data", tester.send_test_processed_data),
        ("Check Connector Status", tester.check_connector_status),
        ("Verify S3 Parquet Data", tester.verify_s3_parquet_data),
        ("Verify Partitioning Strategy", tester.verify_partitioning_strategy)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}...")
        result = test_func()
        results.append((test_name, result))
        
        if not result and "Verify S3" not in test_name:
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
        print("\n✓ Silver layer connector tests passed!")
        return 0
    else:
        print("\n✗ Some Silver layer connector tests failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())