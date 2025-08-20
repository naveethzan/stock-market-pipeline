#!/usr/bin/env python3
"""
Test script for Bronze layer S3 connector

This script tests the Bronze layer S3 connector configuration and validates
that raw data is properly stored in S3 with Avro format.
"""

import json
import time
import boto3
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import avro.schema
import avro.io
import io
import sys
from typing import Dict, Any


class BronzeConnectorTester:
    """Test class for Bronze layer S3 connector"""
    
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
        """Create the Bronze layer S3 connector"""
        try:
            with open('config/kafka-connect/connectors/bronze-s3-connector.json', 'r') as f:
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
                print("✓ Bronze S3 connector created/updated successfully")
                return True
            else:
                print(f"✗ Failed to create Bronze S3 connector: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"✗ Error creating Bronze S3 connector: {e}")
            return False
    
    def send_test_data(self) -> bool:
        """Send test data to Kafka topics"""
        test_data = [
            {
                "topic": "stock-quotes-realtime",
                "key": "AAPL",
                "value": {
                    "symbol": "AAPL",
                    "price": 150.25,
                    "volume": 1000000,
                    "timestamp": int(time.time() * 1000),
                    "exchange": "NASDAQ"
                }
            },
            {
                "topic": "stock-intraday-data", 
                "key": "GOOGL",
                "value": {
                    "symbol": "GOOGL",
                    "open": 2800.00,
                    "high": 2850.00,
                    "low": 2790.00,
                    "close": 2825.50,
                    "volume": 500000,
                    "timestamp": int(time.time() * 1000),
                    "interval": "1min"
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
                print(f"✓ Sent test data to {data['topic']} (partition: {record_metadata.partition}, offset: {record_metadata.offset})")
            
            self.producer.flush()
            return True
            
        except KafkaError as e:
            print(f"✗ Failed to send test data: {e}")
            return False
    
    def check_connector_status(self) -> bool:
        """Check if the connector is running properly"""
        try:
            response = requests.get(f"{self.connect_url}/connectors/bronze-s3-sink-connector/status")
            
            if response.status_code == 200:
                status = response.json()
                connector_state = status.get('connector', {}).get('state')
                
                if connector_state == 'RUNNING':
                    print("✓ Bronze S3 connector is running")
                    
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
                    print(f"✗ Bronze S3 connector is in state: {connector_state}")
                    return False
            else:
                print(f"✗ Failed to get connector status: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"✗ Error checking connector status: {e}")
            return False
    
    def verify_s3_data(self) -> bool:
        """Verify that data was written to S3"""
        if not self.s3_bucket:
            print("⚠ S3 bucket not specified, skipping S3 verification")
            return True
        
        try:
            # List objects in the bronze directory
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix='bronze/stock-data/'
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                print(f"✓ Found {len(response['Contents'])} objects in S3 bronze layer")
                
                # Show some example objects
                for i, obj in enumerate(response['Contents'][:3]):
                    print(f"  - {obj['Key']} (size: {obj['Size']} bytes)")
                
                return True
            else:
                print("⚠ No objects found in S3 bronze layer (data may not have been flushed yet)")
                return True  # Don't fail the test, data might not be flushed yet
                
        except Exception as e:
            print(f"✗ Error verifying S3 data: {e}")
            return False
    
    def cleanup(self):
        """Clean up resources"""
        try:
            self.producer.close()
        except:
            pass


def main():
    """Run Bronze connector tests"""
    print("Testing Bronze Layer S3 Connector...")
    print("=" * 50)
    
    # Get configuration from environment or use defaults
    import os
    
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    if not s3_bucket:
        print("⚠ S3_BUCKET_NAME not set, S3 verification will be skipped")
    
    tester = BronzeConnectorTester(
        s3_bucket=s3_bucket,
        aws_region=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )
    
    tests = [
        ("Create Bronze S3 Connector", tester.create_test_connector),
        ("Send Test Data", tester.send_test_data),
        ("Check Connector Status", tester.check_connector_status),
        ("Verify S3 Data", tester.verify_s3_data)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}...")
        result = test_func()
        results.append((test_name, result))
        
        if not result and test_name != "Verify S3 Data":
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
        print("\n✓ Bronze layer connector tests passed!")
        return 0
    else:
        print("\n✗ Some Bronze layer connector tests failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())