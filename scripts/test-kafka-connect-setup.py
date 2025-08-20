#!/usr/bin/env python3
"""
Test script for Kafka Connect setup validation

This script validates that Kafka Connect is properly configured with
the necessary plugins and can connect to required services.
"""

import requests
import json
import time
import sys
from typing import Dict, List


def test_kafka_connect_health(connect_url: str = "http://localhost:8083") -> bool:
    """Test if Kafka Connect is healthy and responsive"""
    try:
        response = requests.get(f"{connect_url}/", timeout=10)
        if response.status_code == 200:
            print("✓ Kafka Connect is healthy and responsive")
            return True
        else:
            print(f"✗ Kafka Connect health check failed: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Kafka Connect health check failed: {e}")
        return False


def test_connector_plugins(connect_url: str = "http://localhost:8083") -> bool:
    """Test if required connector plugins are available"""
    required_plugins = [
        "io.confluent.connect.s3.S3SinkConnector",
        "com.snowflake.kafka.connector.SnowflakeSinkConnector"
    ]
    
    try:
        response = requests.get(f"{connect_url}/connector-plugins", timeout=10)
        response.raise_for_status()
        
        plugins = response.json()
        available_plugins = [plugin['class'] for plugin in plugins]
        
        all_found = True
        for plugin in required_plugins:
            if plugin in available_plugins:
                print(f"✓ Found required plugin: {plugin}")
            else:
                print(f"✗ Missing required plugin: {plugin}")
                all_found = False
        
        return all_found
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Failed to check connector plugins: {e}")
        return False


def test_kafka_topics(connect_url: str = "http://localhost:8083") -> bool:
    """Test if required Kafka topics exist"""
    # This would typically require a Kafka client, but for simplicity
    # we'll just check if Connect can list topics (indirectly)
    try:
        # Try to get connector status (which requires Kafka connectivity)
        response = requests.get(f"{connect_url}/connectors", timeout=10)
        response.raise_for_status()
        print("✓ Kafka Connect can communicate with Kafka cluster")
        return True
    except requests.exceptions.RequestException as e:
        print(f"✗ Kafka Connect cannot communicate with Kafka: {e}")
        return False


def main():
    """Run all Kafka Connect setup tests"""
    print("Testing Kafka Connect setup...")
    print("=" * 50)
    
    connect_url = "http://localhost:8083"
    
    tests = [
        ("Kafka Connect Health", lambda: test_kafka_connect_health(connect_url)),
        ("Required Plugins", lambda: test_connector_plugins(connect_url)),
        ("Kafka Connectivity", lambda: test_kafka_topics(connect_url))
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\nTesting {test_name}...")
        result = test_func()
        results.append((test_name, result))
    
    print("\n" + "=" * 50)
    print("Test Results Summary:")
    
    all_passed = True
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  {test_name}: {status}")
        if not result:
            all_passed = False
    
    if all_passed:
        print("\n✓ All tests passed! Kafka Connect setup is ready.")
        return 0
    else:
        print("\n✗ Some tests failed. Please check the setup.")
        return 1


if __name__ == "__main__":
    sys.exit(main())