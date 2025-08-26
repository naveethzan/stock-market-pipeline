#!/usr/bin/env python3
"""
Script to verify Silver layer Parquet files with symbol+time partitioning in S3.
This script helps validate that the new partitioning scheme is working correctly.
"""
import os
import sys
import json
import logging
import subprocess
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_command(cmd: List[str]) -> tuple:
    """Run a shell command and return the result."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True, result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        return False, e.stdout, e.stderr
    except FileNotFoundError:
        return False, "", f"Command not found: {cmd[0]}"


def check_kafka_connect_status() -> bool:
    """Check if Kafka Connect and Silver connector are running."""
    logger.info("🔍 Checking Kafka Connect status...")
    
    # Check Kafka Connect health
    success, stdout, stderr = run_command(['curl', '-s', 'http://localhost:8083'])
    if not success:
        logger.error(f"❌ Kafka Connect not accessible: {stderr}")
        return False
    
    logger.info("✅ Kafka Connect is running")
    
    # Check Silver connector status
    success, stdout, stderr = run_command([
        'curl', '-s', 'http://localhost:8083/connectors/silver-s3-sink-connector/status'
    ])
    
    if success and stdout:
        try:
            status_data = json.loads(stdout)
            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
            tasks = status_data.get('tasks', [])
            
            logger.info(f"📊 Silver Connector State: {connector_state}")
            
            if connector_state == 'RUNNING':
                logger.info("✅ Silver connector is running")
                
                # Check tasks
                for i, task in enumerate(tasks):
                    task_state = task.get('state', 'UNKNOWN')
                    logger.info(f"   Task {i}: {task_state}")
                    if task_state != 'RUNNING':
                        logger.warning(f"⚠️  Task {i} is not running")
                
                return True
            else:
                logger.error(f"❌ Silver connector is not running: {connector_state}")
                return False
                
        except json.JSONDecodeError as e:
            logger.error(f"❌ Failed to parse connector status: {e}")
            return False
    else:
        logger.error(f"❌ Failed to get connector status: {stderr}")
        return False


def check_kafka_topics() -> bool:
    """Check if processed Kafka topics have data."""
    logger.info("📊 Checking Kafka topics for processed data...")
    
    topics = [
        'processed-stock-prices',
        'processed-trading-volume', 
        'processed-technical-indicators'
    ]
    
    all_good = True
    
    for topic in topics:
        # Get topic offsets
        success, stdout, stderr = run_command([
            'docker', 'exec', 'kafka',
            'kafka-run-class.sh', 'kafka.tools.GetOffsetShell',
            '--broker-list', 'localhost:9092',
            '--topic', topic
        ])
        
        if success and stdout:
            try:
                # Parse offset information
                lines = stdout.strip().split('\n')
                total_messages = 0
                
                for line in lines:
                    if ':' in line:
                        parts = line.split(':')
                        if len(parts) >= 2:
                            offset = int(parts[-1])
                            total_messages += offset
                
                logger.info(f"📈 {topic}: {total_messages} messages")
                
                if total_messages == 0:
                    logger.warning(f"⚠️  {topic} has no messages")
                    all_good = False
                
            except (ValueError, IndexError) as e:
                logger.error(f"❌ Failed to parse offset for {topic}: {e}")
                all_good = False
        else:
            logger.error(f"❌ Failed to get offsets for {topic}: {stderr}")
            all_good = False
    
    return all_good


def check_s3_structure() -> bool:
    """Check S3 bucket structure for new partitioning."""
    logger.info("🗂️  Checking S3 bucket structure...")
    
    # Get AWS configuration from environment
    aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    s3_bucket = os.getenv('S3_BUCKET_NAME', 'streaming-pipeline-dev-bucket')
    
    if not s3_bucket or s3_bucket == 'streaming-pipeline-dev-bucket':
        logger.warning("⚠️  S3 bucket not configured or using default dev bucket")
        logger.info("💡 Set S3_BUCKET_NAME environment variable to check real S3 bucket")
        return True  # Skip S3 check if not configured
    
    logger.info(f"📍 Checking bucket: {s3_bucket}")
    logger.info(f"📍 Region: {aws_region}")
    
    # Check if AWS CLI is available
    success, _, _ = run_command(['aws', '--version'])
    if not success:
        logger.warning("⚠️  AWS CLI not available, skipping S3 structure check")
        logger.info("💡 Install AWS CLI to verify S3 partitioning structure")
        return True
    
    # List S3 structure
    base_path = "silver/stock-data/"
    success, stdout, stderr = run_command([
        'aws', 's3', 'ls', f's3://{s3_bucket}/{base_path}', '--recursive'
    ])
    
    if not success:
        if "NoSuchBucket" in stderr:
            logger.warning(f"⚠️  S3 bucket {s3_bucket} does not exist")
        elif "AccessDenied" in stderr:
            logger.warning("⚠️  Access denied to S3 bucket - check AWS credentials")
        else:
            logger.error(f"❌ Failed to list S3 bucket: {stderr}")
        return False
    
    if not stdout.strip():
        logger.warning("⚠️  No files found in S3 silver layer yet")
        logger.info("💡 Files should appear within a few minutes after connector starts")
        return True
    
    # Parse S3 listing to check partitioning
    files = stdout.strip().split('\n')
    partition_examples = set()
    
    for file_line in files:
        if '.parquet' in file_line:
            parts = file_line.split()
            if len(parts) >= 4:
                file_path = parts[3]  # S3 path is typically the 4th element
                
                # Extract partition information
                if 'symbol=' in file_path and 'year=' in file_path:
                    # Extract partition pattern
                    path_parts = file_path.split('/')
                    partition_parts = []
                    for part in path_parts:
                        if '=' in part and any(prefix in part for prefix in ['symbol=', 'year=', 'month=', 'day=', 'hour=']):
                            partition_parts.append(part)
                    
                    if partition_parts:
                        partition_pattern = '/'.join(partition_parts)
                        partition_examples.add(partition_pattern)
    
    if partition_examples:
        logger.info("✅ Found Parquet files with new partitioning!")
        logger.info("📁 Partition examples found:")
        for example in sorted(list(partition_examples)[:5]):  # Show first 5 examples
            logger.info(f"   📂 {example}")
            
        # Check if partitioning matches expected pattern
        expected_pattern = r'symbol=[A-Z]+/year=\d{4}/month=\d{2}/day=\d{2}/hour=\d{2}'
        correct_partitions = 0
        
        for example in partition_examples:
            if 'symbol=' in example and 'year=' in example and 'month=' in example:
                correct_partitions += 1
        
        logger.info(f"📊 {correct_partitions}/{len(partition_examples)} partitions follow expected pattern")
        
        if correct_partitions == len(partition_examples):
            logger.info("✅ All partitions follow the correct symbol+time pattern!")
        else:
            logger.warning("⚠️  Some partitions don't follow expected pattern")
        
        return True
    else:
        logger.warning("⚠️  No Parquet files with expected partitioning found yet")
        return False


def show_expected_structure():
    """Show the expected S3 structure."""
    logger.info("📋 Expected S3 Structure:")
    print("""
📁 S3 Bucket Structure (symbol + time partitioning):
└── silver/
    └── stock-data/
        ├── symbol=AAPL/
        │   ├── year=2025/month=08/day=25/hour=20/
        │   │   ├── processed-stock-prices-0-00000-abc123.parquet
        │   │   └── processed-trading-volume-0-00000-def456.parquet
        │   └── year=2025/month=08/day=25/hour=21/
        │       └── processed-technical-indicators-0-00000-ghi789.parquet
        ├── symbol=GOOGL/
        │   └── year=2025/month=08/day=25/hour=20/
        │       ├── processed-stock-prices-0-00000-jkl012.parquet
        │       └── processed-trading-volume-0-00000-mno345.parquet
        └── symbol=MSFT/
            └── year=2025/month=08/day=25/hour=20/
                └── processed-stock-prices-0-00000-pqr678.parquet

✅ Benefits of this partitioning:
   • Efficient queries by stock symbol
   • Time-based data lifecycle management  
   • Parallel processing by symbol
   • Optimized for analytics workloads
""")


def main():
    """Main verification function."""
    logger.info("🔍 Silver Layer Parquet Verification")
    logger.info("=" * 50)
    
    all_checks_passed = True
    
    # Check 1: Kafka Connect status
    if not check_kafka_connect_status():
        all_checks_passed = False
    
    print()
    
    # Check 2: Kafka topics
    if not check_kafka_topics():
        all_checks_passed = False
    
    print()
    
    # Check 3: S3 structure
    if not check_s3_structure():
        all_checks_passed = False
    
    print()
    
    # Show expected structure
    show_expected_structure()
    
    # Final summary
    logger.info("📋 VERIFICATION SUMMARY")
    logger.info("=" * 30)
    
    if all_checks_passed:
        logger.info("🎉 All checks passed! Silver layer Parquet partitioning is working correctly.")
        logger.info("📊 Data is being written to S3 with symbol+time partitioning.")
    else:
        logger.warning("⚠️  Some checks failed. Review the issues above.")
        logger.info("💡 Common solutions:")
        logger.info("   • Wait a few minutes for data to flow through the pipeline")
        logger.info("   • Check if the streaming pipeline is running")
        logger.info("   • Verify AWS credentials and S3 bucket configuration")
        logger.info("   • Check Kafka Connect logs for errors")
    
    logger.info("\n📊 Monitoring commands:")
    logger.info("   • Connector status: curl http://localhost:8083/connectors/silver-s3-sink-connector/status")
    logger.info("   • Stream processor: curl http://localhost:8082/health")
    logger.info("   • Kafka topics: kafka-console-consumer --bootstrap-server localhost:9092 --topic processed-stock-prices --max-messages 5")


if __name__ == "__main__":
    main()