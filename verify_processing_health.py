#!/usr/bin/env python3
"""
Simple script to verify processing layer health using HTTP endpoints.
This bypasses the dependency issues and directly checks service health.
"""
import requests
import json
import subprocess
import sys
from datetime import datetime
from typing import Dict, Any

def get_service_health(service_name: str, port: int) -> Dict[str, Any]:
    """Get health information from a service endpoint."""
    try:
        url = f"http://localhost:{port}/health"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            return {
                "status": "healthy",
                "service": service_name,
                "data": response.json()
            }
        else:
            return {
                "status": "unhealthy",
                "service": service_name,
                "error": f"HTTP {response.status_code}: {response.text}"
            }
    except Exception as e:
        return {
            "status": "error",
            "service": service_name,
            "error": str(e)
        }

def get_kafka_topic_info() -> Dict[str, Any]:
    """Get Kafka topic information."""
    try:
        # Get topic list
        topics_result = subprocess.run([
            'docker', 'exec', 'kafka', 'kafka-topics', 
            '--list', '--bootstrap-server', 'localhost:9092'
        ], capture_output=True, text=True, timeout=30)
        
        if topics_result.returncode != 0:
            return {"status": "error", "error": topics_result.stderr}
        
        topics = [t.strip() for t in topics_result.stdout.split('\n') if t.strip()]
        
        # Get message counts for key topics
        key_topics = [
            'stock-quotes-realtime',
            'processed-stock-prices', 
            'processed-trading-volume',
            'processed-technical-indicators'
        ]
        
        topic_info = {}
        for topic in key_topics:
            if topic in topics:
                offset_result = subprocess.run([
                    'docker', 'exec', 'kafka', 'kafka-run-class',
                    'kafka.tools.GetOffsetShell', '--broker-list', 'localhost:9092',
                    '--topic', topic
                ], capture_output=True, text=True, timeout=10)
                
                if offset_result.returncode == 0:
                    offset_line = offset_result.stdout.strip()
                    if ':' in offset_line:
                        parts = offset_line.split(':')
                        if len(parts) >= 3:
                            offset = int(parts[-1])
                            topic_info[topic] = {
                                "partition": parts[1],
                                "messages": offset
                            }
        
        return {
            "status": "healthy",
            "all_topics": topics,
            "key_topic_info": topic_info
        }
        
    except Exception as e:
        return {"status": "error", "error": str(e)}

def check_docker_services() -> Dict[str, Any]:
    """Check Docker service status."""
    try:
        result = subprocess.run([
            'docker-compose', 'ps'
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            return {"status": "error", "error": result.stderr}
        
        lines = result.stdout.split('\n')
        services = {}
        
        for line in lines[2:]:  # Skip header lines
            if line.strip():
                parts = line.split()
                if len(parts) >= 7:
                    service_name = parts[3]
                    status = parts[5]
                    services[service_name] = status
        
        return {
            "status": "healthy",
            "services": services
        }
        
    except Exception as e:
        return {"status": "error", "error": str(e)}

def print_summary(results: Dict[str, Any]):
    """Print a comprehensive summary of processing layer health."""
    print("\n" + "="*80)
    print("🔍 PROCESSING LAYER HEALTH VERIFICATION")
    print("="*80)
    print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Docker Services Status
    print(f"\n📦 DOCKER SERVICES:")
    docker_info = results.get('docker', {})
    if docker_info.get('status') == 'healthy':
        services = docker_info.get('services', {})
        for service, status in services.items():
            status_icon = "🟢" if "Up" in status else "🔴"
            print(f"   {status_icon} {service}: {status}")
    else:
        print(f"   ❌ Error: {docker_info.get('error', 'Unknown error')}")
    
    # Producer Health
    print(f"\n📊 DATA PRODUCER:")
    producer_info = results.get('producer', {})
    if producer_info.get('status') == 'healthy':
        data = producer_info.get('data', {})
        metrics = data.get('metrics', {})
        messages = metrics.get('messages', {})
        
        print(f"   🟢 Status: {data.get('status', 'Unknown')}")
        print(f"   📈 Messages Sent: {messages.get('sent', 0)}")
        print(f"   ✅ Success Rate: {messages.get('success_rate', 0):.1%}")
        print(f"   📊 Messages/sec: {metrics.get('throughput', {}).get('messages_per_second', 0):.2f}")
    else:
        print(f"   ❌ Status: {producer_info.get('status', 'Unknown')}")
        print(f"   🔧 Error: {producer_info.get('error', 'Unknown error')}")
    
    # Processor Health  
    print(f"\n⚡ STREAM PROCESSOR:")
    processor_info = results.get('processor', {})
    if processor_info.get('status') == 'healthy':
        data = processor_info.get('data', {})
        
        print(f"   🟢 Status: {data.get('status', 'Unknown')}")
        print(f"   🔄 Active Queries: {data.get('active_queries', 0)}")
        print(f"   ✅ Healthy Queries: {data.get('healthy_queries', 0)}")
        print(f"   ⏱️ Uptime: {data.get('uptime_seconds', 0):.1f} seconds")
        
        # Query details
        query_statuses = data.get('query_statuses', {})
        if query_statuses:
            print(f"\n   📋 ACTIVE QUERIES:")
            for query_name, query_info in query_statuses.items():
                is_active = query_info.get('is_active', False)
                batch_id = query_info.get('batch_id', 0)
                input_rate = query_info.get('input_rows_per_second', 0)
                
                status_icon = "🟢" if is_active else "🔴"
                print(f"      {status_icon} {query_name}")
                print(f"         Batch: {batch_id}, Input Rate: {input_rate:.1f} rows/sec")
    else:
        print(f"   ❌ Status: {processor_info.get('status', 'Unknown')}")
        print(f"   🔧 Error: {processor_info.get('error', 'Unknown error')}")
    
    # Kafka Topics
    print(f"\n🚀 KAFKA TOPICS:")
    kafka_info = results.get('kafka', {})
    if kafka_info.get('status') == 'healthy':
        topic_info = kafka_info.get('key_topic_info', {})
        
        print(f"   📊 KEY TOPIC MESSAGE COUNTS:")
        for topic, info in topic_info.items():
            messages = info.get('messages', 0)
            print(f"      📈 {topic}: {messages:,} messages")
        
        # Calculate processing ratios
        input_messages = topic_info.get('stock-quotes-realtime', {}).get('messages', 0)
        processed_prices = topic_info.get('processed-stock-prices', {}).get('messages', 0)
        processed_volume = topic_info.get('processed-trading-volume', {}).get('messages', 0)
        processed_indicators = topic_info.get('processed-technical-indicators', {}).get('messages', 0)
        
        if input_messages > 0:
            print(f"\n   🔄 PROCESSING RATIOS:")
            print(f"      📊 Stock Prices: {processed_prices/input_messages*100:.1f}% processed")
            print(f"      📊 Trading Volume: {processed_volume/input_messages*100:.1f}% processed")  
            print(f"      📊 Technical Indicators: {processed_indicators/input_messages*100:.1f}% processed")
    else:
        print(f"   ❌ Kafka Error: {kafka_info.get('error', 'Unknown error')}")
    
    # Overall Assessment
    print(f"\n🩺 OVERALL ASSESSMENT:")
    
    all_healthy = all([
        results.get('producer', {}).get('status') == 'healthy',
        results.get('processor', {}).get('status') == 'healthy',
        results.get('kafka', {}).get('status') == 'healthy'
    ])
    
    if all_healthy:
        print("   ✅ Processing layer is HEALTHY and functioning correctly!")
        
        # Check if data is flowing
        kafka_info = results.get('kafka', {})
        topic_info = kafka_info.get('key_topic_info', {})
        processed_messages = sum([
            info.get('messages', 0) for topic, info in topic_info.items() 
            if 'processed-' in topic
        ])
        
        if processed_messages > 0:
            print("   ✅ Data is flowing through the processing pipeline")
        else:
            print("   ⚠️ No processed messages detected - may need input data")
    else:
        failed_services = []
        if results.get('producer', {}).get('status') != 'healthy':
            failed_services.append('Producer')
        if results.get('processor', {}).get('status') != 'healthy':
            failed_services.append('Stream Processor')
        if results.get('kafka', {}).get('status') != 'healthy':
            failed_services.append('Kafka')
        
        print(f"   ❌ Issues detected with: {', '.join(failed_services)}")
        print("   🔧 Check the detailed errors above for troubleshooting")
    
    print("="*80)

def main():
    """Main verification function."""
    print("🔍 Verifying processing layer health...")
    
    results = {}
    
    # Check Docker services
    print("📦 Checking Docker services...")
    results['docker'] = check_docker_services()
    
    # Check producer health
    print("📊 Checking data producer...")
    results['producer'] = get_service_health('streaming-producer', 8081)
    
    # Check processor health  
    print("⚡ Checking stream processor...")
    results['processor'] = get_service_health('streaming-processor', 8082)
    
    # Check Kafka topics
    print("🚀 Checking Kafka topics...")
    results['kafka'] = get_kafka_topic_info()
    
    # Print comprehensive summary
    print_summary(results)
    
    # Output JSON if requested
    if len(sys.argv) > 1 and sys.argv[1] == '--json':
        print("\n📄 JSON OUTPUT:")
        print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()