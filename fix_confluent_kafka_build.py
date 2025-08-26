#!/usr/bin/env python3
"""
Confluent Kafka Build Fix Script

This script fixes the confluent-kafka build error by ensuring proper
librdkafka dependencies are installed in Docker containers.
"""

import os
import sys
import subprocess
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_docker_available():
    """Check if Docker is available"""
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"Docker available: {result.stdout.strip()}")
            return True
        else:
            logger.error("Docker not available")
            return False
    except Exception as e:
        logger.error(f"Error checking Docker: {e}")
        return False


def clean_docker_cache():
    """Clean Docker build cache to ensure fresh builds"""
    logger.info("Cleaning Docker build cache...")
    
    try:
        # Remove dangling images
        subprocess.run(['docker', 'image', 'prune', '-f'], capture_output=True)
        
        # Remove build cache
        subprocess.run(['docker', 'builder', 'prune', '-f'], capture_output=True)
        
        logger.info("✅ Docker cache cleaned")
        return True
    except Exception as e:
        logger.warning(f"Could not clean Docker cache: {e}")
        return True  # Not critical


def build_producer_image():
    """Build the producer image with fixed confluent-kafka dependencies"""
    logger.info("Building producer image with fixed confluent-kafka dependencies...")
    
    try:
        cmd = [
            'docker', 'build',
            '-f', 'Dockerfile.streaming-producer',
            '-t', 'streaming-producer:latest',
            '.'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("✅ Producer image built successfully")
            return True
        else:
            logger.error(f"❌ Producer build failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error building producer image: {e}")
        return False


def build_processor_image():
    """Build the processor image with fixed confluent-kafka dependencies"""
    logger.info("Building processor image with fixed confluent-kafka dependencies...")
    
    try:
        cmd = [
            'docker', 'build',
            '-f', 'Dockerfile.streaming-processor',
            '-t', 'streaming-processor:latest',
            '.'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("✅ Processor image built successfully")
            return True
        else:
            logger.error(f"❌ Processor build failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error building processor image: {e}")
        return False


def test_confluent_kafka_import():
    """Test that confluent-kafka can be imported in both images"""
    logger.info("Testing confluent-kafka import in Docker images...")
    
    test_script = '''
import sys
try:
    from confluent_kafka import Producer, Consumer
    print("✅ confluent-kafka imported successfully")
    print(f"✅ Producer and Consumer classes available")
    sys.exit(0)
except ImportError as e:
    print(f"❌ confluent-kafka import failed: {e}")
    sys.exit(1)
'''
    
    # Test producer image
    logger.info("Testing producer image...")
    try:
        cmd = [
            'docker', 'run', '--rm',
            'streaming-producer:latest',
            'python3', '-c', test_script
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            logger.info("✅ Producer image: confluent-kafka working")
            producer_ok = True
        else:
            logger.error(f"❌ Producer image: confluent-kafka failed - {result.stderr}")
            producer_ok = False
    except Exception as e:
        logger.error(f"❌ Error testing producer image: {e}")
        producer_ok = False
    
    # Test processor image
    logger.info("Testing processor image...")
    try:
        cmd = [
            'docker', 'run', '--rm',
            'streaming-processor:latest',
            'python3', '-c', test_script
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            logger.info("✅ Processor image: confluent-kafka working")
            processor_ok = True
        else:
            logger.error(f"❌ Processor image: confluent-kafka failed - {result.stderr}")
            processor_ok = False
    except Exception as e:
        logger.error(f"❌ Error testing processor image: {e}")
        processor_ok = False
    
    return producer_ok and processor_ok


def main():
    """Main function to fix confluent-kafka build issues"""
    logger.info("🔧 Fixing Confluent Kafka Build Issues")
    logger.info("="*50)
    
    # Step 1: Check Docker availability
    if not check_docker_available():
        logger.error("❌ Docker is not available")
        return 1
    
    # Step 2: Clean Docker cache
    logger.info("\n🧹 Step 1: Cleaning Docker cache...")
    clean_docker_cache()
    
    # Step 3: Build producer image
    logger.info("\n📦 Step 2: Building producer image...")
    if not build_producer_image():
        logger.error("❌ Failed to build producer image")
        return 1
    
    # Step 4: Build processor image
    logger.info("\n📦 Step 3: Building processor image...")
    if not build_processor_image():
        logger.error("❌ Failed to build processor image")
        return 1
    
    # Step 5: Test confluent-kafka imports
    logger.info("\n🧪 Step 4: Testing confluent-kafka imports...")
    if not test_confluent_kafka_import():
        logger.error("❌ confluent-kafka import tests failed")
        return 1
    
    # Success summary
    logger.info("\n" + "="*50)
    logger.info("🎉 CONFLUENT KAFKA BUILD FIX COMPLETED!")
    logger.info("="*50)
    logger.info("✅ Enhanced Dockerfiles with proper librdkafka dependencies")
    logger.info("✅ Producer image built successfully")
    logger.info("✅ Processor image built successfully")
    logger.info("✅ confluent-kafka imports working in both images")
    
    logger.info("\n🚀 Next Steps:")
    logger.info("1. Run: docker-compose up -d")
    logger.info("2. Monitor: docker-compose logs -f")
    logger.info("3. Your streaming pipeline should now build and run correctly!")
    
    logger.info("\n🎯 The confluent-kafka build issue has been resolved!")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())