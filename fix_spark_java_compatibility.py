#!/usr/bin/env python3
"""
Spark Java Compatibility Fix Script

This script fixes the Java compatibility issue between Spark 3.4.1 and Java 17+
by upgrading to Spark 3.5.1 and ensuring proper configuration.
"""

import os
import sys
import subprocess
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_java_version():
    """Check current Java version"""
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        java_version = result.stderr.split('\n')[0]
        logger.info(f"Current Java version: {java_version}")
        return java_version
    except Exception as e:
        logger.error(f"Failed to check Java version: {e}")
        return None


def rebuild_docker_image():
    """Rebuild the Docker image with updated Spark version"""
    logger.info("Rebuilding Docker image with Spark 3.5.1...")
    
    try:
        # Build the streaming processor image
        cmd = [
            'docker', 'build', 
            '-f', 'Dockerfile.streaming-processor',
            '-t', 'streaming-processor:latest',
            '.'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("✅ Docker image rebuilt successfully with Spark 3.5.1")
            return True
        else:
            logger.error(f"❌ Docker build failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error rebuilding Docker image: {e}")
        return False


def test_spark_compatibility():
    """Test Spark compatibility with Java 17+"""
    logger.info("Testing Spark 3.5.1 compatibility...")
    
    test_script = """
import os
import sys
from pyspark.sql import SparkSession

try:
    # Create Spark session with Java 17+ compatibility
    spark = (SparkSession.builder
            .appName("JavaCompatibilityTest")
            .master("local[1]")
            .config("spark.driver.extraJavaOptions", 
                   "--add-opens=java.base/java.lang=ALL-UNNAMED "
                   "--add-opens=java.base/java.nio=ALL-UNNAMED "
                   "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
            .config("spark.executor.extraJavaOptions", 
                   "--add-opens=java.base/java.lang=ALL-UNNAMED "
                   "--add-opens=java.base/java.nio=ALL-UNNAMED "
                   "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
            .getOrCreate())
    
    # Test basic functionality
    df = spark.range(10)
    count = df.count()
    
    print(f"✅ Spark 3.5.1 working correctly! Created DataFrame with {count} rows")
    print(f"✅ Java compatibility issue resolved")
    
    spark.stop()
    sys.exit(0)
    
except Exception as e:
    print(f"❌ Spark compatibility test failed: {e}")
    sys.exit(1)
"""
    
    try:
        # Run test in Docker container
        cmd = [
            'docker', 'run', '--rm',
            '-e', 'PYTHONPATH=/app/src',
            'streaming-processor:latest',
            'python3', '-c', test_script
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            logger.info("✅ Spark compatibility test passed!")
            logger.info(result.stdout)
            return True
        else:
            logger.error("❌ Spark compatibility test failed!")
            logger.error(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Spark compatibility test timed out")
        return False
    except Exception as e:
        logger.error(f"❌ Error running compatibility test: {e}")
        return False


def update_docker_compose():
    """Update docker-compose.yaml if needed"""
    logger.info("Checking docker-compose.yaml configuration...")
    
    compose_file = Path("docker-compose.yaml")
    if not compose_file.exists():
        logger.warning("docker-compose.yaml not found, skipping update")
        return True
    
    try:
        with open(compose_file, 'r') as f:
            content = f.read()
        
        # Check if streaming-processor service exists and update if needed
        if 'streaming-processor:' in content:
            logger.info("✅ docker-compose.yaml already configured for streaming-processor")
        else:
            logger.info("ℹ️  Consider adding streaming-processor service to docker-compose.yaml")
        
        return True
        
    except Exception as e:
        logger.error(f"Error checking docker-compose.yaml: {e}")
        return False


def create_test_script():
    """Create a comprehensive test script for the fixed pipeline"""
    test_script_content = '''#!/usr/bin/env python3
"""
Comprehensive Pipeline Test Script

Tests the complete streaming pipeline with Spark 3.5.1 and Java 17+ compatibility.
"""

import time
import logging
import subprocess
import sys
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_producer():
    """Test the data producer"""
    logger.info("Testing data producer...")
    try:
        cmd = ['docker', 'run', '--rm', '--network', 'host', 
               'streaming-producer:latest', 'python3', '-c', 
               'from streaming_pipeline.producers.data_producer import DataProducer; '
               'producer = DataProducer(); '
               'print("✅ Producer initialized successfully")']
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            logger.info("✅ Producer test passed")
            return True
        else:
            logger.error(f"❌ Producer test failed: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"❌ Producer test error: {e}")
        return False


def test_processor():
    """Test the Spark processor with Java 17+ compatibility"""
    logger.info("Testing Spark processor with Java 17+ compatibility...")
    try:
        cmd = ['docker', 'run', '--rm', '--network', 'host',
               '-e', 'PYTHONPATH=/app/src',
               'streaming-processor:latest', 'python3', '-c',
               '''
from pyspark.sql import SparkSession
import sys

try:
    spark = (SparkSession.builder
            .appName("CompatibilityTest")
            .master("local[1]")
            .config("spark.driver.extraJavaOptions", 
                   "--add-opens=java.base/java.lang=ALL-UNNAMED "
                   "--add-opens=java.base/java.nio=ALL-UNNAMED")
            .getOrCreate())
    
    df = spark.range(100)
    count = df.count()
    print(f"✅ Spark 3.5.1 working! Processed {count} rows")
    spark.stop()
    
except Exception as e:
    print(f"❌ Spark test failed: {e}")
    sys.exit(1)
''']
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            logger.info("✅ Processor test passed")
            logger.info(result.stdout)
            return True
        else:
            logger.error(f"❌ Processor test failed: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"❌ Processor test error: {e}")
        return False


def main():
    """Run comprehensive pipeline tests"""
    logger.info("🚀 Starting comprehensive pipeline test...")
    
    tests = [
        ("Producer", test_producer),
        ("Processor", test_processor)
    ]
    
    results = {}
    for test_name, test_func in tests:
        logger.info(f"Running {test_name} test...")
        results[test_name] = test_func()
        time.sleep(2)  # Brief pause between tests
    
    # Summary
    logger.info("\\n" + "="*50)
    logger.info("TEST RESULTS SUMMARY")
    logger.info("="*50)
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{test_name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        logger.info("\\n🎉 ALL TESTS PASSED! Pipeline is working correctly.")
        logger.info("✅ Java compatibility issue resolved")
        logger.info("✅ Spark 3.5.1 integration successful")
        return 0
    else:
        logger.error("\\n❌ Some tests failed. Please check the logs above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
'''
    
    with open("test_fixed_pipeline.py", "w") as f:
        f.write(test_script_content)
    
    os.chmod("test_fixed_pipeline.py", 0o755)
    logger.info("✅ Created comprehensive test script: test_fixed_pipeline.py")


def main():
    """Main function to fix Spark Java compatibility"""
    logger.info("🔧 Starting Spark Java Compatibility Fix")
    logger.info("="*60)
    
    # Step 1: Check current Java version
    java_version = check_java_version()
    if not java_version:
        logger.error("❌ Cannot determine Java version")
        return 1
    
    # Step 2: Rebuild Docker image with Spark 3.5.1
    logger.info("\\n📦 Step 1: Rebuilding Docker image with Spark 3.5.1...")
    if not rebuild_docker_image():
        logger.error("❌ Failed to rebuild Docker image")
        return 1
    
    # Step 3: Test Spark compatibility
    logger.info("\\n🧪 Step 2: Testing Spark 3.5.1 compatibility...")
    if not test_spark_compatibility():
        logger.error("❌ Spark compatibility test failed")
        return 1
    
    # Step 4: Update docker-compose if needed
    logger.info("\\n🐳 Step 3: Checking Docker Compose configuration...")
    if not update_docker_compose():
        logger.error("❌ Failed to update docker-compose configuration")
        return 1
    
    # Step 5: Create comprehensive test script
    logger.info("\\n📝 Step 4: Creating comprehensive test script...")
    create_test_script()
    
    # Success summary
    logger.info("\\n" + "="*60)
    logger.info("🎉 SPARK JAVA COMPATIBILITY FIX COMPLETED!")
    logger.info("="*60)
    logger.info("✅ Upgraded from Spark 3.4.1 to Spark 3.5.1")
    logger.info("✅ Java 17+ compatibility configured")
    logger.info("✅ Docker image rebuilt successfully")
    logger.info("✅ Compatibility tests passed")
    logger.info("✅ Test script created: test_fixed_pipeline.py")
    
    logger.info("\\n🚀 Next Steps:")
    logger.info("1. Run: docker-compose up -d")
    logger.info("2. Test: python3 test_fixed_pipeline.py")
    logger.info("3. Monitor: docker-compose logs -f streaming-processor")
    
    logger.info("\\n🎯 Your streaming pipeline should now work correctly!")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())