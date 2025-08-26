#!/usr/bin/env python3
"""
Quick validation script to verify Spark Java compatibility fix
"""

import sys
import subprocess
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def validate_requirements_update():
    """Validate that requirements.txt has been updated to Spark 3.5.1"""
    logger.info("Validating requirements.txt update...")
    
    try:
        with open("requirements-streaming.txt", "r") as f:
            content = f.read()
        
        if "pyspark==3.5.1" in content:
            logger.info("✅ requirements-streaming.txt updated to Spark 3.5.1")
            return True
        else:
            logger.error("❌ requirements-streaming.txt not updated to Spark 3.5.1")
            return False
    except Exception as e:
        logger.error(f"❌ Error reading requirements-streaming.txt: {e}")
        return False


def validate_dockerfile_update():
    """Validate that Dockerfile has been updated to Spark 3.5.1"""
    logger.info("Validating Dockerfile update...")
    
    try:
        with open("Dockerfile.streaming-processor", "r") as f:
            content = f.read()
        
        if "SPARK_VERSION=3.5.1" in content:
            logger.info("✅ Dockerfile.streaming-processor updated to Spark 3.5.1")
            return True
        else:
            logger.error("❌ Dockerfile.streaming-processor not updated to Spark 3.5.1")
            return False
    except Exception as e:
        logger.error(f"❌ Error reading Dockerfile.streaming-processor: {e}")
        return False


def validate_java_compatibility_config():
    """Validate Java compatibility configuration in stream processor"""
    logger.info("Validating Java compatibility configuration...")
    
    try:
        with open("src/streaming_pipeline/processors/stream_processor.py", "r") as f:
            content = f.read()
        
        if "--add-opens=java.base/java.nio=ALL-UNNAMED" in content:
            logger.info("✅ Java compatibility options configured in stream processor")
            return True
        else:
            logger.error("❌ Java compatibility options not found in stream processor")
            return False
    except Exception as e:
        logger.error(f"❌ Error reading stream_processor.py: {e}")
        return False


def check_docker_images():
    """Check if Docker images need to be rebuilt"""
    logger.info("Checking Docker image status...")
    
    try:
        # Check if streaming-processor image exists
        result = subprocess.run(
            ['docker', 'images', 'streaming-processor', '--format', '{{.Repository}}:{{.Tag}}'],
            capture_output=True, text=True
        )
        
        if result.returncode == 0 and result.stdout.strip():
            logger.info("✅ streaming-processor Docker image exists")
            logger.info("ℹ️  Recommend rebuilding with: docker build -f Dockerfile.streaming-processor -t streaming-processor:latest .")
            return True
        else:
            logger.info("ℹ️  streaming-processor Docker image not found - will be built on first run")
            return True
    except Exception as e:
        logger.warning(f"Could not check Docker images: {e}")
        return True


def main():
    """Run validation checks"""
    logger.info("🔍 Validating Spark Java Compatibility Fix")
    logger.info("="*50)
    
    checks = [
        ("Requirements Update", validate_requirements_update),
        ("Dockerfile Update", validate_dockerfile_update),
        ("Java Compatibility Config", validate_java_compatibility_config),
        ("Docker Images", check_docker_images)
    ]
    
    results = {}
    for check_name, check_func in checks:
        logger.info(f"\n🔍 {check_name}...")
        results[check_name] = check_func()
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("VALIDATION RESULTS")
    logger.info("="*50)
    
    all_passed = True
    for check_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{check_name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        logger.info("\n🎉 ALL VALIDATIONS PASSED!")
        logger.info("\n🚀 Ready to fix the Java compatibility issue!")
        logger.info("\nNext steps:")
        logger.info("1. Run: python3 fix_spark_java_compatibility.py")
        logger.info("2. Or manually rebuild: docker build -f Dockerfile.streaming-processor -t streaming-processor:latest .")
        logger.info("3. Test: docker-compose up -d")
        
        return 0
    else:
        logger.error("\n❌ Some validations failed!")
        logger.error("Please check the issues above before proceeding.")
        return 1


if __name__ == "__main__":
    sys.exit(main())