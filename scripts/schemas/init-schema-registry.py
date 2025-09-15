#!/usr/bin/env python3
"""
Schema Registry initialization script.
Registers all Avro schemas with the Schema Registry service.
"""
import sys
import time
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from streaming_pipeline.schemas.schema_registry_client import SchemaRegistryClient
from streaming_pipeline.schemas.avro_schemas import SCHEMA_REGISTRY_SUBJECTS


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def wait_for_schema_registry(client: SchemaRegistryClient, max_wait: int = 120) -> bool:
    """
    Wait for Schema Registry to become available.
    
    Args:
        client: Schema Registry client
        max_wait: Maximum wait time in seconds
        
    Returns:
        True if available, False if timeout
    """
    logger.info("Waiting for Schema Registry to become available...")
    
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            status = client.get_registry_status()
            if status["status"] == "healthy":
                logger.info("Schema Registry is available!")
                return True
        except Exception as e:
            logger.debug(f"Schema Registry not ready: {e}")
        
        time.sleep(5)
    
    logger.error(f"Schema Registry not available after {max_wait} seconds")
    return False


def register_schemas(schema_registry_url: str = "http://localhost:8085") -> bool:
    """
    Register all schemas with Schema Registry.
    
    Args:
        schema_registry_url: Schema Registry URL
        
    Returns:
        True if successful, False otherwise
    """
    logger.info("Starting schema registration process")
    
    try:
        with SchemaRegistryClient(schema_registry_url) as client:
            # Wait for Schema Registry to be available
            if not wait_for_schema_registry(client):
                return False
            
            # Register all schemas
            results = client.register_all_schemas()
            
            if results:
                logger.info("Schema registration completed successfully")
                logger.info(f"Registered schemas: {list(results.keys())}")
                
                # Print schema IDs
                for subject, schema_id in results.items():
                    logger.info(f"  {subject}: ID {schema_id}")
                
                return True
            else:
                logger.error("No schemas were registered")
                return False
                
    except Exception as e:
        logger.error(f"Schema registration failed: {e}", exc_info=True)
        return False


def verify_schemas(schema_registry_url: str = "http://localhost:8085") -> bool:
    """
    Verify that all schemas are properly registered.
    
    Args:
        schema_registry_url: Schema Registry URL
        
    Returns:
        True if all schemas verified, False otherwise
    """
    logger.info("Verifying schema registration")
    
    try:
        with SchemaRegistryClient(schema_registry_url) as client:
            subjects = client.list_subjects()
            
            logger.info(f"Found {len(subjects)} subjects in registry")
            
            # Check each expected subject
            missing_subjects = []
            for expected_subject in SCHEMA_REGISTRY_SUBJECTS.keys():
                if expected_subject not in subjects:
                    missing_subjects.append(expected_subject)
                else:
                    # Get schema details
                    try:
                        schema_info = client.get_latest_schema(expected_subject)
                        logger.info(
                            f"✓ {expected_subject}: ID {schema_info['id']}, Version {schema_info['version']}"
                        )
                    except Exception as e:
                        logger.warning(f"Could not get details for {expected_subject}: {e}")
            
            if missing_subjects:
                logger.error(f"Missing subjects: {missing_subjects}")
                return False
            
            logger.info("All schemas verified successfully!")
            return True
            
    except Exception as e:
        logger.error(f"Schema verification failed: {e}", exc_info=True)
        return False


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Initialize Schema Registry with Avro schemas")
    parser.add_argument(
        "--schema-registry-url",
        default="http://localhost:8085",
        help="Schema Registry URL (default: http://localhost:8085)"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify existing schemas, don't register new ones"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("Schema Registry Initialization")
    logger.info(f"Schema Registry URL: {args.schema_registry_url}")
    
    if args.verify_only:
        success = verify_schemas(args.schema_registry_url)
    else:
        # Register schemas first
        success = register_schemas(args.schema_registry_url)
        
        # Then verify
        if success:
            success = verify_schemas(args.schema_registry_url)
    
    if success:
        logger.info("Schema Registry initialization completed successfully!")
        sys.exit(0)
    else:
        logger.error("Schema Registry initialization failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()