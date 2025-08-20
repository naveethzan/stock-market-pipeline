# Implementation Plan - Project Cleanup

## Phase 1: Analysis and Preparation

- [-] 1. Analyze current codebase and create backup
  - Create git branch for cleanup work to preserve current state
  - Document current streaming pipeline functionality and dependencies
  - Identify shared components between batch and streaming code
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [ ] 2. Validate current streaming functionality
  - Test Alpha Vantage API integration and data retrieval
  - Verify Kafka producer/consumer functionality works
  - Confirm Spark Structured Streaming processes data correctly
  - Test Snowflake data loading and dimensional model creation
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

## Phase 2: Remove Batch Processing Components

- [ ] 3. Remove batch processing source code
  - Delete entire `src/batch/` directory with all batch ETL jobs
  - Remove `src/airflow/` directory with DAG source code
  - Clean up any batch-specific utilities in `src/` directory
  - _Requirements: 1.1, 1.2_

- [ ] 4. Remove Airflow infrastructure
  - Delete `airflow/` directory with DAGs, logs, and configuration
  - Remove `Dockerfile.airflow` container definition
  - Clean up Airflow-related environment variables and configs
  - _Requirements: 1.1, 1.2_

- [ ] 5. Clean up batch-specific Docker and deployment files
  - Remove batch services from `docker-compose.yaml`
  - Keep `docker-compose.streaming.yaml` as the primary compose file
  - Remove batch-specific Dockerfiles and build scripts
  - Update Makefiles to remove batch processing targets
  - _Requirements: 2.1, 2.4_

## Phase 3: Consolidate Streaming Components

- [ ] 6. Merge duplicate Kafka implementations
  - Compare `src/kafka/` and `src/streaming_pipeline/` implementations
  - Merge best features from `src/kafka/producers/` into `src/streaming_pipeline/producers/`
  - Preserve Avro serialization and error handling from both implementations
  - Remove `src/kafka/` directory after successful merge
  - _Requirements: 3.1, 3.2_

- [ ] 7. Consolidate configuration files
  - Keep `config/.env.streaming.template` as primary environment template
  - Merge streaming-specific variables from other environment files
  - Remove batch-specific configuration files and templates
  - Update `requirements-streaming.txt` and remove `requirements.txt`
  - _Requirements: 3.3, 2.3_

- [ ] 8. Update import statements and dependencies
  - Fix import statements after removing `src/kafka/` directory
  - Update all references to point to consolidated `src/streaming_pipeline/`
  - Remove batch processing dependencies from requirements files
  - Test that all imports resolve correctly
  - _Requirements: 3.1, 1.3_

## Phase 4: Clean Up Data and Cache Files

- [ ] 9. Remove Kafka and Zookeeper data files
  - Delete `data/kafka/` directory with all log files and partitions
  - Delete `data/zookeeper/` directory with cluster data
  - Keep `data/spark/` directory structure but remove any batch-related files
  - _Requirements: 4.1, 4.2_

- [ ] 10. Clean up Python cache and build artifacts
  - Remove all `__pycache__/` directories throughout the project
  - Delete `.pytest_cache/` directory
  - Remove any `.pyc` files and other Python build artifacts
  - Clean up any IDE-specific files like `.DS_Store`
  - _Requirements: 4.3_

- [ ] 11. Update .gitignore for proper exclusions
  - Add patterns to ignore Kafka data files
  - Add patterns to ignore Python cache directories
  - Add patterns to ignore IDE and OS-specific files
  - Remove batch-specific ignore patterns that are no longer needed
  - _Requirements: 4.4_

## Phase 5: Update Documentation and Scripts

- [ ] 12. Update project documentation
  - Remove batch processing sections from all README files
  - Update `docs/streaming_pipeline_setup.md` to reflect cleaned architecture
  - Update `docs/docker_setup.md` to use streaming-only compose file
  - Remove references to Airflow and batch processing from all documentation
  - _Requirements: 5.1, 5.2, 5.5_

- [ ] 13. Clean up deployment and management scripts
  - Remove batch-specific deployment scripts
  - Keep streaming-focused scripts like `kafka-connect-manager.py`
  - Update `setup-docker.sh` to work with streaming-only architecture
  - Remove or update scripts that reference removed components
  - _Requirements: 2.3, 5.4_

- [ ] 14. Update configuration templates and examples
  - Clean up `config/streaming_pipeline.env.template` to remove batch variables
  - Update Kafka Connect connector configurations to focus on streaming
  - Remove batch-specific Snowflake setup scripts
  - Keep dimensional modeling and warehouse setup for streaming
  - _Requirements: 5.3, 2.3_

## Phase 6: Final Validation and Testing

- [ ] 15. Validate Docker deployment
  - Test that `docker-compose.streaming.yaml` starts all services successfully
  - Verify all streaming services can connect and communicate
  - Test that environment variables are properly configured
  - Confirm that all required ports and networks are accessible
  - _Requirements: 7.3, 7.4_

- [ ] 16. Test end-to-end streaming pipeline
  - Verify Alpha Vantage API data flows through to Kafka topics
  - Test Spark Structured Streaming processes data and publishes to output topics
  - Confirm Kafka Connect delivers data to S3 and Snowflake correctly
  - Validate dimensional model data appears correctly in Snowflake
  - _Requirements: 7.1, 7.2_

- [ ] 17. Performance and functionality validation
  - Run streaming pipeline for extended period to test stability
  - Monitor resource usage and performance metrics
  - Test error handling and recovery scenarios
  - Validate that all monitoring and logging functionality works
  - _Requirements: 7.1, 7.2, 7.5_

- [ ] 18. Final cleanup and documentation review
  - Review all remaining files to ensure no batch processing remnants
  - Update any remaining documentation that references removed components
  - Create summary of changes made and components removed
  - Verify project structure matches the target architecture design
  - _Requirements: 5.1, 5.2, 5.5_