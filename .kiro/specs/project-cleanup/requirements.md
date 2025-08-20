# Project Cleanup Requirements Document

## Introduction

This cleanup initiative focuses on streamlining the stock market data pipeline project to concentrate solely on real-time streaming processing. The goal is to remove all batch processing components, unused infrastructure, and redundant code while preserving the core streaming pipeline functionality that processes stock market data through Kafka, Spark Structured Streaming, and Snowflake.

## Requirements

### Requirement 1

**User Story:** As a developer, I want to remove all batch processing components, so that the project focuses exclusively on streaming data processing.

#### Acceptance Criteria

1. WHEN reviewing the codebase THEN the system SHALL remove the entire `src/batch` directory and all batch processing code
2. WHEN cleaning up Airflow components THEN the system SHALL remove all Airflow DAGs, configurations, and related batch scheduling infrastructure
3. WHEN removing batch dependencies THEN the system SHALL clean up batch-specific requirements from requirements.txt files
4. WHEN updating documentation THEN the system SHALL remove references to batch processing workflows
5. IF any shared utilities exist between batch and streaming THEN the system SHALL preserve only streaming-relevant utilities

### Requirement 2

**User Story:** As a developer, I want to remove unused infrastructure components, so that the project deployment is simplified and focused.

#### Acceptance Criteria

1. WHEN cleaning Docker configurations THEN the system SHALL remove batch-specific Dockerfiles and docker-compose services
2. WHEN reviewing Kafka topics THEN the system SHALL remove batch-specific topics like `stock-data-batch`
3. WHEN cleaning up scripts THEN the system SHALL remove batch deployment and management scripts
4. WHEN updating Makefiles THEN the system SHALL remove batch processing targets and commands
5. IF infrastructure components serve both batch and streaming THEN the system SHALL preserve only streaming configurations

### Requirement 3

**User Story:** As a developer, I want to consolidate streaming components, so that the codebase is organized and maintainable.

#### Acceptance Criteria

1. WHEN reviewing streaming code THEN the system SHALL merge duplicate functionality between `src/kafka` and `src/streaming_pipeline`
2. WHEN organizing directories THEN the system SHALL maintain a single streaming pipeline structure under `src/streaming_pipeline`
3. WHEN cleaning up configurations THEN the system SHALL consolidate environment files to focus on streaming requirements
4. WHEN updating documentation THEN the system SHALL ensure all docs reflect the streaming-only architecture
5. IF redundant streaming components exist THEN the system SHALL keep the most complete and well-tested implementation

### Requirement 4

**User Story:** As a developer, I want to remove test data and logs, so that the repository is clean and lightweight.

#### Acceptance Criteria

1. WHEN cleaning up data directories THEN the system SHALL remove Kafka log files and test data
2. WHEN reviewing Airflow logs THEN the system SHALL remove all Airflow execution logs and temporary files
3. WHEN cleaning cache directories THEN the system SHALL remove Python cache files and build artifacts
4. WHEN updating .gitignore THEN the system SHALL ensure proper exclusion of generated files
5. IF any sample data is needed for testing THEN the system SHALL keep minimal, representative datasets only

### Requirement 5

**User Story:** As a developer, I want to update project documentation, so that it accurately reflects the streaming-only architecture.

#### Acceptance Criteria

1. WHEN updating README files THEN the system SHALL remove batch processing instructions and focus on streaming setup
2. WHEN reviewing architecture documentation THEN the system SHALL update diagrams to show streaming-only flow
3. WHEN cleaning up configuration templates THEN the system SHALL remove batch-specific environment variables
4. WHEN updating setup guides THEN the system SHALL simplify deployment to streaming components only
5. IF any documentation references batch processing THEN the system SHALL update or remove those references

### Requirement 6

**User Story:** As a developer, I want to preserve the core streaming functionality, so that the pipeline continues to work after cleanup.

#### Acceptance Criteria

1. WHEN removing components THEN the system SHALL preserve all Alpha Vantage API integration code
2. WHEN cleaning up Kafka components THEN the system SHALL maintain streaming topics and Kafka Connect configurations
3. WHEN reviewing Spark code THEN the system SHALL preserve Structured Streaming processors and transformations
4. WHEN cleaning Snowflake integration THEN the system SHALL maintain dimensional modeling and warehouse connections
5. IF any monitoring or logging components exist THEN the system SHALL preserve streaming-relevant monitoring capabilities

### Requirement 7

**User Story:** As a developer, I want to validate the cleaned project, so that I can ensure all streaming functionality remains intact.

#### Acceptance Criteria

1. WHEN cleanup is complete THEN the system SHALL verify that streaming pipeline can start successfully
2. WHEN testing data flow THEN the system SHALL confirm data flows from Alpha Vantage through Kafka to Snowflake
3. WHEN running containers THEN the system SHALL ensure Docker compose starts all streaming services
4. WHEN checking configurations THEN the system SHALL validate that all environment variables are properly set
5. IF any functionality is broken after cleanup THEN the system SHALL identify and restore necessary components