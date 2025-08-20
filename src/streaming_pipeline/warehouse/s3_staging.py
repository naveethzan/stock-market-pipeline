"""
S3 Staging Manager for Snowflake Data Loading

This module manages S3 staging operations for Snowflake data loading,
including Parquet file uploads, directory management, and metadata tracking.
"""

import logging
import os
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config.settings import get_settings

logger = logging.getLogger(__name__)


class S3StagingManager:
    """Manager for S3 staging operations for Snowflake data loading"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize S3 staging manager
        
        Args:
            config: Optional configuration dict, uses settings if not provided
        """
        self.settings = get_settings()
        self.config = config or self._get_s3_config()
        self.s3_client = self._create_s3_client()
        self.bucket_name = self.config["bucket_name"]
        self.staging_prefix = self.config.get("staging_prefix", "staging/streaming/")
        
    def _get_s3_config(self) -> Dict[str, Any]:
        """Get S3 configuration from settings"""
        return {
            "bucket_name": self.settings.s3_bucket_name,
            "region": self.settings.aws_region,
            "staging_prefix": "staging/streaming/",
            "processed_prefix": "processed/streaming/",
            "error_prefix": "errors/streaming/"
        }
    
    def _create_s3_client(self):
        """Create S3 client with proper configuration"""
        try:
            return boto3.client(
                's3',
                region_name=self.config["region"],
                aws_access_key_id=self.settings.aws_access_key_id,
                aws_secret_access_key=self.settings.aws_secret_access_key
            )
        except NoCredentialsError:
            logger.warning("AWS credentials not found, using default credential chain")
            return boto3.client('s3', region_name=self.config["region"])
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def upload_dataframe_as_parquet(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Upload DataFrame as Parquet file to S3
        
        Args:
            df: DataFrame to upload
            table_name: Target table name for organizing files
            partition_cols: Columns to partition by
            timestamp: Timestamp for file naming (uses current time if not provided)
            
        Returns:
            S3 key of uploaded file
        """
        if df.empty:
            logger.warning("DataFrame is empty, skipping upload")
            return ""
        
        timestamp = timestamp or datetime.now(timezone.utc)
        
        # Generate file path
        date_partition = timestamp.strftime("%Y/%m/%d")
        hour_partition = timestamp.strftime("%H")
        filename = f"{table_name}_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
        
        s3_key = f"{self.staging_prefix}{table_name}/{date_partition}/{hour_partition}/{filename}"
        
        try:
            # Convert DataFrame to Parquet
            table = pa.Table.from_pandas(df)
            
            # Create buffer for Parquet data
            buffer = pa.BufferOutputStream()
            pq.write_table(
                table, 
                buffer,
                compression='snappy',
                use_dictionary=True,
                row_group_size=50000
            )
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=buffer.getvalue().to_pybytes(),
                ContentType='application/octet-stream',
                Metadata={
                    'table_name': table_name,
                    'record_count': str(len(df)),
                    'upload_timestamp': timestamp.isoformat(),
                    'partition_cols': json.dumps(partition_cols) if partition_cols else ''
                }
            )
            
            logger.info(f"Successfully uploaded {len(df)} records to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error uploading DataFrame to S3: {e}")
            raise
    
    def upload_file(
        self,
        local_file_path: str,
        s3_key: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Upload local file to S3
        
        Args:
            local_file_path: Path to local file
            s3_key: S3 key for the uploaded file
            metadata: Optional metadata to attach
            
        Returns:
            True if successful, False otherwise
        """
        try:
            extra_args = {}
            if metadata:
                extra_args['Metadata'] = metadata
            
            self.s3_client.upload_file(
                local_file_path,
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            logger.info(f"Successfully uploaded {local_file_path} to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading file to S3: {e}")
            return False
    
    def list_staged_files(
        self,
        table_name: Optional[str] = None,
        date_prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List staged files in S3
        
        Args:
            table_name: Filter by table name
            date_prefix: Filter by date prefix (YYYY/MM/DD format)
            
        Returns:
            List of file information dictionaries
        """
        prefix = self.staging_prefix
        if table_name:
            prefix += f"{table_name}/"
        if date_prefix:
            prefix += f"{date_prefix}/"
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            files = []
            for obj in response.get('Contents', []):
                # Get object metadata
                try:
                    head_response = self.s3_client.head_object(
                        Bucket=self.bucket_name,
                        Key=obj['Key']
                    )
                    metadata = head_response.get('Metadata', {})
                except Exception as e:
                    logger.warning(f"Could not get metadata for {obj['Key']}: {e}")
                    metadata = {}
                
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'etag': obj['ETag'],
                    'metadata': metadata
                })
            
            logger.info(f"Found {len(files)} staged files with prefix {prefix}")
            return files
            
        except Exception as e:
            logger.error(f"Error listing staged files: {e}")
            return []
    
    def move_to_processed(self, s3_key: str) -> bool:
        """
        Move file from staging to processed directory
        
        Args:
            s3_key: S3 key of the file to move
            
        Returns:
            True if successful, False otherwise
        """
        if not s3_key.startswith(self.staging_prefix):
            logger.error(f"File {s3_key} is not in staging directory")
            return False
        
        # Generate processed key
        processed_key = s3_key.replace(
            self.staging_prefix,
            self.config["processed_prefix"],
            1
        )
        
        try:
            # Copy to processed location
            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                CopySource={'Bucket': self.bucket_name, 'Key': s3_key},
                Key=processed_key
            )
            
            # Delete from staging
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            logger.info(f"Moved {s3_key} to {processed_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error moving file to processed: {e}")
            return False
    
    def move_to_error(self, s3_key: str, error_message: str) -> bool:
        """
        Move file to error directory with error information
        
        Args:
            s3_key: S3 key of the file to move
            error_message: Error message to include in metadata
            
        Returns:
            True if successful, False otherwise
        """
        # Generate error key
        error_key = s3_key.replace(
            self.staging_prefix,
            self.config["error_prefix"],
            1
        )
        
        try:
            # Copy to error location with error metadata
            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                CopySource={'Bucket': self.bucket_name, 'Key': s3_key},
                Key=error_key,
                MetadataDirective='REPLACE',
                Metadata={
                    'error_message': error_message,
                    'error_timestamp': datetime.now(timezone.utc).isoformat()
                }
            )
            
            # Delete from staging
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            logger.info(f"Moved {s3_key} to error directory: {error_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error moving file to error directory: {e}")
            return False
    
    def cleanup_old_files(self, days_old: int = 7) -> int:
        """
        Clean up old processed files
        
        Args:
            days_old: Files older than this many days will be deleted
            
        Returns:
            Number of files deleted
        """
        cutoff_date = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - pd.Timedelta(days=days_old)
        
        deleted_count = 0
        
        try:
            # List processed files
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.config["processed_prefix"]
            )
            
            for obj in response.get('Contents', []):
                if obj['LastModified'].replace(tzinfo=timezone.utc) < cutoff_date:
                    try:
                        self.s3_client.delete_object(
                            Bucket=self.bucket_name,
                            Key=obj['Key']
                        )
                        deleted_count += 1
                        logger.debug(f"Deleted old file: {obj['Key']}")
                    except Exception as e:
                        logger.warning(f"Could not delete {obj['Key']}: {e}")
            
            logger.info(f"Cleaned up {deleted_count} old files")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return 0
    
    def get_staging_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about staged files
        
        Returns:
            Dictionary with staging statistics
        """
        try:
            stats = {
                'total_files': 0,
                'total_size_bytes': 0,
                'tables': {},
                'oldest_file': None,
                'newest_file': None
            }
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.staging_prefix
            )
            
            for obj in response.get('Contents', []):
                stats['total_files'] += 1
                stats['total_size_bytes'] += obj['Size']
                
                # Extract table name from key
                key_parts = obj['Key'].replace(self.staging_prefix, '').split('/')
                if key_parts:
                    table_name = key_parts[0]
                    if table_name not in stats['tables']:
                        stats['tables'][table_name] = {'files': 0, 'size_bytes': 0}
                    stats['tables'][table_name]['files'] += 1
                    stats['tables'][table_name]['size_bytes'] += obj['Size']
                
                # Track oldest and newest files
                if stats['oldest_file'] is None or obj['LastModified'] < stats['oldest_file']:
                    stats['oldest_file'] = obj['LastModified']
                if stats['newest_file'] is None or obj['LastModified'] > stats['newest_file']:
                    stats['newest_file'] = obj['LastModified']
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting staging statistics: {e}")
            return {'error': str(e)}
    
    def create_manifest_file(self, table_name: str, file_keys: List[str]) -> str:
        """
        Create a manifest file for bulk loading
        
        Args:
            table_name: Name of the target table
            file_keys: List of S3 keys to include in manifest
            
        Returns:
            S3 key of the created manifest file
        """
        manifest_data = {
            'entries': [
                {
                    'url': f's3://{self.bucket_name}/{key}',
                    'mandatory': True
                }
                for key in file_keys
            ]
        }
        
        timestamp = datetime.now(timezone.utc)
        manifest_key = f"{self.staging_prefix}manifests/{table_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=manifest_key,
                Body=json.dumps(manifest_data, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"Created manifest file: s3://{self.bucket_name}/{manifest_key}")
            return manifest_key
            
        except Exception as e:
            logger.error(f"Error creating manifest file: {e}")
            raise