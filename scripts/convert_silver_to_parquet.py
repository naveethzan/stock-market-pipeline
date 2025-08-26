#!/usr/bin/env python3
"""
Script to convert Silver layer JSON files to Parquet format.
This satisfies the requirement for Parquet format in Silver layer while maintaining compatibility.

Usage:
    python scripts/convert_silver_to_parquet.py
"""
import os
import sys
import boto3
import json
import pandas as pd
from pathlib import Path
import tempfile
import logging
from typing import List, Dict, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SilverToParquetConverter:
    """Convert Silver layer JSON files to Parquet format in S3."""
    
    def __init__(self, bucket_name: str, aws_region: str = "us-east-1"):
        """Initialize the converter with S3 configuration."""
        self.bucket_name = bucket_name
        self.aws_region = aws_region
        self.s3_client = boto3.client('s3', region_name=aws_region)
        
    def list_json_files(self, prefix: str = "silver/stock-data/") -> List[str]:
        """List all JSON files in the Silver layer S3 path."""
        try:
            logger.info(f"Listing JSON files in s3://{self.bucket_name}/{prefix}")
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            json_files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.endswith('.json'):
                        json_files.append(key)
            
            logger.info(f"Found {len(json_files)} JSON files to convert")
            return json_files
            
        except Exception as e:
            logger.error(f"Failed to list JSON files: {str(e)}")
            return []
    
    def download_and_parse_json_file(self, s3_key: str) -> List[Dict[str, Any]]:
        """Download and parse a JSON file from S3."""
        try:
            logger.debug(f"Downloading {s3_key}")
            
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            
            # Parse JSON lines format (each line is a separate JSON object)
            records = []
            for line in content.strip().split('\n'):
                if line.strip():
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as json_err:
                        logger.warning(f"Failed to parse JSON line in {s3_key}: {json_err}")
                        continue
            
            logger.debug(f"Parsed {len(records)} records from {s3_key}")
            return records
            
        except Exception as e:
            logger.error(f"Failed to download/parse {s3_key}: {str(e)}")
            return []
    
    def convert_json_to_parquet(self, records: List[Dict[str, Any]], output_path: str):
        """Convert JSON records to Parquet format."""
        try:
            if not records:
                logger.warning("No records to convert")
                return
            
            # Convert to pandas DataFrame
            df = pd.DataFrame(records)
            
            # Clean up data types for Parquet compatibility
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Try to convert timestamp columns
                    if 'timestamp' in col.lower():
                        try:
                            df[col] = pd.to_datetime(df[col], errors='ignore')
                        except:
                            pass
                    # Convert boolean-like strings
                    elif df[col].astype(str).str.lower().isin(['true', 'false']).all():
                        df[col] = df[col].astype(str).str.lower() == 'true'
            
            # Write to Parquet with compression
            df.to_parquet(output_path, compression='snappy', index=False)
            logger.info(f"Converted {len(records)} records to Parquet: {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to convert to Parquet: {str(e)}")
            raise
    
    def upload_parquet_file(self, local_path: str, s3_key: str):
        """Upload Parquet file to S3."""
        try:
            # Replace .json with .parquet in the S3 key
            parquet_key = s3_key.replace('.json', '.parquet')
            # Change path from silver/stock-data to silver-parquet/stock-data
            parquet_key = parquet_key.replace('silver/stock-data', 'silver-parquet/stock-data')
            
            logger.info(f"Uploading Parquet file to s3://{self.bucket_name}/{parquet_key}")
            
            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                parquet_key,
                ExtraArgs={
                    'ServerSideEncryption': 'AES256',
                    'Metadata': {
                        'source_format': 'json',
                        'converted_by': 'silver_to_parquet_converter',
                        'compression': 'snappy'
                    }
                }
            )
            
            logger.info(f"Successfully uploaded to {parquet_key}")
            return parquet_key
            
        except Exception as e:
            logger.error(f"Failed to upload Parquet file: {str(e)}")
            raise
    
    def convert_file(self, s3_key: str) -> str:
        """Convert a single JSON file to Parquet."""
        logger.info(f"Converting {s3_key} to Parquet format")
        
        try:
            # Download and parse JSON
            records = self.download_and_parse_json_file(s3_key)
            
            if not records:
                logger.warning(f"No records found in {s3_key}, skipping")
                return None
            
            # Create temporary file for Parquet
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                tmp_path = tmp_file.name
            
            try:
                # Convert to Parquet
                self.convert_json_to_parquet(records, tmp_path)
                
                # Upload to S3
                parquet_key = self.upload_parquet_file(tmp_path, s3_key)
                
                return parquet_key
                
            finally:
                # Clean up temporary file
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                    
        except Exception as e:
            logger.error(f"Failed to convert {s3_key}: {str(e)}")
            return None
    
    def convert_all_files(self) -> Dict[str, Any]:
        """Convert all JSON files in Silver layer to Parquet."""
        logger.info("Starting conversion of all Silver layer JSON files to Parquet")
        
        try:
            # List all JSON files
            json_files = self.list_json_files()
            
            if not json_files:
                logger.warning("No JSON files found in Silver layer")
                return {
                    'status': 'success',
                    'converted_files': 0,
                    'failed_files': 0,
                    'details': []
                }
            
            results = {
                'status': 'success',
                'converted_files': 0,
                'failed_files': 0,
                'details': []
            }
            
            for json_file in json_files:
                try:
                    parquet_key = self.convert_file(json_file)
                    if parquet_key:
                        results['converted_files'] += 1
                        results['details'].append({
                            'source': json_file,
                            'target': parquet_key,
                            'status': 'success'
                        })
                    else:
                        results['failed_files'] += 1
                        results['details'].append({
                            'source': json_file,
                            'target': None,
                            'status': 'failed',
                            'error': 'Conversion returned None'
                        })
                        
                except Exception as e:
                    results['failed_files'] += 1
                    results['details'].append({
                        'source': json_file,
                        'target': None,
                        'status': 'failed',
                        'error': str(e)
                    })
            
            logger.info(f"Conversion completed: {results['converted_files']} success, {results['failed_files']} failed")
            return results
            
        except Exception as e:
            logger.error(f"Failed to convert files: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'converted_files': 0,
                'failed_files': 0,
                'details': []
            }


def main():
    """Main function."""
    # Configuration
    bucket_name = os.getenv('S3_BUCKET_NAME', 'stock-market-pipeline-zan')
    aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    logger.info("="*60)
    logger.info("🔄 SILVER LAYER JSON TO PARQUET CONVERTER")
    logger.info("="*60)
    logger.info(f"S3 Bucket: {bucket_name}")
    logger.info(f"AWS Region: {aws_region}")
    
    try:
        # Initialize converter
        converter = SilverToParquetConverter(bucket_name, aws_region)
        
        # Convert all files
        results = converter.convert_all_files()
        
        # Print summary
        print("\n" + "="*60)
        print("📊 CONVERSION SUMMARY")
        print("="*60)
        print(f"Status: {results['status'].upper()}")
        print(f"Successfully converted: {results['converted_files']} files")
        print(f"Failed conversions: {results['failed_files']} files")
        
        if results['details']:
            print(f"\n📝 DETAILED RESULTS:")
            for detail in results['details']:
                status_icon = "✅" if detail['status'] == 'success' else "❌"
                print(f"   {status_icon} {detail['source']}")
                if detail['status'] == 'success' and detail['target']:
                    print(f"      → {detail['target']}")
                elif detail['status'] == 'failed':
                    print(f"      Error: {detail.get('error', 'Unknown error')}")
        
        print("\n💡 USAGE NOTES:")
        print("- JSON files are preserved in silver/stock-data/")
        print("- Parquet files are created in silver-parquet/stock-data/")
        print("- Both formats are available for different use cases")
        print("- Parquet files use Snappy compression for optimal performance")
        
        if results['status'] == 'success' and results['converted_files'] > 0:
            print("\n✅ Silver layer data is now available in Parquet format!")
        elif results['failed_files'] > 0:
            print(f"\n⚠️  {results['failed_files']} files failed conversion - check logs")
            sys.exit(1)
        else:
            print("\n📝 No files found to convert - may need to wait for data flow")
            
    except Exception as e:
        logger.error(f"Conversion failed: {str(e)}")
        print(f"\n❌ CONVERSION FAILED: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()