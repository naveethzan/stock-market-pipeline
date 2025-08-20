"""
Base consumer class for Kafka stock market data consumers
"""

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from confluent_kafka import Consumer as KafkaConsumer, KafkaException, KafkaError

from src.kafka.config import AppConfig, KafkaConfig, S3Config

logger = logging.getLogger(__name__)


class BaseConsumer(ABC):
    """
    Base class for Kafka consumers with S3 storage capabilities.
    Designed to be used as a context manager to ensure resources are properly managed.
    """

    def __init__(self, config: AppConfig):
        """
        Initialize the base consumer.
        
        Args:
            config: Application configuration.
        """
        self.config = config.kafka
        self.s3_config = config.s3
        self.consumer = None
        self.s3_client = None
        logger.info("BaseConsumer configured.")

    def connect(self):
        """Initialize Kafka and S3 connections."""
        if not self.consumer:
            # confluent-kafka consumer configuration
            self.consumer = KafkaConsumer({
                'bootstrap.servers': self.config.bootstrap_servers,
                'group.id': getattr(self.config, 'group_id', 'stock-batch-consumer'),
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            })
            logger.info(f"Confluent Kafka consumer connected to {self.config.bootstrap_servers}")
        if not self.s3_client:
            self.s3_client = self._initialize_s3_client()

    def close(self):
        """Close Kafka and S3 connections."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed.")
            self.consumer = None

    def __enter__(self):
        """Enter the context manager, connecting to services."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager, closing connections."""
        self.close()
    
    def _initialize_s3_client(self) -> Optional[boto3.client]:
        """
        Initialize S3 client with credentials
        
        Returns:
            S3 client or None if credentials not available
        """
        try:
            if self.s3_config.aws_access_key_id and self.s3_config.aws_secret_access_key:
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.s3_config.aws_access_key_id,
                    aws_secret_access_key=self.s3_config.aws_secret_access_key,
                    region_name=self.s3_config.aws_region
                )
                logger.info(f"S3 client initialized for region {self.s3_config.aws_region}")
                return s3_client
            else:
                # Try to use default credentials (IAM roles, environment variables, etc.)
                s3_client = boto3.client('s3', region_name=self.s3_config.aws_region)
                # Test the connection
                s3_client.head_bucket(Bucket=self.s3_config.bucket_name)
                logger.info(f"S3 client initialized using default credentials for region {self.s3_config.aws_region}")
                return s3_client
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            return None
        except ClientError as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error initializing S3 client: {e}")
            return None
    
    def store_to_s3(self, data: Any, key: str, content_type: str = 'text/csv') -> bool:
        """
        Store data to S3 bucket
        
        Args:
            data: Data to store (string or bytes)
            key: Full S3 object key (including path)
            content_type: MIME type of the content
            
        Returns:
            True if successful, False otherwise
        """
        if not self.s3_client:
            logger.error("S3 client not initialized")
            return False
        
        try:
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_config.bucket_name,
                Key=key,
                Body=data,
                ContentType=content_type
            )
            
            logger.info(f"Data stored to S3: s3://{self.s3_config.bucket_name}/{key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to store data to S3: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error storing data to S3: {e}")
            return False
    
    def create_s3_key(self, symbol: str, timestamp: datetime, data_type: str, record_date: datetime = None) -> str:
        """
        Create S3 object key with Hive-style partitioning based on the actual data date
        Format: raw-data/{data_type}/year=YYYY/month=MM/day=DD/symbol_HHMMSS.csv
        
        Args:
            symbol: Stock symbol
            timestamp: Processing timestamp
            data_type: Type of data (batch or stream)
            record_date: Actual date of the stock data (defaults to timestamp if not provided)
            
        Returns:
            S3 object key with Hive-style partitioning
        """
        # Use record_date if provided, otherwise use the processing timestamp
        date_obj = record_date if record_date else timestamp
        
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        day = date_obj.strftime("%d")
        time_str = datetime.now().strftime("%H%M%S")
        
        return f"raw-data/{data_type}/year={year}/month={month}/day={day}/{symbol}_{time_str}.csv"
    
    @abstractmethod
    def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process a single Kafka message
        
        Args:
            message: Kafka message content
            
        Returns:
            True if processing successful, False otherwise
        """
        pass
    
    def consume_messages(self, topic: str, max_messages: int = None, idle_timeout: int = 30):
        """
        General message consumption loop with idle timeout.

        Args:
            topic: Kafka topic to consume from
            max_messages: Max number of messages to consume before stopping
            idle_timeout: Seconds to wait for new messages before exiting
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect() or use a context manager.")

        self.consumer.subscribe([topic])
        logger.info(f"Subscribed to topic: {topic}. Waiting for messages...")

        msg_count = 0
        last_msg_time = time.monotonic()
        try:
            while True:
                if max_messages and msg_count >= max_messages:
                    logger.info(f"Reached max messages: {max_messages}")
                    break

                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    if time.monotonic() - last_msg_time > idle_timeout:
                        logger.info(f"No new messages for {idle_timeout} seconds. Shutting down.")
                        break
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"Reached end of partition for {msg.topic()} [{msg.partition()}]")
                    else:
                        raise KafkaException(msg.error())
                else:
                    last_msg_time = time.monotonic()
                    try:
                        # Decode payload (confluent-kafka returns bytes)
                        raw_val = msg.value()
                        payload = json.loads(raw_val.decode('utf-8')) if isinstance(raw_val, (bytes, bytearray)) else raw_val
                        if self.process_message(payload):
                            # Commit this message's offset asynchronously after successful processing
                            self.consumer.commit(message=msg, asynchronous=True)
                            msg_count += 1
                            logger.debug(
                                f"Processed msg topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
                            )
                        else:
                            logger.error("Failed to process message payload")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user.")
        finally:
            logger.info(f"Consumed a total of {msg_count} messages.")
            # The close() method is handled by the __exit__ of the context manager.
            # No explicit close() call needed here.
    
    # Duplicate close() method removed; relying on the earlier defined close() and context manager __exit__
