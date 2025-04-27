import logging
import time
from typing import List, Dict, Any, Optional
import asyncio
from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import ASYNCHRONOUS
import config

logger = logging.getLogger("ingestor.influx_writer")

class InfluxDBWriter:
    """
    Manages connection to InfluxDB and handles writing data points to the database.
    Provides batch writing capabilities with retry logic.
    """
    
    def __init__(self):
        self.url = config.INFLUXDB_URL
        self.token = config.INFLUXDB_TOKEN
        self.org = config.INFLUXDB_ORG
        self.bucket = config.INFLUXDB_BUCKET
        self.client = None
        self.write_api = None
        self.is_connected = False
        self.batch_size = config.INFLUXDB_BATCH_SIZE
        self.flush_interval_ms = config.INFLUXDB_FLUSH_INTERVAL_MS
        self.write_precision = config.INFLUXDB_TIMESTAMP_PRECISION
        
    async def connect(self) -> bool:
        """
        Establish connection to InfluxDB.
        Returns True if connection is successful, False otherwise.
        """
        try:
            logger.info(f"Connecting to InfluxDB at {self.url}")
            self.client = InfluxDBClient(
                url=self.url,
                token=self.token,
                org=self.org
            )
            
            # Configure write options for batching
            write_options = WriteOptions(
                batch_size=self.batch_size,
                flush_interval=self.flush_interval_ms,
                write_type=ASYNCHRONOUS
            )
            
            self.write_api = self.client.write_api(write_options=write_options)
            
            # Verify connection by checking health
            health = self.client.health()
            if health.status == "pass":
                logger.info("Successfully connected to InfluxDB")
                self.is_connected = True
                return True
            else:
                logger.error(f"InfluxDB health check failed: {health.status}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {str(e)}")
            self.is_connected = False
            return False
    
    async def ensure_bucket_exists(self) -> bool:
        """
        Ensures the target bucket exists, creating it if necessary.
        Returns True if bucket exists or was created, False otherwise.
        """
        if not self.is_connected:
            logger.error("Cannot check bucket existence: not connected to InfluxDB")
            return False
            
        try:
            buckets_api = self.client.buckets_api()
            bucket = buckets_api.find_bucket_by_name(self.bucket)
            
            if bucket:
                logger.info(f"Bucket '{self.bucket}' already exists")
                return True
                
            logger.info(f"Creating bucket '{self.bucket}'")
            bucket = buckets_api.create_bucket(bucket_name=self.bucket, org=self.org)
            if bucket:
                logger.info(f"Successfully created bucket '{self.bucket}'")
                return True
            else:
                logger.error(f"Failed to create bucket '{self.bucket}'")
                return False
                
        except Exception as e:
            logger.error(f"Error ensuring bucket exists: {str(e)}")
            return False
    
    async def write_points(self, points: List[Dict[str, Any]]) -> bool:
        """
        Write data points to InfluxDB.
        Returns True if write was successful, False otherwise.
        """
        if not self.is_connected:
            logger.error("Cannot write data: not connected to InfluxDB")
            return False
            
        if not points:
            logger.warning("No points to write")
            return True
            
        try:
            # Write points with the configured precision
            self.write_api.write(bucket=self.bucket, record=points, write_precision=self.write_precision)
            logger.debug(f"Wrote {len(points)} points to InfluxDB with precision {self.write_precision}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write data to InfluxDB: {str(e)}")
            return False
    
    async def close(self):
        """
        Close connection to InfluxDB.
        """
        if self.write_api:
            try:
                self.write_api.close()
            except Exception as e:
                logger.error(f"Error closing write API: {str(e)}")
                
        if self.client:
            try:
                self.client.close()
            except Exception as e:
                logger.error(f"Error closing InfluxDB client: {str(e)}")
                
        self.is_connected = False
        logger.info("InfluxDB connection closed") 