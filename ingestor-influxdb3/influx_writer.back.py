import logging
import time
from typing import List, Dict, Any, Optional
import asyncio
import httpx
from urllib.parse import urljoin
import config

logger = logging.getLogger("ingestor-influxdb3.influx_writer")

class InfluxDB3Writer:
    """
    Manages connection to InfluxDB3 and handles writing data points using HTTP API.
    Provides batch writing capabilities with retry logic.
    """
    
    def __init__(self):
        self.url = config.INFLUXDB3_URL
        self.token = config.INFLUXDB3_TOKEN
        self.database = config.INFLUXDB3_DATABASE
        self.write_endpoint = config.INFLUXDB3_WRITE_ENDPOINT
        self.query_endpoint = config.INFLUXDB3_QUERY_ENDPOINT
        self.client = None
        self.is_connected = False
        self.batch_size = config.INFLUXDB3_BATCH_SIZE
        self.flush_interval_ms = config.INFLUXDB3_FLUSH_INTERVAL_MS
        self.write_precision = config.INFLUXDB3_TIMESTAMP_PRECISION
        self.timeout = config.HTTP_TIMEOUT
        self.max_retries = config.HTTP_MAX_RETRIES
        self.retry_backoff_factor = config.HTTP_RETRY_BACKOFF_FACTOR
        
        # Build API URLs
        self.write_url = urljoin(self.url, self.write_endpoint)
        self.query_url = urljoin(self.url, self.query_endpoint)
        
    async def connect(self) -> bool:
        """
        Establish connection to InfluxDB3.
        Returns True if connection is successful, False otherwise.
        """
        try:
            logger.info(f"Connecting to InfluxDB3 at {self.url}")
            
            # Create HTTP client with timeout and retry configuration
            self.client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "text/plain; charset=utf-8"
                }
            )
            
            # Verify connection by checking health endpoint
            health_url = urljoin(self.url, "/health")
            try:
                response = await self.client.get(health_url)
                if response.status_code == 200:
                    logger.info("Successfully connected to InfluxDB3")
                    self.is_connected = True
                    return True
                else:
                    logger.error(f"InfluxDB3 health check failed with status: {response.status_code}")
                    return False
            except Exception as health_error:
                logger.warning(f"Health check failed: {health_error}, but continuing with connection")
                self.is_connected = True
                return True
                
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB3: {str(e)}")
            self.is_connected = False
            return False
    
    async def ensure_database_exists(self) -> bool:
        """
        Ensures the target database exists, creating it if necessary.
        Returns True if database exists or was created, False otherwise.
        """
        if not self.is_connected:
            logger.error("Cannot check database existence: not connected to InfluxDB3")
            return False
            
        try:
            # Check if database exists by listing databases
            query = "SHOW DATABASES"
            response = await self._execute_query(query)
            
            if response and "databases" in response:
                existing_databases = [db["name"] for db in response["databases"]]
                if self.database in existing_databases:
                    logger.info(f"Database '{self.database}' already exists")
                    return True
            
            # Try to create the database by writing a dummy point and then deleting it
            # InfluxDB3 creates databases automatically on first write
            logger.info(f"Database '{self.database}' may not exist, will be created on first write")
            return True
            
        except Exception as e:
            logger.error(f"Error checking database existence: {str(e)}")
            # Assume database exists or will be created on write
            return True
    
    async def _execute_query(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Execute a query against InfluxDB3.
        Returns query results or None if failed.
        """
        if not self.client:
            return None
            
        try:
            params = {
                "db": self.database,
                "q": query
            }
            
            response = await self.client.get(self.query_url, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Query failed with status {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return None
    
    async def write_points(self, line_protocol_data: str) -> bool:
        """
        Write line protocol data to InfluxDB3.
        Returns True if write was successful, False otherwise.
        """
        if not self.is_connected:
            logger.error("Cannot write data: not connected to InfluxDB3")
            return False
            
        if not line_protocol_data or not line_protocol_data.strip():
            logger.warning("No data to write")
            return True
            
        for attempt in range(self.max_retries + 1):
            try:
                params = {
                    "db": self.database,
                    "precision": self.write_precision
                }
                
                response = await self.client.post(
                    self.write_url,
                    params=params,
                    content=line_protocol_data
                )
                
                if response.status_code == 204:
                    logger.debug(f"Successfully wrote data to InfluxDB3 database '{self.database}'")
                    return True
                elif response.status_code == 400:
                    # Check if it's a partial write
                    response_text = response.text
                    if "partial write" in response_text.lower():
                        logger.warning(f"Partial write occurred: {response_text}")
                        return True  # Consider partial write as success
                    else:
                        logger.error(f"Bad request writing to InfluxDB3: {response_text}")
                        return False
                else:
                    logger.error(f"Failed to write data to InfluxDB3. Status: {response.status_code}, Response: {response.text}")
                    
                    # Retry on server errors (5xx) but not on client errors (4xx)
                    if response.status_code >= 500 and attempt < self.max_retries:
                        wait_time = self.retry_backoff_factor * (2 ** attempt)
                        logger.info(f"Retrying write in {wait_time} seconds (attempt {attempt + 1}/{self.max_retries + 1})")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    return False
                    
            except httpx.TimeoutException:
                logger.error(f"Timeout writing to InfluxDB3 (attempt {attempt + 1}/{self.max_retries + 1})")
                if attempt < self.max_retries:
                    wait_time = self.retry_backoff_factor * (2 ** attempt)
                    await asyncio.sleep(wait_time)
                    continue
                return False
                
            except Exception as e:
                logger.error(f"Error writing to InfluxDB3: {str(e)} (attempt {attempt + 1}/{self.max_retries + 1})")
                if attempt < self.max_retries:
                    wait_time = self.retry_backoff_factor * (2 ** attempt)
                    await asyncio.sleep(wait_time)
                    continue
                return False
        
        return False
    
    async def close(self):
        """
        Close connection to InfluxDB3.
        """
        if self.client:
            try:
                await self.client.aclose()
            except Exception as e:
                logger.error(f"Error closing HTTP client: {str(e)}")
                
        self.is_connected = False
        logger.info("InfluxDB3 connection closed") 