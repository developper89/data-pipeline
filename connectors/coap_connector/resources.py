# coap_gateway/resources.py
import logging
import base64
import json
from datetime import datetime, timezone
import uuid # To generate request ID if needed

import aiocoap
import aiocoap.resource as resource
from kafka.errors import KafkaError

from shared.models.common import RawMessage # Import shared model
from kafka_producer import KafkaMsgProducer # Import the Kafka producer wrapper
from command_consumer import CommandConsumer

logger = logging.getLogger(__name__)

class DeviceDataHandlerResource(resource.Resource):
    """Handles POST/PUT requests for a specific device ID."""

    def __init__(self, device_id: str, kafka_producer: KafkaMsgProducer, command_consumer: CommandConsumer): 
        super().__init__()
        self.device_id = device_id
        self.kafka_producer = kafka_producer
        self.command_consumer = command_consumer
        logger.debug(f"Initialized handler resource for device: {self.device_id}")

    async def render_post(self, request: aiocoap.Message) -> aiocoap.Message:
        """Handles incoming POST requests."""
        return await self._process_request(request, method="POST")

    async def render_put(self, request: aiocoap.Message) -> aiocoap.Message:
        """Handles incoming PUT requests."""
        return await self._process_request(request, method="PUT")

    async def _process_request(self, request: aiocoap.Message, method: str) -> aiocoap.Message:
        """
        Common logic for processing POST/PUT.
        Also includes pending commands in the response if any exist.
        """
        request_id = str(uuid.uuid4()) # Generate a unique ID for this request
        payload_bytes = request.payload
        source_address = request.remote.hostinfo
        request_uri_path = "/".join(request.opt.uri_path)

        logger.info(f"[{request_id}] Received CoAP {method} for device '{self.device_id}' from {source_address} to uri '{request_uri_path}'. Payload size: {len(payload_bytes)} bytes.")

        if not payload_bytes:
            logger.warning(f"[{request_id}] Empty payload received for device {self.device_id}. Sending Bad Request.")
            return aiocoap.Message(code=aiocoap.Code.BAD_REQUEST, payload=b"Payload must not be empty")

        try:
            # 1. Prepare the RawMessage
            raw_message = RawMessage(
                request_id=request_id, # Include the generated request ID
                device_id=self.device_id,
                payload_hex=payload_bytes.hex(),  # Convert binary to hex string
                protocol="coap",
                metadata={
                    "source_address": source_address,
                    "method": method,
                    "uri_path": request_uri_path,
                }
            )

            # 2. Publish to Kafka (Synchronous call to the wrapper)
            try:
                self.kafka_producer.publish_raw_message(raw_message)
                # Logging is handled within the wrapper
            except (KafkaError, Exception) as publish_err:
                 # If publishing fails, log the error and return a server error to CoAP client
                 logger.error(f"[{request_id}] Failed to publish CoAP data to Kafka for device {self.device_id}: {publish_err}")
                 # Attempt to publish a gateway error event (this might also fail)
                 try:
                      self.kafka_producer.publish_error(
                           "Kafka Publish Failed", str(publish_err),
                           {"device_id": self.device_id, "method": method},
                           request_id=request_id
                      )
                 except Exception as e_pub_err:
                      logger.error(f"[{request_id}] Additionally failed to publish gateway error event after Kafka failure: {e_pub_err}")

                 return aiocoap.Message(code=aiocoap.Code.INTERNAL_SERVER_ERROR, payload=b"Failed to forward data internally")

            # 3. Check for pending commands for this device
            # Try to get a formatted command using the bidirectional parser
            formatted_command = await self.command_consumer.get_formatted_command(self.device_id)
            
            if formatted_command:
                # Get the command details for logging
                pending_commands = self.command_consumer.get_pending_commands(self.device_id)
                command_id = pending_commands[0].get("request_id", "unknown") if pending_commands else "unknown"
                
                logger.info(f"[{request_id}] Sending formatted command {command_id} to device {self.device_id}")
                
                # Acknowledge the command was sent (removes from pending)
                if pending_commands:
                    self.command_consumer.acknowledge_command(self.device_id, command_id)
                
                # Send the formatted binary command in response
                success_code = aiocoap.Code.CHANGED if method == "PUT" else aiocoap.Code.CREATED
                return aiocoap.Message(code=success_code, payload=formatted_command)
            else:
                # No commands pending or no parser available - just send success code
                success_code = aiocoap.Code.CHANGED if method == "PUT" else aiocoap.Code.CREATED
                logger.debug(f"[{request_id}] Successfully processed data for {self.device_id}. No commands sent.")
                return aiocoap.Message(code=success_code)

        except Exception as e:
            # Catch any other unexpected errors during RawMessage creation etc.
            logger.exception(f"[{request_id}] Unexpected error processing CoAP request for device {self.device_id}: {e}")
            # Attempt to publish a gateway error event
            try:
                self.kafka_producer.publish_error(
                    "CoAP Processing Error", str(e),
                    {"device_id": self.device_id, "method": method},
                    request_id=request_id
                 )
            except Exception as e_pub_err:
                 logger.error(f"[{request_id}] Additionally failed to publish gateway error event after processing error: {e_pub_err}")

            return aiocoap.Message(code=aiocoap.Code.INTERNAL_SERVER_ERROR, payload=b"Internal server error during processing")


class DataRootResource(resource.Site): # Inherit from Site for automatic child handling
    """
    Acts as a factory for DeviceDataHandlerResource based on path.
    Listens on the base path (e.g., /data) and delegates requests
    like /data/device123 to a handler for 'device123'.
    """
    def __init__(self, kafka_producer: KafkaMsgProducer, command_consumer: CommandConsumer):
        super().__init__()
        self.kafka_producer = kafka_producer
        self.command_consumer = command_consumer
        logger.debug(f"Initialized DataRootResource.")

    # async def render(self, request):
    #      # Requests directly to the root (e.g., /data) are not allowed
    #      logger.warning(f"Request received directly to data root path {request.opt.uri_path}. Method Not Allowed.")
    #      return aiocoap.Message(code=aiocoap.Code.METHOD_NOT_ALLOWED)

    def _find_child_and_pathstripped_message(self, request):
        #  """
        #  Dynamically create a handler for the device ID path segment.
        #  'path' contains the remaining path segments.
        #  """
        """Override the original method to add our debug print statement.
        
        This method finds the child that will handle the request and strips
        all path components that are covered by the child's position within
        the site.
        """
        original_request_uri = getattr(
            request,
            "_original_request_uri",
            request.get_request_uri(local_is_server=True),
        )
        
        # Add comprehensive debug logging
        logger.info(f"Request received - URI: {original_request_uri}")
        logger.info(f"Path components: {request.opt.uri_path}")
        
        # Continue with the rest of the original method implementation
        if request.opt.uri_path in self._resources:
            logger.debug(f"Found exact resource match for path: {request.opt.uri_path}")
            stripped = request.copy(uri_path=())
            stripped._original_request_uri = original_request_uri
            return self._resources[request.opt.uri_path], stripped

        if not request.opt.uri_path:
            logger.debug("No URI path components, resource not found")
            raise KeyError()

        remainder = [request.opt.uri_path[-1]]
        path = request.opt.uri_path[:-1]
        logger.debug(f"Looking for partial path match. Initial path: {path}, remainder: {remainder}")
        
        while path:
            logger.debug(f"Checking path: {path}")
            if path in self._subsites:
                res = self._subsites[path]
                logger.debug(f"Found matching subsite for path: {path}")
                if remainder == [""]:
                    # sub-sites should see their root resource like sites
                    remainder = []
                stripped = request.copy(uri_path=remainder)
                stripped._original_request_uri = original_request_uri
                logger.debug(f"Forwarding to subsite with remainder: {remainder}")
                return res, stripped
            remainder.insert(0, path[-1])
            path = path[:-1]
            
        logger.debug(f"No matching resource found for path: {request.opt.uri_path}")
        raise KeyError()
        #  if len(path) == 1: # Expecting only one segment: the device ID
        #      device_id_bytes = path[0]
        #      try:
        #          device_id = device_id_bytes.decode('utf-8')
        #          logger.debug(f"Request for device sub-path '{device_id}'. Creating handler.")
        #          # Return a *new instance* of the handler for this specific device ID
        #          return DeviceDataHandlerResource(device_id, self.kafka_producer, self.command_consumer)
        #      except UnicodeDecodeError:
        #           logger.warning(f"Invalid UTF-8 in path element: {device_id_bytes!r}. Rejecting request.")
        #           # Returning None results in 4.04 Not Found
        #           return None
        #      except Exception as e:
        #          logger.exception(f"Error creating child resource for path element {device_id_bytes!r}: {e}")
        #          return None
        #  else:
        #      # Path doesn't match /data/{device_id} structure
        #      logger.warning(f"Request path structure not recognized: {path}")
        #      return None # Results in 4.04 Not Found