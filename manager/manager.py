import os
import time
import yaml
import docker
import logging
import json
import hashlib
from typing import Dict, Set, Tuple
from datetime import datetime, timedelta, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.getenv('CONFIG_PATH', 'shared/connectors_config.yaml')
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', '30'))  # seconds
NETWORK_NAME = "preservarium_dev_net"  # Must match the network name in docker-compose.yml
MAX_RESTART_ATTEMPTS = 3  # Maximum number of restart attempts within the time window
RESTART_WINDOW = 300  # Time window in seconds (5 minutes) for counting restarts

# Get host paths for volume mounting
HOST_SHARED_PATH = os.getenv('HOST_SHARED_PATH')
if HOST_SHARED_PATH:
    logger.info(f"Using host shared path from environment: {HOST_SHARED_PATH}")
else:
    logger.warning("HOST_SHARED_PATH environment variable not set. Volume mounting may fail.")

def load_config(path: str) -> dict:
    """Load and parse the YAML configuration file."""
    try:
        with open(path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config from {path}: {e}")
        return {"connectors": []}

def generate_container_name(connector_id: str) -> str:
    """Generate a consistent container name for a connector."""
    return connector_id

def ensure_network_exists(client: docker.DockerClient) -> str:
    """Ensure the required network exists, create it if it doesn't."""
    try:
        network = client.networks.get(NETWORK_NAME)
        logger.debug(f"Found existing network: {NETWORK_NAME}")
        return network.id
    except docker.errors.NotFound:
        logger.info(f"Network {NETWORK_NAME} not found, creating it...")
        network = client.networks.create(
            name=NETWORK_NAME,
            driver="bridge",
            check_duplicate=True
        )
        logger.info(f"Created network: {NETWORK_NAME}")
        return network.id
    except Exception as e:
        logger.error(f"Error ensuring network exists: {e}")
        raise

def check_container_health(container: docker.models.containers.Container) -> Tuple[bool, str]:
    """Check if a container is healthy and running properly."""
    try:
        container.reload()  # Refresh container state
        
        # Check container state
        if container.status != 'running':
            return False, f"Container is not running (status: {container.status})"
            
        # Get container logs to check for errors
        # logs = container.logs(tail=50, timestamps=True).decode('utf-8').strip()
        # Temporarily disable error detection to avoid restarting containers with debug logs
        # if "Error:" in logs or "error:" in logs:
        #     return False, f"Container logs indicate errors: {logs.split('Error:')[-1].split('\\n')[0]}"
            
        # Check if container is restarting too frequently
        if container.attrs['RestartCount'] > 0:
            started_at = datetime.fromisoformat(container.attrs['State']['StartedAt'].replace('Z', '+00:00'))
            if datetime.now(timezone.utc) - started_at < timedelta(minutes=1):
                return False, "Container is restarting too frequently"
                
        return True, "Container is healthy"
    except Exception as e:
        return False, f"Error checking container health: {e}"

def get_existing_connectors(client: docker.DockerClient) -> Dict[str, docker.models.containers.Container]:
    """Get all existing connector containers."""
    containers = {}
    for container in client.containers.list(all=True):
        # Check if container is managed by preservarium
        labels = container.labels or {}
        if labels.get("preservarium.managed") == "true":
            connector_id = labels.get("preservarium.connector.id")
            if connector_id:
                containers[connector_id] = container
            else:
                # Fallback to name-based detection for legacy containers
                if container.name.startswith("connector_container_"):
                    connector_id = container.name.replace("connector_container_", "")
                    containers[connector_id] = container
        elif container.name.startswith("connector_container_"):
            # Legacy container without labels
            connector_id = container.name.replace("connector_container_", "")
            containers[connector_id] = container
    return containers

def create_connector_container(client: docker.DockerClient, connector_config: dict) -> None:
    """Create and start a new connector container."""
    connector_id = connector_config["connector_id"]
    container_name = generate_container_name(connector_id)
    
    try:
        # Ensure network exists
        ensure_network_exists(client)

        volumes = {}
        
        # Add shared volume mount if host path is available
        if HOST_SHARED_PATH:
            volumes[HOST_SHARED_PATH] = {"bind": "/app/shared", "mode": "rw"}
            logger.info(f"Using host path for shared volume: {HOST_SHARED_PATH}:/app/shared")
        logger.info(f"Volumes: {volumes}")
        
        # Setup environment variables
        environment = connector_config.get("env", {}).copy()
        
        # Add PYTHONPATH to environment to allow finding the shared module
        environment["PYTHONPATH"] = "/app"
        
        # Setup port mapping if EXPOSE_PORT is defined
        ports = {}
        expose_ports = environment.get("EXPOSE_PORT")
        if expose_ports:
            # Handle both single port (string) and multiple ports (list)
            if isinstance(expose_ports, str):
                expose_ports = [expose_ports]
            elif isinstance(expose_ports, list):
                # Convert all list items to strings
                expose_ports = [str(port) for port in expose_ports]
            
            exposed_ports_list = []
            for port_spec in expose_ports:
                # Parse port specification (e.g., "8456/udp", "8080/tcp", or just "8080")
                if '/' in port_spec:
                    port, protocol = port_spec.rsplit('/', 1)
                    # Map container port to host port (same port number) with specified protocol
                    ports[f"{port}/{protocol}"] = port
                    exposed_ports_list.append(port_spec)
                else:
                    # If no protocol specified, default to both UDP and TCP (backwards compatibility)
                    port = port_spec
                    ports[f"{port}/udp"] = port  # UDP mapping
                    ports[f"{port}/tcp"] = port  # TCP mapping
                    exposed_ports_list.append(f"{port}/udp")
                    exposed_ports_list.append(f"{port}/tcp")
            
            logger.info(f"Exposing ports {', '.join(exposed_ports_list)} for container {container_name}")
        
        # Create labels to track configuration for change detection
        translators_config = connector_config.get("translators", [])
        translators_hash = hashlib.md5(
            json.dumps(translators_config, sort_keys=True).encode()
        ).hexdigest()
        
        labels = {
            "preservarium.connector.id": connector_id,
            "preservarium.translators.hash": translators_hash,
            "preservarium.managed": "true"
        }
        
        logger.info(f"Creating container {container_name} from image {connector_config['image']}")
        logger.debug(f"Translators config hash: {translators_hash}")
        
        container = client.containers.create(
            image=connector_config["image"],
            name=container_name,
            detach=True,
            environment=environment,
            restart_policy={"Name": "on-failure", "MaximumRetryCount": MAX_RESTART_ATTEMPTS},
            network=NETWORK_NAME,
            volumes=volumes,
            ports=ports if ports else None,
            labels=labels
        )
        
        # Start the container
        container.start()
        logger.info(f"Successfully created and started container {container_name}")
        
        # Initial health check
        is_healthy, message = check_container_health(container)
        if not is_healthy:
            logger.warning(f"Container {container_name} health check failed after creation: {message}")
        
    except Exception as e:
        logger.error(f"Failed to create container {container_name}: {e}")
        raise

def handle_unhealthy_container(client: docker.DockerClient, container: docker.models.containers.Container, connector_config: dict) -> None:
    """Handle an unhealthy container by attempting to restart it or recreate if necessary."""
    try:
        container.reload()
        container_name = container.name
        
        # Check restart count within time window
        started_at = datetime.fromisoformat(container.attrs['State']['StartedAt'].replace('Z', '+00:00'))
        restart_count = container.attrs['RestartCount']
        
        if restart_count >= MAX_RESTART_ATTEMPTS and datetime.now(timezone.utc) - started_at < timedelta(seconds=RESTART_WINDOW):
            logger.error(f"Container {container_name} has been restarting too frequently. Recreating container...")
            container.remove(force=True)
            create_connector_container(client, connector_config)
        else:
            logger.warning(f"Attempting to restart container {container_name}")
            container.restart()
            
    except Exception as e:
        logger.error(f"Error handling unhealthy container: {e}")

def get_expected_container_config(connector_config: dict) -> dict:
    """Generate the expected container configuration from connector config."""
    environment = connector_config.get("env", {}).copy()
    environment["PYTHONPATH"] = "/app"
    
    # Setup expected ports
    expected_ports = {}
    expose_ports = environment.get("EXPOSE_PORT")
    if expose_ports:
        if isinstance(expose_ports, str):
            expose_ports = [expose_ports]
        elif isinstance(expose_ports, list):
            expose_ports = [str(port) for port in expose_ports]
        
        for port_spec in expose_ports:
            if '/' in port_spec:
                port, protocol = port_spec.rsplit('/', 1)
                expected_ports[f"{port}/{protocol}"] = port
            else:
                port = port_spec
                expected_ports[f"{port}/udp"] = port
                expected_ports[f"{port}/tcp"] = port
    
    # Setup expected volumes
    expected_volumes = {}
    if HOST_SHARED_PATH:
        expected_volumes[HOST_SHARED_PATH] = {"bind": "/app/shared", "mode": "rw"}
    
    return {
        "image": connector_config["image"],
        "environment": environment,
        "ports": expected_ports,
        "volumes": expected_volumes,
        "network": NETWORK_NAME,
        "translators": connector_config.get("translators", [])  # Track translator config
    }

def container_config_matches(container: docker.models.containers.Container, expected_config: dict) -> Tuple[bool, str]:
    """Check if container configuration matches the expected configuration."""
    try:
        container.reload()
        container_attrs = container.attrs
        
        # Check image
        current_image = container_attrs['Config']['Image']
        expected_image = expected_config['image']
        if current_image != expected_image:
            return False, f"Image mismatch: current={current_image}, expected={expected_image}"
        
        # Check environment variables
        current_env = {}
        if container_attrs['Config'].get('Env'):
            for env_var in container_attrs['Config']['Env']:
                if '=' in env_var:
                    key, value = env_var.split('=', 1)
                    current_env[key] = value
        
        expected_env = expected_config['environment']
        for key, expected_value in expected_env.items():
            current_value = current_env.get(key)
            if current_value != str(expected_value):
                return False, f"Environment variable mismatch for {key}: current={current_value}, expected={expected_value}"
        
        # Check ports
        current_ports = {}
        if container_attrs['NetworkSettings'].get('Ports'):
            for port_key, port_bindings in container_attrs['NetworkSettings']['Ports'].items():
                if port_bindings:
                    current_ports[port_key] = port_bindings[0]['HostPort']
        
        expected_ports = expected_config['ports']
        for port_key, expected_host_port in expected_ports.items():
            current_host_port = current_ports.get(port_key)
            if current_host_port != str(expected_host_port):
                return False, f"Port mapping mismatch for {port_key}: current={current_host_port}, expected={expected_host_port}"
        
        # Check network
        current_networks = list(container_attrs['NetworkSettings']['Networks'].keys())
        expected_network = expected_config['network']
        if expected_network not in current_networks:
            return False, f"Network mismatch: current={current_networks}, expected={expected_network}"
        
        # Check translators configuration (stored as container label)
        expected_translators = expected_config.get('translators', [])
        expected_translators_hash = hashlib.md5(
            json.dumps(expected_translators, sort_keys=True).encode()
        ).hexdigest()
        
        current_labels = container_attrs['Config'].get('Labels') or {}
        current_translators_hash = current_labels.get('preservarium.translators.hash', '')
        
        if current_translators_hash != expected_translators_hash:
            return False, f"Translators configuration changed (hash mismatch: current={current_translators_hash[:8]}..., expected={expected_translators_hash[:8]}...)"
        
        return True, "Configuration matches"
        
    except Exception as e:
        return False, f"Error checking container configuration: {e}"

def recreate_container(client: docker.DockerClient, container: docker.models.containers.Container, connector_config: dict) -> None:
    """Recreate a container with new configuration."""
    container_name = container.name
    connector_id = connector_config["connector_id"]
    
    try:
        logger.info(f"Recreating container {container_name} due to configuration changes")
        
        # Stop and remove the existing container
        if container.status == 'running':
            container.stop(timeout=10)
        container.remove()
        
        # Create the new container with updated configuration
        create_connector_container(client, connector_config)
        
        logger.info(f"Successfully recreated container {container_name}")
        
    except Exception as e:
        logger.error(f"Failed to recreate container {container_name}: {e}")
        raise

def sync_containers(client: docker.DockerClient, config: dict) -> None:
    """Synchronize running containers with the configuration."""
    connector_configs = {p["connector_id"]: p for p in config.get("connectors", [])}
    existing_containers = get_existing_connectors(client)

    # Remove containers for connectors no longer in config
    for connector_id, container in existing_containers.items():
        if connector_id not in connector_configs:
            logger.info(f"Removing container for connector {connector_id} (no longer in config)")
            try:
                container.stop()
                container.remove()
            except Exception as e:
                logger.error(f"Failed to remove container {container.name}: {e}")

    # Create or update containers for configured connectors
    for connector_id, connector_config in connector_configs.items():
        if connector_id in existing_containers:
            container = existing_containers[connector_id]
            
            # Check if container configuration matches the expected configuration
            expected_config = get_expected_container_config(connector_config)
            config_matches, config_message = container_config_matches(container, expected_config)
            
            if not config_matches:
                logger.info(f"Container {container.name} configuration has changed: {config_message}")
                recreate_container(client, container, connector_config)
            else:
                # Configuration matches, check container health
                is_healthy, health_message = check_container_health(container)
                if not is_healthy:
                    logger.warning(f"Container {container.name} is unhealthy: {health_message}")
                    handle_unhealthy_container(client, container, connector_config)
                else:
                    logger.info(f"Container {container.name} is healthy and up-to-date")
        else:
            create_connector_container(client, connector_config)

def main():
    """Main loop that periodically syncs containers with configuration."""
    logger.info("Starting connector manager")
    client = docker.from_env()

    # Ensure network exists at startup
    ensure_network_exists(client)

    while True:
        try:
            config = load_config(CONFIG_PATH)
            sync_containers(client, config)
            logger.info("Container sync completed")
        except Exception as e:
            logger.error(f"Error during sync: {e}")
        
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main() 