import os
import time
import yaml
import docker
import logging
from typing import Dict, Set, Tuple
from datetime import datetime, timedelta, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.getenv('CONFIG_PATH', 'connectors_config.yaml')
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
    return f"connector_container_{connector_id}"

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
        logs = container.logs(tail=50, timestamps=True).decode('utf-8').strip()
        if "Error:" in logs or "error:" in logs:
            return False, f"Container logs indicate errors: {logs.split('Error:')[-1].split('\\n')[0]}"
            
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
        if container.name.startswith("connector_container_"):
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
            for port in expose_ports:
                # Map container port to host port (same port number)
                ports[f"{port}/udp"] = port  # UDP mapping
                ports[f"{port}/tcp"] = port  # TCP mapping
                exposed_ports_list.append(port)
            
            logger.info(f"Exposing ports {', '.join(exposed_ports_list)} for container {container_name}")
        
        logger.info(f"Creating container {container_name} from image {connector_config['image']}")
        
        container = client.containers.create(
            image=connector_config["image"],
            name=container_name,
            detach=True,
            environment=environment,
            restart_policy={"Name": "on-failure", "MaximumRetryCount": MAX_RESTART_ATTEMPTS},
            network=NETWORK_NAME,
            volumes=volumes,
            ports=ports if ports else None
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
            # Check container health
            is_healthy, message = check_container_health(container)
            if not is_healthy:
                logger.warning(f"Container {container.name} is unhealthy: {message}")
                handle_unhealthy_container(client, container, connector_config)
            else:
                logger.info(f"Container {container.name} is healthy")
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