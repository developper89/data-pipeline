#!/bin/bash
# docker_recovery.sh

echo "Checking Docker status..."

if docker info >/dev/null 2>&1; then
    echo "✅ Docker is running normally"
    exit 0
fi

echo "❌ Docker daemon is not responding"
echo "Attempting to restart Docker..."

# On macOS, restart Docker Desktop
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Restarting Docker Desktop..."
    osascript -e 'quit app "Docker"'
    sleep 5
    open -a Docker
    
    echo "Waiting for Docker to start..."
    for i in {1..30}; do
        if docker info >/dev/null 2>&1; then
            echo "✅ Docker is now running"
            exit 0
        fi
        echo "Waiting... ($i/30)"
        sleep 2
    done
    
    echo "❌ Docker failed to start automatically"
    echo "Please manually restart Docker Desktop"
else
    echo "Please restart Docker using your system's method"
fi