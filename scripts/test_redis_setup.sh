#!/bin/bash

# Script to start a temporary Redis instance for testing using Docker
# Can be run manually or as part of automated testing

REDIS_PORT=${REDIS_PORT:-6380}
CONTAINER_NAME="redis_test_$$"

echo "Starting temporary Redis instance on port $REDIS_PORT using Docker"
echo "Container name: $CONTAINER_NAME"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "Docker not found. Falling back to native Redis installation."
    
    # Check if Redis is available
    if ! command -v redis-server &> /dev/null; then
        echo "Redis server not found. Please install Redis first."
        echo "On Ubuntu/Debian: sudo apt-get install redis-server"
        echo "On macOS: brew install redis"
        echo "On RHEL/CentOS: sudo yum install redis"
        exit 1
    fi
    
    # Use native Redis
    REDIS_DATA_DIR="/tmp/redis_test_$$"
    mkdir -p "$REDIS_DATA_DIR"
    redis-server --port $REDIS_PORT --dir "$REDIS_DATA_DIR" --daemonize yes --logfile "$REDIS_DATA_DIR/redis.log"
    
    if [ $? -eq 0 ]; then
        echo "Redis started successfully on port $REDIS_PORT"
        echo "Log file: $REDIS_DATA_DIR/redis.log"
        echo "To stop: redis-cli -p $REDIS_PORT shutdown"
        echo "To clean up: rm -rf $REDIS_DATA_DIR"
    else
        echo "Failed to start Redis"
        rm -rf "$REDIS_DATA_DIR"
        exit 1
    fi
else
    # Use Docker
    docker run -d --name $CONTAINER_NAME -p $REDIS_PORT:6379 redis:7-alpine
    
    if [ $? -eq 0 ]; then
        echo "Redis Docker container started successfully on port $REDIS_PORT"
        echo "Container name: $CONTAINER_NAME"
        echo "To stop: docker stop $CONTAINER_NAME"
        echo "To clean up: docker rm $CONTAINER_NAME"
        
        # Wait for Redis to be ready
        echo "Waiting for Redis to be ready..."
        for i in {1..30}; do
            if docker exec $CONTAINER_NAME redis-cli ping | grep -q PONG; then
                echo "Redis is ready!"
                break
            fi
            sleep 1
            if [ $i -eq 30 ]; then
                echo "Timeout waiting for Redis to be ready"
                docker stop $CONTAINER_NAME
                docker rm $CONTAINER_NAME
                exit 1
            fi
        done
    else
        echo "Failed to start Redis Docker container"
        exit 1
    fi
fi

echo ""
echo "Set environment variable for tests:"
echo "export REDIS_TEST_ADDR=localhost:$REDIS_PORT"
