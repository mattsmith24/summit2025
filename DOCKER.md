# Docker Setup for Redis Publisher

This document explains how to run the Redis Publisher in a containerized environment using Docker and Docker Compose.

## 🐳 Quick Start with Docker Compose

The easiest way to run the Redis Publisher with Redis is using Docker Compose:

```bash
# Build and run both Redis and the publisher
docker-compose up --build

# Run in background
docker-compose up -d --build

# View logs
docker-compose logs -f redis-publisher

# Stop services
docker-compose down
```

## 📋 Docker Compose Configuration

The `docker-compose.yml` includes:

- **Redis Server**: Redis 7 Alpine with persistence
- **Redis Publisher**: Your application container
- **Shared Network**: Both containers on `redis-network`
- **Health Checks**: Ensures Redis is ready before starting publisher

### Environment Variables

The publisher container uses these environment variables:

```yaml
environment:
  - REDIS_HOST=redis          # Redis container hostname
  - REDIS_PORT=6379          # Redis port
  - REDIS_DB=0               # Redis database number
```

## 🔧 Manual Docker Commands

### Build the Image

```bash
docker build -t redis-publisher .
```

### Run Redis Container

```bash
docker run -d --name redis-server \
  --network redis-net \
  -p 6379:6379 \
  redis:7-alpine
```

### Run Publisher Container

```bash
# Basic usage
docker run --rm \
  --network redis-net \
  -e REDIS_HOST=redis-server \
  -v $(pwd)/sample_notes.csv:/app/sample_notes.csv:ro \
  redis-publisher sample_notes.csv --redis-type pubsub

# With custom Redis settings
docker run --rm \
  --network redis-net \
  -e REDIS_HOST=redis-server \
  -e REDIS_PORT=6379 \
  -e REDIS_DB=0 \
  -v $(pwd)/my_notes.csv:/app/my_notes.csv:ro \
  redis-publisher my_notes.csv --redis-type stream
```

## 📁 Custom CSV Files

To use your own CSV file:

### Option 1: Volume Mount
```bash
docker run --rm \
  --network redis-net \
  -e REDIS_HOST=redis-server \
  -v /path/to/your/notes.csv:/app/notes.csv:ro \
  redis-publisher notes.csv --redis-type set
```

### Option 2: Update docker-compose.yml
```yaml
services:
  redis-publisher:
    # ... other config
    volumes:
      - ./your_custom_notes.csv:/app/notes.csv:ro
    command: ["python", "redis_publisher.py", "notes.csv", "--redis-type", "stream"]
```

## 🚀 Publishing Types

### Pub/Sub Mode
```bash
docker-compose run redis-publisher python redis_publisher.py sample_notes.csv --redis-type pubsub
```
- Publishes to `csv_notes_channel`
- Includes metadata (timestamp, source, row number)

### Sets Mode
```bash
docker-compose run redis-publisher python redis_publisher.py sample_notes.csv --redis-type set
```
- Adds to `csv_notes_set`
- Categorizes by duration: `short_tasks_set`, `long_tasks_set`

### Streams Mode
```bash
docker-compose run redis-publisher python redis_publisher.py sample_notes.csv --redis-type stream
```
- Publishes to `csv_notes_stream`
- Priority streams: `high_priority_stream`, `normal_priority_stream`

## 🔍 Monitoring and Debugging

### View Redis Data

Connect to Redis container:
```bash
docker exec -it redis-server redis-cli

# Check pub/sub channels
PUBSUB CHANNELS

# Check sets
SMEMBERS csv_notes_set

# Check streams
XRANGE csv_notes_stream - +
```

### View Application Logs

```bash
# Follow logs
docker-compose logs -f redis-publisher

# View specific container logs
docker logs redis-publisher
```

### Debug Connection Issues

```bash
# Test Redis connectivity from publisher container
docker-compose exec redis-publisher python -c "
import redis
r = redis.Redis(host='redis', port=6379, db=0)
print('Redis ping:', r.ping())
"
```

## ⚙️ Advanced Configuration

### Custom Redis Configuration

Create a custom Redis config and mount it:

```yaml
services:
  redis:
    image: redis:7-alpine
    volumes:
      - ./redis.conf:/usr/local/etc/redis/redis.conf:ro
    command: redis-server /usr/local/etc/redis/redis.conf
```

### Multiple Publishers

Run multiple publisher instances with different CSV files:

```yaml
services:
  redis-publisher-1:
    build: .
    depends_on: [redis]
    networks: [redis-network]
    environment:
      - REDIS_HOST=redis
    volumes:
      - ./notes1.csv:/app/notes.csv:ro
    command: ["python", "redis_publisher.py", "notes.csv", "--redis-type", "pubsub"]

  redis-publisher-2:
    build: .
    depends_on: [redis]
    networks: [redis-network]
    environment:
      - REDIS_HOST=redis
    volumes:
      - ./notes2.csv:/app/notes.csv:ro
    command: ["python", "redis_publisher.py", "notes.csv", "--redis-type", "stream"]
```

## 🔐 Security Considerations

### Redis Authentication

Add password protection:

```yaml
services:
  redis:
    image: redis:7-alpine
    command: redis-server --requirepass yourpassword
    
  redis-publisher:
    environment:
      - REDIS_HOST=redis
      - REDIS_PASSWORD=yourpassword
```

### Non-root User

The Dockerfile already creates and uses a non-root `app` user for security.

## 📊 Performance Tips

1. **Batch Processing**: For large CSV files, consider processing in batches
2. **Connection Pooling**: Redis-py handles connection pooling automatically
3. **Memory Limits**: Set Docker memory limits for production use:

```yaml
services:
  redis-publisher:
    deploy:
      resources:
        limits:
          memory: 512M
        reservations:
          memory: 256M
```

## 🛠️ Troubleshooting

### Common Issues

1. **Connection Refused**: Ensure Redis container is running and healthy
2. **File Not Found**: Check CSV file path and volume mounts
3. **Permission Denied**: Ensure CSV file has read permissions

### Health Checks

The Docker Compose includes health checks. Check status:

```bash
docker-compose ps
```

### Reset Everything

```bash
# Stop and remove all containers, networks, and volumes
docker-compose down -v
docker system prune -f
```
