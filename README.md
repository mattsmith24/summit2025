# Redis Object Publisher

A Python script that provides comprehensive functionality to publish objects to Redis using various methods.

## Features

- **Pub/Sub Publishing**: Publish objects to Redis channels
- **Key-Value Storage**: Store objects with keys and optional expiration
- **List Operations**: Push objects to Redis lists (queues)
- **Set Operations**: Add objects to Redis sets
- **Metadata Support**: Publish objects with additional metadata
- **Multiple Serialization**: Support for JSON and Pickle serialization
- **Error Handling**: Robust error handling and connection management

## Installation

1. Install Python dependencies:
   ```bash
    sudo apt install libportaudio2
   ```

## Run It

```
docker run -p 6379:6379 -d redis
uv run music_player.py --redis-type=poll_stream
```

In another terminal:

```
uv run redis_publisher.py songs/take_on_me.csv --redis-type stream --speed=3
```

## Usage

### Basic Usage

```python
from redis_publisher import RedisPublisher

# Initialize publisher
publisher = RedisPublisher(host='localhost', port=6379)

# Publish to a channel
publisher.publish_to_channel('my_channel', {'message': 'Hello Redis!'})

# Store an object with a key
publisher.set_object('user:123', {'name': 'John', 'age': 30})

# Push to a list/queue
publisher.push_to_list('task_queue', {'task': 'process_data', 'id': 1})

# Add to a set
publisher.add_to_set('unique_items', 'item_1')

# Close connection
publisher.close()
```

### Run Demo

```bash
python redis_publisher.py
```

This will demonstrate all the publishing methods with sample data.

## Methods

### `publish_to_channel(channel, obj, serialization='json')`
Publishes an object to a Redis channel using pub/sub pattern.

### `set_object(key, obj, serialization='json', expiration=None)`
Stores an object in Redis with a key, optionally with expiration.

### `push_to_list(list_name, obj, serialization='json', direction='right')`
Pushes an object to a Redis list (left or right).

### `add_to_set(set_name, obj, serialization='json')`
Adds an object to a Redis set.

### `publish_with_metadata(channel, obj, metadata=None)`
Publishes an object with timestamp and custom metadata.

## Configuration

The `RedisPublisher` class accepts the following parameters:

- `host`: Redis server host (default: 'localhost')
- `port`: Redis server port (default: 6379)
- `db`: Redis database number (default: 0)
- `password`: Redis password (optional)
- `decode_responses`: Whether to decode responses to strings (default: True)

## Serialization Options

- **JSON**: Human-readable, works with basic Python types
- **Pickle**: Binary format, supports complex Python objects

## Error Handling

The script includes comprehensive error handling for:
- Connection failures
- Serialization errors
- Redis operation errors

All errors are logged with descriptive messages and emoji indicators for easy identification.

# Troubleshooting

If you get the error:

```
qt.qpa.plugin: From 6.5.0, xcb-cursor0 or libxcb-cursor0 is needed to load the Qt xcb platform plugin.
qt.qpa.plugin: Could not load the Qt platform plugin "xcb" in "" even though it was found.
This application failed to start because no Qt platform plugin could be initialized. Reinstalling the application may fix this problem.

Available platform plugins are: eglfs, minimalegl, vkkhrdisplay, wayland, minimal, vnc, xcb, wayland-egl, linuxfb, offscreen.
```

Then install `libxcb-cursor-dev`

```bash
sudo apt install libxcb-cursor-dev
```