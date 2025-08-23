#!/usr/bin/env python3
"""
Redis Object Publisher Script

This script provides functionality to publish Python objects to Redis using various methods:
- Publish to Redis channels (pub/sub)
- Store objects in Redis with keys
- Push objects to Redis lists/queues

Dependencies:
    pip install redis

Usage:
    python redis_publisher.py
"""

import redis
import json
import pickle
import time
import csv
import argparse
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Union


class RedisPublisher:
    """A class to handle publishing objects to Redis."""

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 db: Optional[int] = None, password: Optional[str] = None,
                 decode_responses: bool = True):
        """
        Initialize Redis connection.

        Args:
            host: Redis server host (defaults to REDIS_HOST env var or 'localhost')
            port: Redis server port (defaults to REDIS_PORT env var or 6379)
            db: Redis database number (defaults to REDIS_DB env var or 0)
            password: Redis password (defaults to REDIS_PASSWORD env var if set)
            decode_responses: Whether to decode responses to strings
        """
        # Use environment variables as defaults if not provided
        host = host or os.getenv('REDIS_HOST', 'localhost')
        port = port or int(os.getenv('REDIS_PORT', '6379'))
        db = db or int(os.getenv('REDIS_DB', '0'))
        password = password or os.getenv('REDIS_PASSWORD')

        try:
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=decode_responses
            )
            # Test connection
            self.redis_client.ping()
            print(f"✅ Connected to Redis at {host}:{port}")
        except redis.ConnectionError as e:
            print(f"❌ Failed to connect to Redis: {e}")
            raise

    def publish_to_channel(self, channel: str, obj: Any) -> int:
        """
        Publish an object to a Redis channel (pub/sub pattern).

        Args:
            channel: Redis channel name
            obj: Object to publish

        Returns:
            Number of subscribers that received the message
        """
        try:
            message = json.dumps(obj, default=str)
            subscribers = self.redis_client.publish(channel, message)
            return subscribers
        except Exception as e:
            print(f"❌ Error publishing to channel '{channel}': {e}")
            raise

    def set_object(self, key: str, obj: Any) -> bool:
        """
        Store an object in Redis with a key.

        Args:
            key: Redis key
            obj: Object to store

        Returns:
            True if successful
        """
        try:
            value = json.dumps(obj, default=str)
            result = self.redis_client.set(key, value)
            return result
        except Exception as e:
            print(f"❌ Error storing object with key '{key}': {e}")
            raise

    def add_to_stream(self, stream_name: str, obj: Any,
                     maxlen: Optional[int] = None) -> str:
        """
        Add an object to a Redis stream.

        Args:
            stream_name: Name of the Redis stream
            obj: Object to add
            maxlen: Maximum length of the stream (optional)

        Returns:
            Stream entry ID
        """
        try:
            fields = {'data': json.dumps(obj, default=str)}
            # Add timestamp
            fields['timestamp'] = datetime.now().isoformat()
            entry_id = self.redis_client.xadd(stream_name, fields, maxlen=maxlen)
            return entry_id
        except Exception as e:
            print(f"❌ Error adding to stream '{stream_name}': {e}")
            raise

    def close(self):
        """Close the Redis connection."""
        if hasattr(self, 'redis_client'):
            self.redis_client.close()
            print("🔌 Redis connection closed")


def parse_notes_from_csv(csv_file_path: str) -> List[Dict[str, Any]]:
    """
    Parse note objects from a CSV file.

    Args:
        csv_file_path: Path to the CSV file

    Returns:
        List of note objects with 'note' and 'duration' fields

    CSV Format:
        First column: note (string)
        Second column: duration (integer)
    """
    note_objects = []

    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)

            # Skip header row if it exists
            first_row = next(csv_reader, None)
            if first_row and (first_row[0].lower() in ['note', 'notes', 'description'] or
                            first_row[1].lower() in ['duration', 'time', 'minutes']):
                print(f"📋 Skipping header row: {first_row}")
            else:
                # Process first row as data if it's not a header
                if first_row:
                    try:
                        note_obj = {
                            "note": str(first_row[0]).strip(),
                            "duration": float(first_row[1])
                        }
                        note_objects.append(note_obj)
                    except (ValueError, IndexError) as e:
                        print(f"⚠️  Skipping invalid row: {first_row} - {e}")

            # Process remaining rows
            for row_num, row in enumerate(csv_reader, start=2):
                try:
                    if len(row) >= 2 and row[0].strip() and row[1].strip():
                        note_obj = {
                            "note": str(row[0]).strip(),
                            "duration": float(row[1])
                        }
                        note_objects.append(note_obj)
                    else:
                        print(f"⚠️  Skipping empty or incomplete row {row_num}: {row}")
                except (ValueError, IndexError) as e:
                    print(f"⚠️  Skipping invalid row {row_num}: {row} - {e}")

        print(f"✅ Parsed {len(note_objects)} note objects from {csv_file_path}")
        return note_objects

    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_file_path}")
        return []
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return []


def create_argument_parser():
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        description="Redis Object Publisher - Publish note objects to Redis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python redis_publisher.py songs/sample_notes.csv --redis-type pubsub   # Publish to pub/sub
  python redis_publisher.py songs/sample_notes.csv --redis-type set      # Publish to sets
  python redis_publisher.py songs/sample_notes.csv --redis-type stream   # Publish to streams
        """
    )


    parser.add_argument(
        'csv_file',
        nargs='?',
        help='Path to CSV file'
    )

    # Optional arguments
    parser.add_argument(
        '--redis-type',
        choices=['pubsub', 'set', 'stream'],
        default='pubsub',
        help='Redis publishing type (default: pubsub)'
    )

    # Redis connection arguments
    parser.add_argument(
        '--redis-host',
        default=None,
        help='Redis server host (default: REDIS_HOST env var or localhost)'
    )

    parser.add_argument(
        '--redis-port',
        type=int,
        default=None,
        help='Redis server port (default: REDIS_PORT env var or 6379)'
    )

    parser.add_argument(
        '--redis-db',
        type=int,
        default=None,
        help='Redis database number (default: REDIS_DB env var or 0)'
    )

    parser.add_argument(
        '--redis-password',
        default=None,
        help='Redis password (default: REDIS_PASSWORD env var)'
    )
    parser.add_argument(
        '--speed',
        default=120,
        type=int,
        help="Speed (bpm)"
    )

    return parser


def publish_notes_from_csv_with_connection(csv_file_path: str, redis_type: str = "pubsub",
                                          redis_host: Optional[str] = None,
                                          redis_port: Optional[int] = None,
                                          redis_db: Optional[int] = None,
                                          redis_password: Optional[str] = None,
                                          speed=120/60):
    """Publish note objects from CSV with custom Redis connection settings."""

    # Parse notes from CSV
    note_objects = parse_notes_from_csv(csv_file_path)

    if not note_objects:
        print("❌ No valid note objects found in CSV file")
        return

    # Initialize publisher with custom connection settings
    publisher = RedisPublisher(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password
    )

    print(f"\n📝 Publishing {len(note_objects)} note objects from CSV to Redis using '{redis_type}' type...\n")

    for note_obj in note_objects:
        print(note_obj["note"])
        # adjust speed
        note_obj["duration"] = note_obj["duration"] / speed
        if redis_type == "pubsub":
            publisher.publish_to_channel("music", note_obj)
        elif redis_type == "set":
            publisher.set_object("music", note_obj)
        elif redis_type == "stream":
            publisher.add_to_stream("music_stream", note_obj, maxlen=100)
        time.sleep(note_obj["duration"])

    print(f"\n✅ All {len(note_objects)} note objects published to Redis using '{redis_type}' type!")
    publisher.close()


if __name__ == "__main__":
    parser = create_argument_parser()
    args = parser.parse_args()
    try:
        publish_notes_from_csv_with_connection(
            args.csv_file,
            args.redis_type,
            args.redis_host,
            args.redis_port,
            args.redis_db,
            args.redis_password,
            args.speed/60
        )

    except redis.ConnectionError:
        print("\n⚠️  Redis server is not running or not accessible.")
        print("Please start Redis server with: redis-server")
        print("Or install Redis: https://redis.io/download")
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
