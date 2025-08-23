#!/usr/bin/env python3
"""
Music Player Script

This script subscribes to Redis channel "music" and plays musical notes based on:
- note: The musical note name (e.g., "C4", "A#3", "Bb4")
- duration: The duration to play the note (in seconds or beats)

Dependencies:
    pip install redis numpy sounddevice

Usage:
    python music_player.py [--redis-host HOST] [--redis-port PORT]
"""

import redis
import json
import time
import threading
import argparse
import os
import sys
from typing import Dict, Optional, Any
from datetime import datetime

# Try to import audio libraries
try:
    import numpy as np
    import sounddevice as sd
    AUDIO_AVAILABLE = True
except ImportError:
    AUDIO_AVAILABLE = False
    print("⚠️  Audio libraries not available. Install with: pip install numpy sounddevice")


class MusicPlayer:
    """A class to play musical notes from Redis channel."""

    # Musical note frequencies (in Hz)
    NOTE_FREQUENCIES = {
        # Octave 3
        'C3': 130.81, 'C#3': 138.59, 'DB3': 138.59, 'D3': 146.83, 'D#3': 155.56, 'EB3': 155.56,
        'E3': 164.81, 'F3': 174.61, 'F#3': 185.00, 'GB3': 185.00, 'G3': 196.00, 'G#3': 207.65, 'AB3': 207.65,
        'A3': 220.00, 'A#3': 233.08, 'BB3': 233.08, 'B3': 246.94,

        # Octave 4 (Middle octave)
        'C4': 261.63, 'C#4': 277.18, 'DB4': 277.18, 'D4': 293.66, 'D#4': 311.13, 'EB4': 311.13,
        'E4': 329.63, 'F4': 349.23, 'F#4': 369.99, 'GB4': 369.99, 'G4': 392.00, 'G#4': 415.30, 'AB4': 415.30,
        'A4': 440.00, 'A#4': 466.16, 'BB4': 466.16, 'B4': 493.88,

        # Octave 5
        'C5': 523.25, 'C#5': 554.37, 'DB5': 554.37, 'D5': 587.33, 'D#5': 622.25, 'EB5': 622.25,
        'E5': 659.25, 'F5': 698.46, 'F#5': 739.99, 'GB5': 739.99, 'G5': 783.99, 'G#5': 830.61, 'AB5': 830.61,
        'A5': 880.00, 'A#5': 932.33, 'BB5': 932.33, 'B5': 987.77,

        # Octave 6
        'C6': 1046.50, 'C#6': 1108.73, 'DB6': 1108.73, 'D6': 1174.66, 'D#6': 1244.51, 'EB6': 1244.51,
        'E6': 1318.51, 'F6': 1396.91, 'F#6': 1479.98, 'GB6': 1479.98, 'G6': 1567.98, 'G#6': 1661.22, 'AB6': 1661.22,
        'A6': 1760.00, 'A#6': 1864.66, 'BB6': 1864.66, 'B6': 1975.53,
    }

    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379,
                 redis_db: int = 0, redis_password: Optional[str] = None,
                 sample_rate: int = 44100):
        """
        Initialize the music player.

        Args:
            redis_host: Redis server host
            redis_port: Redis server port
            redis_db: Redis database number
            redis_password: Redis password (if required)
            sample_rate: Audio sample rate in Hz
        """
        # Use environment variables as defaults if not provided
        self.redis_host = redis_host or os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = redis_port or int(os.getenv('REDIS_PORT', '6379'))
        self.redis_db = redis_db or int(os.getenv('REDIS_DB', '0'))
        self.redis_password = redis_password or os.getenv('REDIS_PASSWORD')

        self.sample_rate = sample_rate
        self.is_playing = False
        self.stop_event = threading.Event()

        # keep track of the last id we got from the stream
        self.last_stream_id = None

        # Initialize Redis connection
        self._connect_to_redis()

        # Check audio availability
        self.audio_available = AUDIO_AVAILABLE
        if not AUDIO_AVAILABLE:
            print("🔇 Running in silent mode (audio libraries not available)")
        else:
            # Test audio device availability
            try:
                devices = sd.query_devices()
                if not devices or len(devices) == 0:
                    print("🔇 Running in silent mode (no audio devices found)")
                    self.audio_available = False
                else:
                    print(f"🔊 Audio available - found {len(devices)} audio devices")
            except Exception as e:
                print(f"🔇 Running in silent mode (audio device error: {e})")
                self.audio_available = False
        if self.audio_available:
            try:
                # Try to find a suitable output device
                default_device = sd.default.device
                if default_device is None or default_device[1] == -1:
                    # Try to find any available output device
                    output_devices = [i for i, dev in enumerate(devices)
                                    if dev['max_output_channels'] > 0]
                    if output_devices:
                        sd.default.device = (None, output_devices[0])
                        print(f"🔊 Using audio device: {devices[output_devices[0]]['name']}")
                    else:
                        print("⚠️  No output devices available, running in silent mode")

            except Exception as e:
                print(f"⚠️  Audio error: {e}")
                print("🔇 Falling back to silent mode")


    def _connect_to_redis(self):
        """Connect to Redis server."""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                decode_responses=True
            )
            # Test connection
            self.redis_client.ping()
            print(f"✅ Connected to Redis at {self.redis_host}:{self.redis_port}")
        except redis.ConnectionError as e:
            print(f"❌ Failed to connect to Redis: {e}")
            raise

    def close(self):
        self.redis_client.close()

    def generate_tone(self, frequency: float, duration: float, volume: float = 0.3) -> np.ndarray:
        """
        Generate a sine wave tone for a given frequency and duration.

        Args:
            frequency: Frequency in Hz
            duration: Duration in seconds
            volume: Volume (0.0 to 1.0)

        Returns:
            Audio samples as numpy array
        """
        try:
            if not AUDIO_AVAILABLE:
                return np.array([])

            total_samples = int(self.sample_rate * duration)

            # Generate time array
            t = np.linspace(0, duration, total_samples, False)

            # Generate sine wave with envelope (fade in/out to avoid clicks)
            wave = np.sin(2 * np.pi * frequency * t)

            fade_samples = int(0.005 * self.sample_rate)
            gap_samples = int(0.03 * self.sample_rate)
            if gap_samples + 2 * fade_samples > total_samples:
                gap_samples = 0
            envelope = np.linspace(volume, volume, total_samples)
            envelope[:fade_samples] = np.linspace(0, volume, fade_samples)
            if gap_samples > 0:
                envelope[-(fade_samples+gap_samples):-gap_samples] = np.linspace(volume, 0, fade_samples)
                envelope[-gap_samples:] = np.linspace(0, 0, gap_samples)
            else:
                envelope[-fade_samples:] = np.linspace(volume, 0, fade_samples)
            return (wave * envelope).astype(np.float32)
        except Exception as e:
            print(f"generate_tone error: {e}")
            raise e

    def play_note(self, note_name: str, duration: float):
        """
        Play a musical note.

        Args:
            note_name: Note name (e.g., "C4", "A#3", "Bb4")
            duration: Duration in seconds
        """
        # Clean up note name
        note_name = note_name.strip().upper()

        if note_name == "R":  # rest
            time.sleep(duration)
            return

        # Get frequency for the note
        frequency = self.NOTE_FREQUENCIES.get(note_name)

        if frequency is None:
            print(f"⚠️  Unknown note: {note_name}")
            # Try to parse as a simple note without octave (default to octave 4)
            simple_note = note_name.replace('#', '#').replace('B', 'b')
            if len(simple_note) <= 2 and not any(char.isdigit() for char in simple_note):
                note_name = simple_note + '4'
                frequency = self.NOTE_FREQUENCIES.get(note_name)

        if frequency is None:
            print(f"❌ Cannot play note: {note_name}")
            time.sleep(duration)  # Still wait for the duration
            return

        print(f"🎵 Playing {note_name} ({frequency:.2f} Hz) for {duration:.2f}s")

        if self.audio_available:

            # Generate and play the tone
            tone = self.generate_tone(frequency, duration)
            if len(tone) > 0:
                try:
                    sd.play(tone, self.sample_rate)
                    sd.wait()  # Wait until the note finishes playing
                except Exception as e:
                    print(f"Sound device exception: {e}")
        else:
            # Silent mode - just wait
            time.sleep(duration)

    def parse_note_message(self, message: str) -> Optional[Dict[str, Any]]:
        """
        Parse a JSON message from Redis.

        Args:
            message: JSON string from Redis

        Returns:
            Parsed note object or None if invalid
        """
        try:
            note_obj = json.loads(message)

            if not isinstance(note_obj, dict):
                print(f"⚠️  Message is not a JSON object: {message}")
                return None

            if 'note' not in note_obj or 'duration' not in note_obj:
                print(f"⚠️  Message missing 'note' or 'duration': {note_obj}")
                return None

            return note_obj

        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON message: {message} - {e}")
            return None

    def play_note_from_obj(self, note_obj):
        # Extract note and duration
        note_name = str(note_obj['note'])
        try:
            duration = float(note_obj['duration'])
        except (ValueError, TypeError):
            print(f"⚠️  Invalid duration: {note_obj['duration']}")

        # Play the note
        self.play_note(note_name, duration)


    def pubsub_play(self, channel):
        try:
            # Create pubsub object
            pubsub = self.redis_client.pubsub()
            pubsub.subscribe(channel)

            self.is_playing = True

            # Listen for messages
            for message in pubsub.listen():
                if self.stop_event.is_set():
                    break

                # Skip subscription confirmation message
                if message['type'] != 'message':
                    continue

                # Parse the message
                note_obj = self.parse_note_message(message['data'])
                if note_obj is None:
                    continue
                self.play_note_from_obj(note_obj)


        except KeyboardInterrupt:
            print("\n🛑 Stopping music player...")
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            self.is_playing = False
            try:
                pubsub.close()
            except:
                pass

    def poll_play(self, channel, interval):
        try:
            self.is_playing = True
            while True:
                channel_value = self.redis_client.get(channel)
                if channel_value is None:
                    time.sleep(interval)
                    continue
                note_obj = self.parse_note_message(channel_value)
                if note_obj is None:
                    continue
                self.play_note_from_obj(note_obj)
                time.sleep(interval)

        except KeyboardInterrupt:
            print("\n🛑 Stopping music player...")
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            self.is_playing = False

    def stream_play(self, channel, id):
        try:
            self.is_playing = True
            while True:
                if id is None:
                    id = 0
                print(f"Reading from Redis stream {channel} at id {id}")
                messages = self.redis_client.xread(streams={channel: id}, count=100, block=1000)
                for message in messages:
                    message_channel = message[0]
                    if message_channel != channel:
                        print(f"Got the wrong channel {message_channel}")
                        continue
                    message_payload = message[1]
                    for message_item in message_payload:
                        id = message_item[0]
                        self.last_stream_id = id
                        data = message_item[1]['data']
                        note_obj = self.parse_note_message(data)
                        if note_obj is None:
                            continue
                        self.play_note_from_obj(note_obj)
        except KeyboardInterrupt:
            print("\n🛑 Stopping music player...")
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            self.is_playing = False
            print(f"Last stream ID: {self.last_stream_id}")



def create_argument_parser():
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        description="Music Player - Play musical notes from Redis channel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python music_player.py                           # Subscribe to 'music' channel
  python music_player.py --redis-host redis       # Connect to Redis container
        """
    )

    # Optional arguments
    parser.add_argument(
        '--channel',
        default='music',
        help='Redis channel to subscribe to (default: music)'
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
        '--sample-rate',
        type=int,
        default=44100,
        help='Audio sample rate in Hz (default: 44100)'
    )
    parser.add_argument(
        '--redis-type',
        default='pubsub',
        help='Redis type: poll, pubsub or stream'
    )
    parser.add_argument(
        '--poll-interval',
        type=float,
        default=0.5,
        help='How often to poll redis'
    )
    parser.add_argument(
        '--stream-from-id',
        type=int,
        default=0,
        help='How often to poll redis'
    )

    return parser


if __name__ == "__main__":
    parser = create_argument_parser()
    args = parser.parse_args()

    try:
        # Initialize music player
        player = MusicPlayer(
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_db=args.redis_db,
            redis_password=args.redis_password,
            sample_rate=args.sample_rate
        )

        # Subscribe and play mode
        print(f"🎧 Subscribing to Redis channel '{args.channel}'...")
        print("🎵 Waiting for musical notes... (Press Ctrl+C to stop)")
        if args.redis_type == 'pubsub':
            player.pubsub_play(args.channel)
        elif args.redis_type == 'poll':
            player.poll_play(args.channel, args.poll_interval)
        elif args.redis_type == 'poll_stream':
            channel = args.channel
            # We substitute the channel because the "music" channel definitely won't work.
            if channel == "music":
                channel = "music_stream"
            player.stream_play(channel, args.stream_from_id)

    except redis.ConnectionError:
        print("\n⚠️  Redis server is not running or not accessible.")
        print("Please start Redis server with: redis-server")
        print("Or install Redis: https://redis.io/download")
    except KeyboardInterrupt:
        print("\n🛑 Music player interrupted by user")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
    finally:
        if 'player' in locals():
            player.close()
