#!/usr/bin/env python3
"""
Mandelbrot Set Generator Host
Divides canvas into 4 quarters and posts coordinates to Redis streams
"""

import redis
import json
import time
from typing import Tuple, List, Dict

class MandelbrotHost:
    def __init__(self, canvas_width: int = 800, canvas_height: int = 600, 
                 redis_host: str = 'localhost', redis_port: int = 6379):
        """
        Initialize the Mandelbrot host with canvas dimensions and Redis connection
        
        Args:
            canvas_width: Width of the canvas in pixels
            canvas_height: Height of the canvas in pixels
            redis_host: Redis server hostname
            redis_port: Redis server port
        """
        self.canvas_width = canvas_width
        self.canvas_height = canvas_height
        
        # Connect to Redis
        try:
            self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            self.redis_client.ping()  # Test connection
            print(f"Connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError:
            print(f"Failed to connect to Redis at {redis_host}:{redis_port}")
            raise
        
        # Stream name for Mandelbrot work distribution
        self.stream_name = "mandelbrot:work"
        self.result_stream_name = "mandelbrot:results"
        
    def get_quarter_coordinates(self) -> List[Dict[str, Tuple[int, int]]]:
        """
        Divide the canvas into 4 quarters and return their coordinates
        
        Returns:
            List of dictionaries containing top_left and bottom_right coordinates for each quarter
        """
        half_width = self.canvas_width // 2
        half_height = self.canvas_height // 2
        
        quarters = [
            {
                "quarter": "top_left",
                "top_left": (0, 0),
                "bottom_right": (half_width, half_height)
            },
            {
                "quarter": "top_right", 
                "top_left": (half_width, 0),
                "bottom_right": (self.canvas_width, half_height)
            },
            {
                "quarter": "bottom_left",
                "top_left": (0, half_height),
                "bottom_right": (half_width, self.canvas_height)
            },
            {
                "quarter": "bottom_right",
                "top_left": (half_width, half_height),
                "bottom_right": (self.canvas_width, self.canvas_height)
            }
        ]
        
        return quarters
    
    def post_quarters_to_redis(self) -> None:
        """
        Post each quarter's coordinates to Redis stream
        """
        quarters = self.get_quarter_coordinates()
        
        print(f"Canvas size: {self.canvas_width}x{self.canvas_height}")
        print(f"Posting {len(quarters)} quarters to Redis stream '{self.stream_name}'")
        
        for quarter in quarters:
            # Prepare the stream entry data
            stream_data = {
                "quarter_name": quarter["quarter"],
                "top_left_x": quarter["top_left"][0],
                "top_left_y": quarter["top_left"][1],
                "bottom_right_x": quarter["bottom_right"][0],
                "bottom_right_y": quarter["bottom_right"][1],
                "timestamp": int(time.time()),
                "canvas_width": self.canvas_width,
                "canvas_height": self.canvas_height
            }
            
            # Add entry to Redis stream
            stream_id = self.redis_client.xadd(self.stream_name, stream_data)
            
            print(f"Posted {quarter['quarter']}: "
                  f"({quarter['top_left'][0]}, {quarter['top_left'][1]}) to "
                  f"({quarter['bottom_right'][0]}, {quarter['bottom_right'][1]}) "
                  f"[Stream ID: {stream_id}]")
    
    def clear_streams(self) -> None:
        """
        Clear the Redis stream (useful for testing)
        """
        try:
            self.redis_client.delete(self.stream_name)
            print(f"Cleared Redis stream '{self.stream_name}'")
            self.redis_client.delete(self.result_stream_name)
            print(f"Cleared Redis stream '{self.result_stream_name}'")
        except Exception as e:
            print(f"Error clearing stream: {e}")
    
    def create_consumer_group(self, group_name: str = "workers") -> None:
        """
        Create a Redis stream consumer group
        
        Args:
            group_name: Name of the consumer group to create
        """
        try:
            # Create consumer group starting from the beginning of the stream
            self.redis_client.xgroup_create(self.stream_name, group_name, id='0', mkstream=True)
            print(f"Created consumer group '{group_name}' for stream '{self.stream_name}'")
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                print(f"Consumer group '{group_name}' already exists")
            else:
                print(f"Error creating consumer group: {e}")
    
    def show_stream_info(self) -> None:
        """
        Display information about the Redis stream
        """
        try:
            stream_info = self.redis_client.xinfo_stream(self.stream_name)
            print(f"\nStream '{self.stream_name}' info:")
            print(f"  Length: {stream_info['length']}")
            print(f"  First entry ID: {stream_info.get('first-entry', 'None')}")
            print(f"  Last entry ID: {stream_info.get('last-entry', 'None')}")
            
            # Show all entries in the stream
            entries = self.redis_client.xrange(self.stream_name)
            print(f"\nStream entries:")
            for entry_id, fields in entries:
                print(f"  {entry_id}: {fields}")
            
            # Show consumer group info
            try:
                groups = self.redis_client.xinfo_groups(self.stream_name)
                if groups:
                    print(f"\nConsumer groups:")
                    for group in groups:
                        print(f"  Group: {group['name']}, Consumers: {group['consumers']}, Pending: {group['pending']}")
                else:
                    print(f"\nNo consumer groups found")
            except redis.ResponseError:
                print(f"\nNo consumer groups found")
                
        except redis.ResponseError as e:
            if "no such key" in str(e).lower():
                print(f"Stream '{self.stream_name}' does not exist yet")
            else:
                print(f"Error getting stream info: {e}")

def main():
    """
    Main function to demonstrate the Mandelbrot host functionality
    """
    print("Mandelbrot Set Generator Host")
    print("=" * 40)
    
    # Create host with default 800x600 canvas
    host = MandelbrotHost(canvas_width=800, canvas_height=600)
    
    # Clear any existing stream data
    host.clear_streams()
    
    # Post quarters to Redis
    host.post_quarters_to_redis()
    
    # Create consumer group for workers
    host.create_consumer_group("workers")
    
    # Show stream information
    host.show_stream_info()
    
    print("\nQuarters posted to Redis stream successfully!")
    print(f"Workers can now consume from stream '{host.stream_name}' using consumer group 'workers'")

if __name__ == "__main__":
    main()