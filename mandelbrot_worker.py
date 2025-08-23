#!/usr/bin/env python3
"""
Mandelbrot Set Worker
Consumes work from Redis stream, calculates Mandelbrot colors, and subdivides regions
"""

import redis
import time
import uuid
import math
from typing import Tuple, Dict, Optional

class MandelbrotWorker:
    def __init__(self, worker_id: str = None, redis_host: str = 'localhost', redis_port: int = 6379):
        """
        Initialize the Mandelbrot worker
        
        Args:
            worker_id: Unique identifier for this worker instance
            redis_host: Redis server hostname
            redis_port: Redis server port
        """
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        
        # Connect to Redis
        try:
            self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            self.redis_client.ping()  # Test connection
            print(f"Worker {self.worker_id} connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError:
            print(f"Failed to connect to Redis at {redis_host}:{redis_port}")
            raise
        
        # Stream names
        self.work_stream = "mandelbrot:work"
        self.result_stream = "mandelbrot:results"
        self.consumer_group = "workers"
        
        # Mandelbrot parameters
        self.max_iterations = 100
        self.escape_radius = 2.0
        
        # Complex plane bounds (standard Mandelbrot view)
        self.complex_min_x = -2.5
        self.complex_max_x = 1.5
        self.complex_min_y = -2.0
        self.complex_max_y = 2.0
    
    def pixel_to_complex(self, pixel_x: int, pixel_y: int, canvas_width: int, canvas_height: int) -> complex:
        """
        Convert pixel coordinates to complex plane coordinates
        
        Args:
            pixel_x: X coordinate in pixels
            pixel_y: Y coordinate in pixels
            canvas_width: Total canvas width
            canvas_height: Total canvas height
            
        Returns:
            Complex number representing the point in the complex plane
        """
        # Map pixel coordinates to complex plane
        real = self.complex_min_x + (pixel_x / canvas_width) * (self.complex_max_x - self.complex_min_x)
        imag = self.complex_min_y + (pixel_y / canvas_height) * (self.complex_max_y - self.complex_min_y)
        return complex(real, imag)
    
    def mandelbrot_iterations(self, c: complex) -> int:
        """
        Calculate the number of iterations for a complex point in the Mandelbrot set
        
        Args:
            c: Complex point to test
            
        Returns:
            Number of iterations before escape (or max_iterations if it doesn't escape)
        """
        z = 0
        for n in range(self.max_iterations):
            if abs(z) > self.escape_radius:
                return n
            z = z * z + c
        return self.max_iterations
    
    def iterations_to_color(self, iterations: int) -> Tuple[int, int, int]:
        """
        Convert iteration count to RGB color
        
        Args:
            iterations: Number of iterations before escape
            
        Returns:
            RGB color tuple (r, g, b) with values 0-255
        """
        if iterations == self.max_iterations:
            # Point is in the set - black
            return (0, 0, 0)
        
        # Create a color gradient based on iteration count
        # Use a smooth color transition
        t = iterations / self.max_iterations
        
        # Create a colorful gradient
        r = int(255 * (0.5 + 0.5 * math.sin(3.0 * t)))
        g = int(255 * (0.5 + 0.5 * math.sin(3.0 * t + 2.0)))
        b = int(255 * (0.5 + 0.5 * math.sin(3.0 * t + 4.0)))
        
        return (r, g, b)
    
    def calculate_region_color(self, top_left_x: int, top_left_y: int, 
                             bottom_right_x: int, bottom_right_y: int,
                             canvas_width: int, canvas_height: int) -> Tuple[int, int, int]:
        """
        Calculate the Mandelbrot color for the center of a region
        
        Args:
            top_left_x, top_left_y: Top-left corner coordinates
            bottom_right_x, bottom_right_y: Bottom-right corner coordinates
            canvas_width, canvas_height: Canvas dimensions
            
        Returns:
            RGB color tuple for the region center
        """
        # Calculate center coordinates
        center_x = (top_left_x + bottom_right_x) // 2
        center_y = (top_left_y + bottom_right_y) // 2
        
        # Convert to complex plane
        c = self.pixel_to_complex(center_x, center_y, canvas_width, canvas_height)
        
        # Calculate iterations and convert to color
        iterations = self.mandelbrot_iterations(c)
        return self.iterations_to_color(iterations)
    
    def subdivide_region(self, top_left_x: int, top_left_y: int,
                        bottom_right_x: int, bottom_right_y: int) -> list:
        """
        Subdivide a region into 4 quarters
        
        Args:
            top_left_x, top_left_y: Top-left corner coordinates
            bottom_right_x, bottom_right_y: Bottom-right corner coordinates
            
        Returns:
            List of 4 quarter regions with their coordinates
        """
        mid_x = (top_left_x + bottom_right_x) // 2
        mid_y = (top_left_y + bottom_right_y) // 2

        if mid_x == top_left_x and mid_y == top_left_y:
            return None
        
        quarters = [
            {
                "quarter": "top_left",
                "top_left_x": top_left_x,
                "top_left_y": top_left_y,
                "bottom_right_x": mid_x,
                "bottom_right_y": mid_y
            },
            {
                "quarter": "top_right",
                "top_left_x": mid_x,
                "top_left_y": top_left_y,
                "bottom_right_x": bottom_right_x,
                "bottom_right_y": mid_y
            },
            {
                "quarter": "bottom_left",
                "top_left_x": top_left_x,
                "top_left_y": mid_y,
                "bottom_right_x": mid_x,
                "bottom_right_y": bottom_right_y
            },
            {
                "quarter": "bottom_right",
                "top_left_x": mid_x,
                "top_left_y": mid_y,
                "bottom_right_x": bottom_right_x,
                "bottom_right_y": bottom_right_y
            }
        ]
        
        return quarters
    
    def post_result_to_stream(self, region_data: dict, color: Tuple[int, int, int]) -> None:
        """
        Post region result to the results stream
        
        Args:
            region_data: Original region data from work stream
            color: Calculated RGB color for the region
        """
        result_data = {
            "worker_id": self.worker_id,
            "quarter_name": region_data.get("quarter_name", "unknown"),
            "top_left_x": region_data["top_left_x"],
            "top_left_y": region_data["top_left_y"],
            "bottom_right_x": region_data["bottom_right_x"],
            "bottom_right_y": region_data["bottom_right_y"],
            "color_r": color[0],
            "color_g": color[1],
            "color_b": color[2],
            "timestamp": int(time.time()),
            "canvas_width": region_data["canvas_width"],
            "canvas_height": region_data["canvas_height"]
        }
        
        stream_id = self.redis_client.xadd(self.result_stream, result_data)
        print(f"Posted result to {self.result_stream}: RGB{color} [Stream ID: {stream_id}]")
    
    def post_subdivisions_to_work_stream(self, quarters: list, canvas_width: int, canvas_height: int) -> bool:
        """
        Post subdivided quarters back to the work stream
        
        Args:
            quarters: List of quarter regions
            canvas_width, canvas_height: Canvas dimensions
            
        Returns:
            True if any subdivisions were posted, False if all were too small (1 pixel)
        """
        posted_count = 0
        
        for quarter in quarters:
            # Skip regions that are too small (1x1 pixel or smaller)
            width = quarter["bottom_right_x"] - quarter["top_left_x"]
            height = quarter["bottom_right_y"] - quarter["top_left_y"]
            
            if width <= 1 or height <= 1:
                print(f"Skipping {quarter['quarter']} subdivision - too small ({width}x{height})")
                continue
            
            work_data = {
                "quarter_name": f"{quarter['quarter']}_sub",
                "top_left_x": quarter["top_left_x"],
                "top_left_y": quarter["top_left_y"],
                "bottom_right_x": quarter["bottom_right_x"],
                "bottom_right_y": quarter["bottom_right_y"],
                "timestamp": int(time.time()),
                "canvas_width": canvas_width,
                "canvas_height": canvas_height,
                "subdivided_by": self.worker_id
            }
            
            stream_id = self.redis_client.xadd(self.work_stream, work_data)
            print(f"Posted subdivision {quarter['quarter']} back to work stream [Stream ID: {stream_id}]")
            posted_count += 1
        
        return posted_count > 0
    
    def process_work_item(self, stream_id: str, fields: dict) -> bool:
        """
        Process a single work item from the stream
        
        Args:
            stream_id: Redis stream entry ID
            fields: Work item data fields
            
        Returns:
            True if subdivisions were posted (continue processing), False if all subdivisions were 1 pixel (should exit)
        """
        print(f"\nProcessing work item {stream_id}: {fields.get('quarter_name', 'unknown')}")
        
        # Extract coordinates and canvas dimensions
        top_left_x = int(fields["top_left_x"])
        top_left_y = int(fields["top_left_y"])
        bottom_right_x = int(fields["bottom_right_x"])
        bottom_right_y = int(fields["bottom_right_y"])
        canvas_width = int(fields["canvas_width"])
        canvas_height = int(fields["canvas_height"])
        
        # Calculate color for the center of this region
        color = self.calculate_region_color(
            top_left_x, top_left_y, bottom_right_x, bottom_right_y,
            canvas_width, canvas_height
        )
        
        print(f"Calculated color RGB{color} for region center")
        
        # Post result to results stream
        self.post_result_to_stream(fields, color)
        
        # Subdivide the region into quarters
        quarters = self.subdivide_region(top_left_x, top_left_y, bottom_right_x, bottom_right_y)
        
        # Post subdivisions back to work stream
        if quarters:
            self.post_subdivisions_to_work_stream(quarters, canvas_width, canvas_height)
        
        # Acknowledge the message
        self.redis_client.xack(self.work_stream, self.consumer_group, stream_id)
        print(f"Acknowledged work item {stream_id}")
        
        if not quarters:
            print("All subdivisions are 1 pixel or smaller - no more work to generate")
        
        return quarters
    
    def run(self) -> None:
        """
        Main worker loop - consume and process messages from the work stream
        
        Args:
            max_messages: Maximum number of messages to process before stopping
        """
        print(f"Worker {self.worker_id} starting to consume from stream '{self.work_stream}'")
        print(f"Consumer group: '{self.consumer_group}'")
        
        processed_count = 0
        
        while True:
            try:
                # Read messages from the stream using consumer group
                messages = self.redis_client.xreadgroup(
                    self.consumer_group,
                    self.worker_id,
                    {self.work_stream: '>'},
                    count=1,
                    block=5000  # Block for 5 seconds waiting for messages
                )
                
                if not messages:
                    print("No new messages, time to die...")
                    break
                
                # Process each message
                for stream_name, stream_messages in messages:
                    for stream_id, fields in stream_messages:
                        has_subdivisions = self.process_work_item(stream_id, fields)
                        processed_count += 1
                        
                        # Exit if no more subdivisions can be generated (all are 1 pixel)
                        if not has_subdivisions:
                            print(f"Worker {self.worker_id} exiting - all subdivisions are 1 pixel size")
                            return
                        
            except KeyboardInterrupt:
                print(f"\nWorker {self.worker_id} interrupted by user")
                break
            except Exception as e:
                print(f"Error processing messages: {e}")
                time.sleep(1)
        
        print(f"Worker {self.worker_id} processed {processed_count} messages and is stopping")

def main():
    """
    Main function to run the Mandelbrot worker
    """
    print("Mandelbrot Set Worker")
    print("=" * 30)
    
    # Create and run worker
    worker = MandelbrotWorker()
    
    # Process up to 20 work items
    worker.run()

if __name__ == "__main__":
    main()