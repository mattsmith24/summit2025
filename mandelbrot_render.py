#!/usr/bin/env python3
"""
Mandelbrot Set Renderer
Reads results from Redis stream and displays as a raster image in a window
"""

import redis
import numpy as np
import pygame
import time
import threading
from typing import Dict, Tuple, Optional

class MandelbrotRenderer:
    def __init__(self, canvas_width: int = 800, canvas_height: int = 600,
                 redis_host: str = 'localhost', redis_port: int = 6379):
        """
        Initialize the Mandelbrot renderer
        
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
            print(f"Renderer connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError:
            print(f"Failed to connect to Redis at {redis_host}:{redis_port}")
            raise
        
        # Stream name for results
        self.result_stream = "mandelbrot:results"
        
        # Initialize pygame
        pygame.init()
        self.screen = pygame.display.set_mode((canvas_width, canvas_height))
        pygame.display.set_caption("Mandelbrot Set - Distributed Rendering")
        
        # Create image array (RGB)
        self.image_array = np.zeros((canvas_height, canvas_width, 3), dtype=np.uint8)
        
        # Threading control
        self.running = True
        self.lock = threading.Lock()
        
        # Statistics
        self.regions_rendered = 0
        self.last_update_time = time.time()
        
    def fill_region(self, top_left_x: int, top_left_y: int, 
                   bottom_right_x: int, bottom_right_y: int, 
                   color: Tuple[int, int, int]) -> None:
        """
        Fill a rectangular region with the specified color
        
        Args:
            top_left_x, top_left_y: Top-left corner coordinates
            bottom_right_x, bottom_right_y: Bottom-right corner coordinates
            color: RGB color tuple
        """
        # Ensure coordinates are within bounds
        top_left_x = max(0, min(top_left_x, self.canvas_width - 1))
        top_left_y = max(0, min(top_left_y, self.canvas_height - 1))
        bottom_right_x = max(0, min(bottom_right_x, self.canvas_width))
        bottom_right_y = max(0, min(bottom_right_y, self.canvas_height))
        
        # Fill the region in the image array
        with self.lock:
            self.image_array[top_left_y:bottom_right_y, top_left_x:bottom_right_x] = color
            self.regions_rendered += 1
    
    def process_result_entry(self, entry_id: str, fields: dict) -> None:
        """
        Process a single result entry from the Redis stream
        
        Args:
            entry_id: Redis stream entry ID
            fields: Result data fields
        """
        try:
            # Extract coordinates and color
            top_left_x = int(fields["top_left_x"])
            top_left_y = int(fields["top_left_y"])
            bottom_right_x = int(fields["bottom_right_x"])
            bottom_right_y = int(fields["bottom_right_y"])
            
            color_r = int(fields["color_r"])
            color_g = int(fields["color_g"])
            color_b = int(fields["color_b"])
            
            color = (color_r, color_g, color_b)
            
            # Fill the region with the calculated color
            self.fill_region(top_left_x, top_left_y, bottom_right_x, bottom_right_y, color)
            
            # Log progress
            worker_id = fields.get("worker_id", "unknown")
            quarter_name = fields.get("quarter_name", "unknown")
            
            region_size = (bottom_right_x - top_left_x) * (bottom_right_y - top_left_y)
            print(f"Rendered {quarter_name} from {worker_id}: "
                  f"({top_left_x},{top_left_y})-({bottom_right_x},{bottom_right_y}) "
                  f"RGB{color} [{region_size} pixels]")
            
        except (KeyError, ValueError) as e:
            print(f"Error processing result entry {entry_id}: {e}")
    
    def read_results_from_stream(self) -> None:
        """
        Continuously read results from the Redis stream in a separate thread
        """
        print(f"Starting to read results from stream '{self.result_stream}'")
        last_id = "0"  # Start from the beginning
        
        while self.running:
            try:
                # Read new entries from the stream
                entries = self.redis_client.xread({self.result_stream: last_id}, 
                                                count=10, block=1000)
                
                if not entries:
                    continue
                
                # Process each entry
                for stream_name, stream_entries in entries:
                    for entry_id, fields in stream_entries:
                        self.process_result_entry(entry_id, fields)
                        last_id = entry_id
                        
            except redis.RedisError as e:
                if self.running:  # Only log if we're still supposed to be running
                    print(f"Redis error reading results: {e}")
                    time.sleep(1)
            except Exception as e:
                if self.running:
                    print(f"Error reading results: {e}")
                    time.sleep(1)
    
    def update_display(self) -> None:
        """
        Update the pygame display with the current image
        """
        with self.lock:
            # Convert numpy array to pygame surface
            surface = pygame.surfarray.make_surface(self.image_array.swapaxes(0, 1))
            self.screen.blit(surface, (0, 0))
            
            # Add status text
            current_time = time.time()
            if current_time - self.last_update_time > 1.0:  # Update stats every second
                font = pygame.font.Font(None, 36)
                text = font.render(f"Regions: {self.regions_rendered}", True, (255, 255, 255))
                self.screen.blit(text, (10, 10))
                self.last_update_time = current_time
            
            pygame.display.flip()
    
    def run(self) -> None:
        """
        Main rendering loop
        """
        print("Starting Mandelbrot renderer...")
        print("Press ESC or close window to exit")
        
        # Start the Redis reading thread
        redis_thread = threading.Thread(target=self.read_results_from_stream, daemon=True)
        redis_thread.start()
        
        # Main display loop
        clock = pygame.time.Clock()
        
        try:
            while self.running:
                # Handle pygame events
                for event in pygame.event.get():
                    if event.type == pygame.QUIT:
                        self.running = False
                    elif event.type == pygame.KEYDOWN:
                        if event.key == pygame.K_ESCAPE:
                            self.running = False
                
                # Update the display
                self.update_display()
                
                # Control frame rate
                clock.tick(30)  # 30 FPS
                
        except KeyboardInterrupt:
            print("\nRenderer interrupted by user")
        finally:
            self.running = False
            pygame.quit()
            print(f"Renderer stopped. Total regions rendered: {self.regions_rendered}")
    
    def save_image(self, filename: str = "mandelbrot_result.png") -> None:
        """
        Save the current image to a file
        
        Args:
            filename: Output filename
        """
        with self.lock:
            # Convert numpy array to pygame surface and save
            surface = pygame.surfarray.make_surface(self.image_array.swapaxes(0, 1))
            pygame.image.save(surface, filename)
            print(f"Saved Mandelbrot image to {filename}")

def main():
    """
    Main function to run the Mandelbrot renderer
    """
    print("Mandelbrot Set Renderer")
    print("=" * 30)
    
    try:
        # Create and run renderer
        renderer = MandelbrotRenderer(canvas_width=800, canvas_height=600)
        renderer.run()
        
        # Optionally save the final image
        renderer.save_image("mandelbrot_final.png")
        
    except Exception as e:
        print(f"Error running renderer: {e}")

if __name__ == "__main__":
    main()