#!/usr/bin/env python3
"""
Temperature Sensor Simulator
Uses Redis TimeSeries to simulate CPU temperature readings over time
"""

import redis
import time
import random
import math
import argparse
from typing import Optional
from datetime import datetime
import matplotlib.pyplot as plt

HOUR_MSECS = 3600000

# Set matplotlib to non-blocking mode
plt.ion()

class TemperatureSensor:
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        """
        Initialize the temperature sensor simulator

        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
        """
        # Connect to Redis
        try:
            self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            self.redis_client.ping()  # Test connection
            print(f"Temperature sensor connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError:
            print(f"Failed to connect to Redis at {redis_host}:{redis_port}")
            raise

        # TimeSeries key for CPU temperature
        self.ts_key = "cpu:temperature"
        self.ts_compaction_key = "cpu:temperature:compaction"

        # CPU temperature simulation parameters
        self.base_temp = 45.0  # Base CPU temperature in Celsius
        self.idle_temp = 35.0  # Idle temperature
        self.max_temp = 85.0   # Maximum safe temperature
        self.current_temp = self.base_temp

        # Simulation state
        self.load_factor = 0.3  # Current CPU load factor (0.0 to 1.0)
        self.ambient_temp = 22.0  # Room temperature

    def create_timeseries(self) -> None:
        """
        Create a Redis TimeSeries for CPU temperature data
        """
        try:
            self.redis_client.ts().delete(self.ts_key, '-', '+')
        except redis.ResponseError as e:
            if "the key does not exist" in str(e).lower():
                print(f"TimeSeries {self.ts_key} already exists")
            else:
                print(f"Error deleting TimeSeries: {e}")
                raise
        try:
            # Create the timeseries with retention policy and labels
            self.redis_client.ts().create(
                self.ts_key,
                labels={'sensor': 'cpu', 'unit': 'celsius', 'location': 'server'}
            )
            print(f"Created TimeSeries: {self.ts_key}")

        except redis.ResponseError as e:
            if "key already exists" in str(e).lower():
                print(f"TimeSeries {self.ts_key} already exists")
            else:
                print(f"Error creating TimeSeries: {e}")
                raise
        try:
            self.redis_client.ts().delete(self.ts_compaction_key, '-', '+')
        except redis.ResponseError as e:
            if "the key does not exist" in str(e).lower():
                print(f"TimeSeries {self.ts_compaction_key} already exists")
            else:
                print(f"Error deleting TimeSeries: {e}")
                raise
        try:
            self.redis_client.ts().create(self.ts_compaction_key)
            print(f"Created TimeSeries: {self.ts_compaction_key}")
        except redis.ResponseError as e:
            if "key already exists" in str(e).lower():
                print(f"TimeSeries {self.ts_compaction_key} already exists")
            else:
                print(f"Error creating TimeSeries: {e}")
                raise
        try:
            self.redis_client.ts().deleterule(self.ts_key, self.ts_compaction_key)
        except redis.ResponseError as e:
            if "compaction rule does not exist" in str(e).lower():
                print(f"TimeSeries {self.ts_compaction_key} already exists")
            else:
                print(f"Error deleting TimeSeries: {e}")
                raise
        try:
            self.redis_client.ts().createrule(self.ts_key, self.ts_compaction_key,
                aggregation_type='avg', bucket_size_msec=HOUR_MSECS)
            print(f"Created compaction rule for TimeSeries: {self.ts_key}")
        except redis.ResponseError as e:
            if "the destination key already has a src rule" in str(e).lower():
                print(f"Compaction rule for TimeSeries {self.ts_key} already exists")
            else:
                print(f"Error creating compaction rule: {e}")
                raise

    def simulate_cpu_load_change(self, timestamp: Optional[int] = None) -> None:
        """
        Simulate changes in CPU load over time

        Args:
            timestamp: Unix timestamp in milliseconds (current time if None)
        """
        # Random load fluctuations with some persistence
        load_change = random.gauss(0, 0.05)  # Small random changes

        # Add some periodic load patterns (simulating scheduled tasks, etc.)
        if timestamp is None:
            time_factor = time.time() / 3600  # Hours since epoch
        else:
            time_factor = (timestamp / 1000) / 3600  # Convert ms to seconds, then to hours

        periodic_load = 0.2 * math.sin(time_factor * 2 * math.pi / 24)  # Daily cycle
        periodic_load += 0.1 * math.sin(time_factor * 2 * math.pi / 2)   # 2-hour cycle

        # Update load factor
        self.load_factor += load_change + periodic_load * 0.01
        self.load_factor = max(0.0, min(1.0, self.load_factor))  # Clamp to [0, 1]

    def calculate_temperature(self) -> float:
        """
        Calculate realistic CPU temperature based on load and other factors

        Returns:
            Simulated CPU temperature in Celsius
        """
        # Base temperature calculation
        load_temp = self.idle_temp + (self.max_temp - self.idle_temp) * self.load_factor

        # Add thermal inertia (temperature changes gradually)
        temp_diff = load_temp - self.current_temp
        thermal_response = 0.1  # How quickly temperature responds to load changes
        self.current_temp += temp_diff * thermal_response

        # Add ambient temperature influence
        ambient_influence = (self.ambient_temp - 20.0) * 0.3
        self.current_temp += ambient_influence * 0.01

        # Add some realistic noise
        noise = random.gauss(0, 0.5)  # Small temperature fluctuations
        measured_temp = self.current_temp + noise

        # Add occasional temperature spikes (thermal throttling, background processes)
        if random.random() < 0.001:  # 0.1% chance of spike
            spike = random.uniform(5, 15)
            measured_temp += spike
            print(f"Temperature spike detected: +{spike:.1f}°C")

        # Ensure temperature stays within realistic bounds
        measured_temp = max(self.ambient_temp, min(self.max_temp + 5, measured_temp))

        return round(measured_temp, 2)

    def add_temperature_sample(self, temperature: float, timestamp: Optional[int] = None) -> None:
        """
        Add a temperature sample to the Redis TimeSeries

        Args:
            temperature: Temperature value in Celsius
            timestamp: Unix timestamp in milliseconds (current time if None)
        """
        if timestamp is None:
            timestamp = int(time.time() * 1000)  # Current time in milliseconds

        try:
            TIME24HOURS_MSECS = 86400000
            self.redis_client.ts().add(self.ts_key, timestamp, temperature, retention_msecs=TIME24HOURS_MSECS)
        except redis.ResponseError as e:
            print(f"Error adding sample to TimeSeries: {e}")

    def get_timeseries_info(self) -> dict:
        """
        Get information about the TimeSeries

        Returns:
            Dictionary containing TimeSeries metadata
        """
        try:
            return self.redis_client.ts().info(self.ts_key)
        except redis.ResponseError as e:
            print(f"Error getting TimeSeries info: {e}")
            return {}

    def simulate_historical_data(self, num_samples: int = 10000, interval_seconds: int = 1) -> None:
        """
        Generate and add historical temperature data to simulate past readings

        Args:
            num_samples: Number of temperature samples to generate
            interval_seconds: Time interval between samples in seconds
        """
        print(f"Generating {num_samples} historical temperature samples...")
        print(f"Interval: {interval_seconds} second(s) between samples")

        # Start from current time and work backwards
        current_time = int(time.time() * 1000)  # Current time in milliseconds
        start_time = current_time - (num_samples * interval_seconds * 1000)

        samples_added = 0

        for i in range(num_samples):
            # Calculate timestamp for this sample
            sample_time = start_time + (i * interval_seconds * 1000)

            # Simulate load changes over time
            self.simulate_cpu_load_change(sample_time)

            # Calculate temperature for this point in time
            temperature = self.calculate_temperature()

            # Add sample to TimeSeries
            self.add_temperature_sample(temperature, sample_time)
            samples_added += 1

            # Progress reporting
            if samples_added % 1000 == 0:
                progress = (samples_added / num_samples) * 100
                print(f"Progress: {samples_added}/{num_samples} samples ({progress:.1f}%) - "
                      f"Current temp: {temperature:.2f}°C, Load: {self.load_factor:.2f}")

        print(f"✅ Successfully added {samples_added} temperature samples to TimeSeries")

    def run_real_time_simulation(self, duration_seconds: int = 60) -> None:
        """
        Run real-time temperature simulation

        Args:
            duration_seconds: How long to run the simulation
        """
        print(f"Starting real-time temperature simulation for {duration_seconds} seconds...")

        start_time = time.time()
        samples_added = 0

        try:
            while time.time() - start_time < duration_seconds:
                timestamp = int(time.time() * 1000)  # Current time in milliseconds
                # Simulate load changes
                self.simulate_cpu_load_change(timestamp)

                # Calculate current temperature
                temperature = self.calculate_temperature()

                # Add sample to TimeSeries
                self.add_temperature_sample(temperature, timestamp)
                samples_added += 1

                # Display current reading
                print(f"🌡️  CPU Temp: {temperature:5.2f}°C | Load: {self.load_factor:4.2f} | "
                      f"Samples: {samples_added}")

                # Wait for next sample
                time.sleep(1)

        except KeyboardInterrupt:
            print(f"\n⏹️  Simulation stopped by user after {samples_added} samples")

        print(f"✅ Real-time simulation completed. Added {samples_added} samples.")

    def display_five_minute_stats(self, info: dict) -> None:
        try:
            # Get last 5 minutes to calculate statistics
            FIVE_MINUTES_MSECS = 300000
            recent_data = self.redis_client.ts().range(
                self.ts_key, info.get('last_timestamp')-FIVE_MINUTES_MSECS, info.get('last_timestamp')
            )

            if recent_data:
                temperatures = [float(sample[1]) for sample in recent_data]
                min_temp = min(temperatures)
                max_temp = max(temperatures)
                avg_temp = sum(temperatures) / len(temperatures)

                print(f"\nLast five minutes Temperature Range:")
                print(f"Min: {min_temp:.2f}°C")
                print(f"Max: {max_temp:.2f}°C")
                print(f"Avg: {avg_temp:.2f}°C")


                # Display the data on a graph
                x = [sample[0] for sample in recent_data]
                y = [float(sample[1]) for sample in recent_data]
                plt.figure(figsize=(10, 6))
                plt.plot(x, y, 'b-', linewidth=1.5)
                plt.xlabel('Timestamp (milliseconds)')
                plt.ylabel('Temperature (°C)')
                plt.title('Five Minute Temperature Data')

                # Pick ten evenly spaced ticks from the x data
                x_ticks = x[::len(x)//10]
                x_labels = [datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S') for ts in x_ticks]
                plt.xticks(x_ticks, x_labels, rotation=45)

                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                plt.show(block=False)
                plt.draw()
                print("📊 Five-minute temperature plot opened in new window")



        except Exception as e:
            print(f"Could not calculate temperature statistics: {e}")

    def display_hourly_averages(self) -> None:
        hourly_averages = self.redis_client.ts().range(
            self.ts_key, '-', '+',
            aggregation_type='avg', bucket_size_msec=HOUR_MSECS
        )

        if hourly_averages:
            print("\nHourly Averages:")
            for avg in hourly_averages:
                timestamp = avg[0]
                temperature = avg[1]
                timestamp_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                print(f"{timestamp_str}: {temperature:.2f}°C")

            # Display the data on a graph
            if len(hourly_averages) > 0:
                timestamps = [sample[0] for sample in hourly_averages]
                temperatures = [float(sample[1]) for sample in hourly_averages]

                # Convert timestamps to datetime objects for better x-axis labels
                datetime_labels = [datetime.fromtimestamp(ts / 1000) for ts in timestamps]

                plt.figure(figsize=(12, 6))
                plt.bar(range(len(temperatures)), temperatures, width=0.8, color='orange', alpha=0.7)
                plt.xlabel('Time')
                plt.ylabel('Temperature (°C)')
                plt.title('Hourly Average Temperatures')
                plt.grid(True, alpha=0.3)

                # Set x-axis labels to show datetime
                plt.xticks(range(len(datetime_labels)),
                          [dt.strftime('%m-%d %H:%M') for dt in datetime_labels],
                          rotation=45)
                plt.tight_layout()
                plt.show(block=False)
                plt.draw()
                print("📊 Hourly averages bar chart opened in new window")
            else:
                print("No hourly average data available for plotting.")

    def display_spikes(self) -> None:
        spikes = self.redis_client.ts().range(
            self.ts_key, '-', '+',
            filter_by_min_value=85.0,
            filter_by_max_value=100.0,
        )
        if spikes:
            print("\nSpikes (>=85°C):")
            prev_timestamp = None
            max_value = 0.0
            max_value_timestamp = None
            for spike in spikes:
                timestamp = spike[0]
                temperature = spike[1]
                # Consider only spikes that are at least 1 minuteapart
                if not prev_timestamp or timestamp - prev_timestamp < 60000:
                    prev_timestamp = timestamp
                    max_value = max(max_value, temperature)
                    max_value_timestamp = timestamp
                    continue
                if max_value > 0.0:
                    timestamp_str = datetime.fromtimestamp(max_value_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"{timestamp_str}: {max_value:.2f}°C")
                    max_value = 0.0
                    max_value_timestamp = None
                prev_timestamp = timestamp
            if max_value > 0.0:
                timestamp_str = datetime.fromtimestamp(max_value_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                print(f"{timestamp_str}: {max_value:.2f}°C")

            # Display the data on a graph
            if len(spikes) > 0:
                timestamps = [sample[0] for sample in spikes]
                temperatures = [float(sample[1]) for sample in spikes]

                plt.figure(figsize=(12, 6))
                plt.scatter(timestamps, temperatures)
                plt.xlabel('Time')
                plt.ylabel('Temperature (°C)')
                plt.title('Spikes')
                # Pick ten evenly spaced ticks from the x data
                x_ticks = timestamps[::len(timestamps)//10]
                x_labels = [datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S') for ts in x_ticks]
                plt.xticks(x_ticks, x_labels, rotation=45)
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                plt.show(block=False)
                plt.draw()
                print("📊 Spikes scatter plot opened in new window")

    def display_compaction(self) -> None:
        try:
            compaction_data = self.redis_client.ts().range(
                self.ts_compaction_key, '-', '+'
            )
            if compaction_data:
                print("\nHourly Averages (Compaction Data):")
                for avg in compaction_data:
                    timestamp = avg[0]
                    temperature = avg[1]
                    timestamp_str = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"{timestamp_str}: {temperature:.2f}°C")
        except Exception as e:
            print(f"Could not calculate temperature statistics: {e}")

    def display_statistics(self) -> None:
        """
        Display statistics about the stored temperature data
        """
        try:
            # Get TimeSeries info
            info = self.get_timeseries_info()

            print("\n📊 Temperature TimeSeries Statistics:")
            print(f"   Key: {self.ts_key}")
            print(f"   Total Samples: {info.get('total_samples')}")
            print(f"   Memory Usage: {info.get('memory_usage')} bytes")
            print(f"   First Timestamp: {info.get('first_timestamp')}")
            print(f"   Last Timestamp: {info.get('last_timestamp')}")

            self.display_five_minute_stats(info)
            self.display_hourly_averages()
            self.display_spikes()

            campaction_info = self.redis_client.ts().info(self.ts_compaction_key)
            print("\n📊 Temperature TimeSeries Compaction Statistics:")
            print(f"   Key: {self.ts_compaction_key}")
            print(f"   Total Samples: {campaction_info.get('total_samples')}")
            print(f"   Memory Usage: {campaction_info.get('memory_usage')} bytes")
            print(f"   First Timestamp: {campaction_info.get('first_timestamp')}")
            print(f"   Last Timestamp: {campaction_info.get('last_timestamp')}")
            self.display_compaction()
        except Exception as e:
            print(f"Error displaying statistics: {e}")

def create_argument_parser() -> argparse.ArgumentParser:
    """
    Create command line argument parser

    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        description='CPU Temperature Sensor Simulator using Redis TimeSeries',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python temperature_sensor.py --historical 10000    # Generate 10k historical samples
  python temperature_sensor.py --realtime 300        # Run real-time simulation for 5 minutes
  python temperature_sensor.py --stats               # Show TimeSeries statistics
        """
    )

    parser.add_argument(
        '--redis-host',
        default='localhost',
        help='Redis server hostname (default: localhost)'
    )

    parser.add_argument(
        '--redis-port',
        type=int,
        default=6379,
        help='Redis server port (default: 6379)'
    )

    parser.add_argument(
        '--historical',
        type=int,
        metavar='SAMPLES',
        help='Generate historical temperature data with specified number of samples'
    )

    parser.add_argument(
        '--interval',
        type=int,
        default=1,
        help='Interval between samples in seconds (default: 1)'
    )

    parser.add_argument(
        '--realtime',
        type=int,
        metavar='SECONDS',
        help='Run real-time temperature simulation for specified duration'
    )

    parser.add_argument(
        '--stats',
        action='store_true',
        help='Display TimeSeries statistics'
    )

    return parser

def main():
    """
    Main function to run the temperature sensor simulator
    """
    parser = create_argument_parser()
    args = parser.parse_args()

    print("🌡️  CPU Temperature Sensor Simulator")
    print("=" * 50)

    try:
        # Create temperature sensor instance
        sensor = TemperatureSensor(redis_host=args.redis_host, redis_port=args.redis_port)

        if args.stats:
            sensor.display_statistics()
            # Keep the script running until user decides to exit
            try:
                input("Press Enter to exit...")
            except KeyboardInterrupt:
                pass
            return

        # Create TimeSeries
        sensor.create_timeseries()

        # Execute requested operations
        if args.historical:
            sensor.simulate_historical_data(args.historical, args.interval)

        if args.realtime:
            sensor.run_real_time_simulation(args.realtime)


        # If no specific operation requested, generate default historical data
        if not any([args.historical, args.realtime, args.stats]):
            print("No operation specified. Generating 25,000 historical samples...")
            sensor.simulate_historical_data(25000, 1)
            sensor.display_statistics()
            # Keep the script running until user decides to exit
            try:
                input("Press Enter to exit...")
            except KeyboardInterrupt:
                pass

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    print("\n✅ Temperature sensor simulation completed!")
    return 0

if __name__ == "__main__":
    exit(main())