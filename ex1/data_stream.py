#!/usr/bin/python3

from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass


    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        return data_batch
    
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {}


class SensorStream(DataStream):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
    
    
    def process_batch(self, data_batch):
        self.readings_num = 0
        self.avg_temp = "N/A"

        for item in data_batch:
            self.readings_num += 1

        try:
            parts = data_batch[0].split(":")
            if parts[0] == "temp":
                self.avg_temp = float(parts[1])
        except IndexError as e:
            print(e)
        except ValueError:
            print("Error: Temperature value is not a valid integer.")

        return f"Processing sensor batch: {data_batch}"


    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        filtered = []
        for item in data_batch:
            parts = item.split(":")
            if criteria is None or parts[0] == criteria:
                filtered += [item]
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = {}
        stats["readings"] = self.readings_num
        stats["avg temp"] = self.avg_temp
        return stats

    def format_stats(self) -> str:
        return f"- Sensor data: {self.readings_num} readings processed"


class TransactionStream(DataStream):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id

    def process_batch(self, data_batch: List[Any]) -> str:
        self.operations = 0
        self.net_flow = 0
        for item in data_batch:
            self.operations += 1

            parts = item.split(":")
            transaction_type = parts[0]
            try:
                amount = int(parts[1])
            except IndexError as e:
                print(e)
                amount = 0
            except ValueError:
                print("Error: amount value is not a valid integer.")
                amount = 0
            if transaction_type == "buy":
                self.net_flow += amount
            elif transaction_type == "sell":
                self.net_flow -= amount

        return f"Processing transaction batch: {data_batch}"

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        filtered = []
        for item in data_batch:
            parts = item.split(":")
            if criteria is None or parts[0] == criteria:
                filtered += [item]
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = {}
        stats["operations"] = self.operations
        stats["net flow"] = self.net_flow
        return stats

    def format_stats(self) -> str:
        return f"- Transaction data: {self.operations} operations processed"


class EventStream(DataStream):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id

    def process_batch(self, data_batch: List[Any]) -> str:
        self.events_count = 0
        self.errors_count = 0
        for event in data_batch:
            self.events_count += 1
            if event == "error":
                self.errors_count += 1
        return f"Processing event batch: {data_batch}"

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        filtered = []
        for item in data_batch:
            if criteria is None or item == criteria:
                filtered += [item]
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = {}
        stats["events"] = self.events_count
        stats["error"] = self.errors_count
        return stats

    def format_stats(self) -> str:
        return f"- Event data: {self.events_count} events processed"


class StreamProcessor:
    def __init__(self):
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream):
        self.streams += [stream]

    def process_all(self, data_batches: Dict[str, List[Any]]):
        for stream in self.streams:
            if stream.stream_id in data_batches:
                stream.process_batch(data_batches[stream.stream_id])
                print(stream.format_stats())


def test():
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    # Sensor Stream
    print("Initializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: Environmental Data")
    sensor_batch = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(sensor.process_batch(sensor_batch))
    print(f"Sensor analysis: {sensor.get_stats()['readings']} readings processed, "
          f"avg temp: {sensor.get_stats()['avg temp']}°C")
    print()

    # Transaction Stream
    print("Initializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    print("Stream ID: TRANS_001, Type: Financial Data")
    transaction_batch = ["buy:100", "sell:150", "buy:75"]
    print(transaction.process_batch(transaction_batch))
    net_flow = transaction.get_stats()['net flow']
    if net_flow >= 0:
        net_flow_str = f"+{net_flow}"
    else:
        net_flow_str = f"{net_flow}"

    print(f"Transaction analysis: {transaction.get_stats()['operations']} operations, "
        f"net flow: {net_flow_str} units")
    print()

    # Event Stream
    print("Initializing Event Stream...")
    event = EventStream("EVENT_001")
    print("Stream ID: EVENT_001, Type: System Events")
    event_batch = ["login", "error", "logout"]
    print(event.process_batch(event_batch))
    print(f"Event analysis: {event.get_stats()['events']} events, "
          f"{event.get_stats()['error']} error detected")
    print()

    # Polymorphic processing
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print()

    processor = StreamProcessor()
    processor.add_stream(sensor)
    processor.add_stream(transaction)
    processor.add_stream(event)

    mixed_batches = {
        "SENSOR_001": ["temp:30", "temp:35"],
        "TRANS_001": ["buy:200", "sell:50"],
        "EVENT_001": ["login", "login", "error"]
    }

    print("Batch 1 Results:")
    processor.process_all(mixed_batches)

    print()
    filtered_sensor = sensor.filter_data(mixed_batches["SENSOR_001"], "temp")
    filtered_transaction = transaction.filter_data(mixed_batches["TRANS_001"], "buy")

    sensor_count = 0
    for _ in filtered_sensor:
        sensor_count += 1

    transaction_count = 0
    for _ in filtered_transaction:
        transaction_count += 1

    print("Stream filtering active: High-priority data only")
    print(f"Filtered results: {sensor_count} critical sensor alerts, {transaction_count} large transaction")


    print()
    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    test()
