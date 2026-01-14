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
                self.avg_temp = int(parts[1])
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


class StreamProcessor:
    def __init__(self):
        streams = [
            SensorStream("SENSOR_001"),
            TransactionStream("TRANS_001"),
            EventStream("EVENT_001")
        ]
    
    def run_batches(self, batches: List[List[Any]]):
        for batch in batches:
            batch.process_batch(batch)
            batch.get_stats()