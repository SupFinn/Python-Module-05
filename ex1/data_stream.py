from abc import ABC, abstractmethod
from typing import List, Any, Dict, Union, Optional


# Base abstract class which the parent for the
# specialized streams below.
class DataStream(ABC):
    """Abstract class for data streaming"""

    def __init__(self) -> None:
        """A Constructor for storing a default batch
        data so can be used in the normal methods below
        if not overrided"""
        # counter to use in the default get_status method
        # if not overrided
        self.element_count_processed: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """An Abstract method to process a batch of data"""
        pass

    # Default method that can overrided
    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None
                    ) -> List[Any]:
        """A default data filtering that will be overriding"""
        return data_batch

    # Default method that can overrided
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return a default statistics"""

        return {"Data Proceded: ": self.element_count_processed}


# A Boss class that show the demonstration of polymorphism
# with processing a minimal status about all specialized
# classes processing.
class StreamProcessor:
    """Structuring a minimal result
    about sharing the same interface
    between the specialized stream systems."""

    def __init__(self, stream_systems: List[Any]) -> None:
        """Initialize StreamProcessor with a list of stream systems."""

        self.stream_systems: List[Any] = stream_systems

    def process_all(self, systems_data: List[List]) -> str:
        """Process data for all systems"""

        systems_count: int = get_len(systems_data)
        stream_systems: List = self.stream_systems
        i: int = 0
        # Loop trough stream systems and pass the data provided
        # to the system by its process_batch method, even if its
        # not printed we got the element_count_processed in the system object.
        try:
            while i < systems_count:
                stream_systems[i].process_batch(systems_data[i])
                i += 1
        except Exception:
            print("Error: Data and systems provided doesn't match")
            return

        i: int = 0
        # get the type of data based on its the class of system.
        # for structure the formate demo.
        formated_result: str = ""
        while i < systems_count:
            if isinstance(stream_systems[i], SensorStream):
                system_type: str = "SensorStream"
                element_count: int = stream_systems[i].element_count_processed
                formated_result += (f"- {system_type}: "
                                    f"{element_count} readings processed\n")
            elif isinstance(stream_systems[i], TransactionStream):
                system_type: str = "TransactionStream"
                element_count: int = stream_systems[i].element_count_processed
                formated_result += (
                    f"- {system_type}: {element_count} operations processed\n"
                )
            elif isinstance(stream_systems[i], EventStream):
                system_type: str = "EventStream"
                element_count: int = stream_systems[i].element_count_processed
                formated_result += (
                        f"- {system_type}: {element_count} events processed\n"
                )
            i += 1
        return formated_result

    # A self dependence method that filter the data of the stream systems
    # based on their filter method.
    def filter_data(self,
                    all_systems_data: List[List],
                    criteria: Optional[str] = None
                    ) -> str:
        """Method that filter and structure the result"""
        i: int = 0
        all_filtered_result: List[List] = []
        stream_systems_count: int = get_len(self.stream_systems)
        stream_systems: List = self.stream_systems
        while i < stream_systems_count:
            # For each process system pass its specific data to its filter
            # which will filter the most important piece of its data
            # and return it as a list, store the result in a big list.
            result: List = stream_systems[i].filter_data(all_systems_data[i])
            all_filtered_result.append(result)
            i += 1

        # Extract result for each system.
        try:
            sensor_alerts: int = get_len(all_filtered_result[0])
            large_transaction: int = get_len(all_filtered_result[1])
            events_errors: int = get_len(all_filtered_result[2])
        except Exception:
            print("Error: You forget a stream system, check again.")

        formated_result: str = "Filtered results: "

        if sensor_alerts > 0:
            formated_result += f"{sensor_alerts} critical sensor alerts, "
        if large_transaction > 0:
            formated_result += f"{large_transaction} large_transaction, "
        if events_errors > 0:
            formated_result += f"{events_errors} failed events, "

        return formated_result[:-2]  # Remove last ugly ", "


# ###################Sensor stream ################################
class SensorStream(DataStream):
    """Specialized Sensor detector that overriding the
    base structure and handle a specific kind of data"""

    def __init__(self, stream_id: str) -> None:
        """Sensor constructor that notify
        initialization and print id"""

        # Use the base method and update
        super().__init__()
        self.stream_id: str = stream_id
        print("Initializing Sensor Stream...")
        print(f"Stream ID: {self.stream_id}, Type: Environmental Data")

    # the overrided abstractmethod
    # that process and format the data provided.
    def process_batch(self, data_batch: List[Any]) -> str:
        """Processing a data"""

        # Count the total elements that processed
        # and override the default 0 count.
        self.element_count_processed: int = data_batch.__len__()

        try:
            # Format manually instead of join since not authorized
            formatted: str = "["
            for item in data_batch:
                for k in item:
                    formatted += f"{k}:{item[k]}, "
            formatted: str = formatted[:-2] + "]"  # Remove last part ", "
        except Exception:
            print("Error: check the data entered, offten must be dictionary.")
            # just a safety if list empty fix the formatted string.
            if get_len(data_batch) == 0:
                formatted = "[]"

        # Extracting the temps since the most important in
        # a sensor logic, and store them so can calculate
        # the average later in get_status.
        self.temps_list: List = []
        for dic in data_batch:
            for key in dic:
                if key == "temp":
                    # Assign the value into the self to be used later.
                    self.temps_list.append(dic[key])

        return f"Processing sensor batch: {formatted}"

    # Filtered method specialized an
    # each type even if not abstract
    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None
                    ) -> List[Any]:
        """Filtering the alert sensors and store
        them to be used later if needed."""
        sensor_alerts_values: List = []
        # Store values that dangerous as sensor alerts
        # in a list and return it to match the prototype.
        for dic in data_batch:
            for key in dic:
                if dic[key] < 0 or dic[key] >= 50:
                    sensor_alerts_values.append(dic[key])

        return sensor_alerts_values

    # Overrided the default method to specify the formatting.
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistic,
        Overrided version of get status"""
        # Get the element processed
        processed_count: int = self.element_count_processed
        total_temps_count: int = self.temps_list.__len__()
        average: float = 0
        for temp in self.temps_list:
            average += temp
        try:
            # get average of the temps if multiple.
            average: float = average / total_temps_count
        except ZeroDivisionError:
            print("Error: some temps have 0 value")

        return {
            "Sensor analysis: ": f"{processed_count} readings processed, ",
            "avg temp: ": f"{average}°C"
            }


# ####################Transaction steam #############################
class TransactionStream(DataStream):
    """Specialized Transaction steam that interpret
    the Transaction data provided"""

    def __init__(self, stream_id: str) -> None:
        """Transaction constructor that
        initialize the stream is and print
        some info"""

        # Use the base method and update
        super().__init__()
        self.stream_id: str = stream_id
        print("Initializing Transaction Stream...")
        print(f"Stream ID: {self.stream_id}, Type: Financial Data")

    # the overrided abstractmethod
    # that process and format the data provided.
    def process_batch(self, data_batch: List[Any]) -> str:
        """Processing a data"""

        # Count the total elements that processed
        # and override the default 0 count.
        self.element_count_processed: int = data_batch.__len__()
        # For safety if data not valid type
        try:
            # Manually format the dictionary to show as a list
            # format, instead of using join since not authorized
            formatted: str = "["
            for item in data_batch:
                for k in item:
                    formatted += f"{k}:{item[k]}, "
            formatted: str = formatted[:-2] + "]"  # Remove last part ", "
        except Exception:
            print("Error: check the data entered, offten must be dictionary.")
            return

        # Extracting the net flow which is the result
        # from multi buying and selling operations
        # subtracted.
        # and store them so can print 'net flow'
        # later in get_status.
        all_buying: int = 0
        all_selling: int = 0
        for dic in data_batch:
            for key in dic:
                if key == "buy":
                    all_buying += dic[key]
                elif key == "sell":
                    all_selling += dic[key]
        # assign the net flow to self so
        # can be used in get_status
        self.units: int = all_buying - all_selling

        return f"Processing transaction batch: {formatted}"

    # Filtered method specialized an
    # each type even if not abstrcat
    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None
                    ) -> List[Any]:
        """Filtering the large transaction, lets say
        over than 500,"""

        # Store the count in a list as elements of "1"
        # so i can later get the count just wiht .__len__()
        # and don't break the method prototype contract.
        large_transactions_count: List = []
        for dic in data_batch:
            for key in dic:
                if dic[key] > 500:
                    large_transactions_count.append(1)

        # return a list just to match the prototype contract
        # but in the formating filer ill just use len(list)
        # since it just count the large transactions
        return [large_transactions_count]

    # Override the default method to specify the formatting.
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistic,
        Overrided version of get status"""

        # Get the element processed
        data_count: int = self.element_count_processed
        if self.units > 0:
            net_flow_string: str = f"+{self.units} units"
        elif self.units < 0:
            net_flow_string: str = f"-{self.units} units"
        else:
            # if the units is 0
            net_flow_string: str = f"{self.units} units"

        return {
            "Transaction analysis: ": f"{data_count} operations, ",
            "net flow: ": net_flow_string
            }


# ################# Even stream ###############################
class EventStream(DataStream):
    """Interpret the list of events and handle structure
    them in a good format"""

    def __init__(self, stream_id: str) -> None:
        """Event constructor that notify
        initialization the stream and print
        some info"""

        # Use the base method and update
        super().__init__()
        self.stream_id: str = stream_id
        print("Initializing Event Stream...")
        print(f"Stream ID: {self.stream_id}, Type: System Events")

    # the overrided abstractmethod
    # that process and format the data provided.
    def process_batch(self, data_batch: List[Any]) -> str:
        """Processing a data"""

        # Count the total elements that processed
        # and override the default 0 count.
        self.element_count_processed: int = data_batch.__len__()
        self.errors_count: int = 0

        try:
            # Manually format the dictionary to show as a list
            # format, instead of using join since not authorized
            # and also count errors detected for get status.
            formatted: str = "["
            for item in data_batch:
                formatted += f"{item}, "
                if item == "error":
                    self.errors_count += 1
            formatted: str = formatted[:-2] + "]"  # Remove last part ", "
        except Exception:
            print("Error: check the data entered, offten must be dictionary.")

        return f"Processing event batch: {formatted}"

    # Filtered method specialized an
    # each type even if not abstrcat
    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None
                    ) -> List[Any]:
        """Filtering the error evenets, and
        store them in the list"""

        errors_events: List = []
        for event in data_batch:
            if event == "error":
                errors_events.append(event)

        return errors_events

    # Override the default method to specify the formatting.
    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistic,
        Overrided version of get status"""

        # Get the element processed
        data_count: int = self.element_count_processed
        errors_count: int = self.errors_count

        return {
            "Event analysis: ": f"{data_count} events, ",
            f"{errors_count} error detected": ""
            }


# Helper to count length
def get_len(item: Any) -> int:
    """Calculate the length of an object manually using dunder len."""
    return item.__len__()


# Main start
def main() -> None:
    """The main entry point to demonstrate data stream"""

    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print("")

    # ########## Demonstrate the Sensor Stream #####################
    sensors_stream: SensorStream = SensorStream("SENSOR_001")
    sensors_data: List[Any] = [
        {"temp": 22.5},
        {"humidity": 65},
        {"pressure": 1013}
    ]
    print(sensors_stream.process_batch(sensors_data))
    # Formate the output dictionary from
    # get_status and print is as the demo string.
    status: Dict = sensors_stream.get_stats()
    for key in status:
        print(f"{key}{status[key]}", end="")
    print("")

    print("")
    # ########## Demonstrate the transaction stream ################
    transactions_stream: TransactionStream = TransactionStream("TRANS_001")
    transactions_data: List[Any] = [
        {"buy": 100},
        {"sell": 150},
        {"buy": 75},
    ]
    print(transactions_stream.process_batch(transactions_data))
    status: Dict = transactions_stream.get_stats()
    for key in status:
        print(f"{key}{status[key]}", end="")
    print("")

    print("")
    # ########### Demonstrate Events stream ####################
    events_stream: EventStream = EventStream("EVENT_001")
    # Just a normal list as showed in demo.
    events_data: List[Any] = ["login", "error", "logout"]
    print(events_stream.process_batch(events_data))
    status: Dict = events_stream.get_stats()
    for key in status:
        print(f"{key}{status[key]}", end="")
    print("")

    print("")
    # ######## Demonstrate polymorphic ##########
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print("")
    print("Batch 1 Results:")
    # List object(instance) of all systems
    all_systems: List = [sensors_stream, transactions_stream, events_stream]
    mother_processor: StreamProcessor = StreamProcessor(all_systems)
    all_systems_data: List[List] = [
            [{"temp": -20}, {"temp": 50}],  # Sensor data.
            [{"buy": 50}, {"sell": 60}, {"buy": 100}, {"sell": 400}],  # trans
            ["login", "sudo", "logout"]  # Events data
    ]
    # Process and print the data processed and its type.
    print(mother_processor.process_all(all_systems_data))

    print("Stream filtering active: High-priority data only")
    print(mother_processor.filter_data(all_systems_data))
    print("")
    print("All streams processed successfully. Nexus throughput optimal.")


main()
