#!/usr/bin/python3

from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    """Abstract base class for all data processors."""

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the given data and return a formatted string result."""
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Check if the data is valid for this processor type."""
        pass

    def format_output(self, result: str) -> str:
        """Format the raw data for display purposes."""
        return f'"{result}"'


class NumericProcessor(DataProcessor):
    """Processor for numeric data: counts, sums, and averages numbers."""

    def process(self, data: list[int]) -> str:
        count = 0
        num_sum = 0
        for num in data:
            count += 1
            num_sum += num

        avg = num_sum / count if count > 0 else 0
        return f"Processed {count} numeric values, sum={num_sum}, avg={avg}"

    def validate(self, data: list[int]) -> bool:
        """Validate that all elements in the data are numbers."""
        try:
            for num in data:
                int(num)
            return True
        except ValueError:
            return False


class TextProcessor(DataProcessor):
    """Processor for text data: counts characters and words."""

    def process(self, data: str) -> str:
        words = data.split()

        w_count = 0
        for _ in words:
            w_count += 1

        l_count = 0
        for _ in data:
            l_count += 1

        return f"Processed text: {l_count} characters, {w_count} words"

    def validate(self, data: str) -> bool:
        """Validate that the data is a text string (not purely numeric)."""
        try:
            int(data)
            return False
        except ValueError:
            return True


class LogProcessor(DataProcessor):
    """Processor for log messages: detects levels and formats output."""

    def process(self, data: Any) -> str:
        parts = data.split()

        if not parts:
            return "[INFO] Empty log entry"

        level = ""
        if parts[0] == "ERROR:":
            level = "ERROR"
            label = "[ALERT]"
        elif parts[0] == "INFO:":
            level = "INFO"
            label = "[INFO]"
        else:
            label = "[INFO]"
            level = "INFO"

        message = ""
        i = 1
        while i < len(parts):
            if message != "":
                message += " "
            message += parts[i]
            i += 1

        return f"{label} {level} level detected: {message}"

    def validate(self, data: Any) -> bool:
        """All log messages are considered valid."""
        return True


def main():
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    numbers = NumericProcessor()
    data = [1, 2, 3, 4, 5]
    print(f"Processing data: {data}")
    if numbers.validate(data):
        print("Validation: Numeric data verified")
    else:
        print("Validation: Numeric data Validation failed")
    print(f"Output: {numbers.process(data)}")

    print("\nInitializing Text Processor...")
    text = TextProcessor()
    data = "Hello Nexus World"
    print(f'Processing data: {text.format_output(data)}')
    if text.validate(data):
        print("Validation: Text data verified")
    else:
        print("Validation: Text data Validation failed")
    print(f"Output: {text.process(data)}")

    print("\nInitializing Log Processor...")
    log = LogProcessor()
    data = "ERROR: Connection timeout"
    print(f"Processing data: {log.format_output(data)}")
    if log.validate(data):
        print("Validation: Log entry verified")
    print(f"Output: {log.process(data)}")

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    nums = NumericProcessor()
    texts = TextProcessor()
    logs = LogProcessor()
    print(f"Result 1: {nums.process([1, 2, 3])}")
    print(f"Result 2: {texts.process('Hello Worlld')}")
    print(f"Result 3: {logs.process('INFO: System ready')}")

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
