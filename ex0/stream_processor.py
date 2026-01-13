from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod

class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass


    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass


    def format_output(self, result: str) -> str:
        return result

    
class NumericProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        count = 0
        num_sum = 0
        for num in data:
            count += 1
            num_sum += num
        
        if count == 0:
            avg = 0
        else:
            avg = num_sum / count

        return f"Output: Processed {count} numeric values, sum={num_sum}, avg={avg}"

    def validate(self, data: Any) -> bool:
        try:
            for num in data:
                int(num)
            return True
        except ValueError:
            return False


class TextProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        w_count = 0
        l_count = 0
        in_word = False

        for letter in data:
            l_count += 1
            if letter != ' ' and not in_word:
                w_count += 1
                in_word = True
            elif letter == ' ':
                in_word = False

        return f"Output: Processed text: {l_count} characters, {w_count} words"

    def validate(self, data: Any) -> bool:
        try:
            int(data)
            return False
        except ValueError:
            return True


class LogProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        if "ERROR" in data:
            label = "[ALERT]"
            e_type = "ERROR"
        elif "INFO" in data:
            label = "[INFO]"
            e_type = "INFO"
        else:
            label = "[INFO]"
            e_type = "INFO"
        
        return f"Output: {label} {e_type} level detected {data}"


    def validate(self, data: Any) -> bool:
        return True
