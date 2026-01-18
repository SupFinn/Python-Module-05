#!/usr/bin/python3

from abc import ABC, abstractmethod
from typing import Any, List, Union, Protocol

class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        pass

class InputStage:
    def process(self, data: Any) -> Any:
        print("Stage 1: Input validation and parsing")
        if data is None:
            print("Warning: Invalid input data detected...")
            return None
        return data

class TransformStage:
    def process(self, data: Any) -> Any:
        print("Stage 2: Data transformation and enrichment")
        if not data:  # handles None or empty input
            print("Warning: No data to transform")
            return data
        if isinstance(data, dict):
            data["validated"] = True
        return data
    
class OutputStage:
    def process(self, data: Any) -> Any:
        print("Stage 3: Output formatting and delivery")
        if data is None:
            return "No valid data processed"
        return f"Processed output -> {data}"


class ProcessingPipeline(ABC):
    def __init__(self) -> None:
        self.stages: List[ProcessingStage] = []
        self.processed_count = 0
        self.errors = 0

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages += [stage]

    def run(self, data: Any) -> Any:
        for stage in self.stages:
            try:
                data = stage.process(data)
            except Exception as e:
                self.errors += 1
                print(f"Warning: Error in stage: {e}")
                return None
        self.processed_count += 1
        return data

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        return self.run(data)

class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        return self.run(data)

class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        return self.run(data)


class NexusManager:
    def __init__(self) -> None:
        self.pipelines = {}  # store pipelines by name/id

    def register(self, name: str, pipeline: ProcessingPipeline) -> None:
        self.pipelines[name] = pipeline

    def chain(self, data: Any, pipeline_names: list) -> Any:
        """Pass data through multiple pipelines in order"""
        for name in pipeline_names:
            if name not in self.pipelines:
                continue  # skip if pipeline not registered
            pipeline = self.pipelines[name]  # direct access, no .get()
            data = pipeline.process(data)    # feed data into this pipeline
        return data



def main():
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")

    # 1️⃣ Initialize manager
    manager = NexusManager()

    # 2️⃣ Create pipelines
    json_pipeline = JSONAdapter("json_01")
    csv_pipeline = CSVAdapter("csv_01")
    stream_pipeline = StreamAdapter("stream_01")

    # 3️⃣ Add stages to each pipeline
    for pipeline in [json_pipeline, csv_pipeline, stream_pipeline]:
        pipeline.add_stage(InputStage())
        pipeline.add_stage(TransformStage())
        pipeline.add_stage(OutputStage())

    # 4️⃣ Register pipelines in manager
    manager.register("JSON", json_pipeline)
    manager.register("CSV", csv_pipeline)
    manager.register("STREAM", stream_pipeline)

    # 5️⃣ Run some data through each pipeline
    print("\n=== Multi-Format Data Processing ===")
    print(json_pipeline.process({"sensor": "temp", "value": 23}))
    print(csv_pipeline.process("user,action,timestamp"))
    print(stream_pipeline.process(["reading1", "reading2", "reading3"]))

    # 6️⃣ Demonstrate pipeline chaining
    print("\n=== Pipeline Chaining Demo ===")
    chain_result = manager.chain({"sensor": "temp", "value": 23}, ["JSON", "CSV", "STREAM"])
    print(chain_result)

    # 7️⃣ Optional: error demo
    print("\n=== Error Recovery Test ===")
    print(json_pipeline.process(None))  # should safely handle None

if __name__ == "__main__":
    main()
