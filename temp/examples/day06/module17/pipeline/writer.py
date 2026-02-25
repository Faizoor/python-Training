from typing import List, Dict, Any


class OrderWriter:
    def __init__(self, config: Dict[str, Any]):
        self.output_path = config["paths"]["output"]

    def write(self, records: List[Dict[str, Any]]) -> None:
        print(f"[OrderWriter] Writing {len(records)} records to: {self.output_path}")
        for record in records:
            print(f"  Written: {record}")
