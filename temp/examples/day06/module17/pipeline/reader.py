from typing import List, Dict, Any


class OrderReader:
    def __init__(self, config: Dict[str, Any]):
        self.input_path = config["paths"]["input"]
        self.batch_size = config["pipeline"]["batch_size"]

    def read(self) -> List[Dict[str, Any]]:
        print(f"[OrderReader] Reading from: {self.input_path}")
        print(f"[OrderReader] Batch size:   {self.batch_size}")
        return [{"order_id": str(i), "amount": i * 10.0} for i in range(1, 6)]
