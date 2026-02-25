from abc import ABC, abstractmethod
from typing import List, Dict, Any


class DataSource(ABC):
    """
    Abstract contract for all pipeline data sources.
    """

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def read(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
