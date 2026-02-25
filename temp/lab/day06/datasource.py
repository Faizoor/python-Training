from abc import ABC, abstractmethod

class DataSource(ABC):
    @abstractmethod
    def read(self):
        """Return an iterable of records (list of dicts or similar)."""
        raise NotImplementedError()
