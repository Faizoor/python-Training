from typing import Any


class BaseConnector:
    """Base contract for connectors used by the pipeline."""
    name = "base"

    def __init__(self, **cfg):
        self.cfg = cfg

    def connect(self) -> None:
        raise NotImplementedError

    def fetch(self) -> Any:
        raise NotImplementedError

    def close(self) -> None:
        pass


def make_connector_class(name: str, fetch_impl):
    """Create a connector class at runtime.

    - `name`: class name
    - `fetch_impl`: function implementing fetch(self)
    """

    attrs = {
        "name": name,
        "connect": lambda self: print(f"[{name}] connected with {self.cfg}"),
        "fetch": fetch_impl,
        "close": lambda self: print(f"[{name}] closed"),
    }

    return type(f"{name}Connector", (BaseConnector,), attrs)


def mysql_fetch(self):
    return [{"id": 1, "user": "alice"}]


def api_fetch(self):
    return [{"id": "a1", "value": "sample"}]


MySQLConnector = make_connector_class("MySQL", mysql_fetch)
APIConnector = make_connector_class("API", api_fetch)


if __name__ == "__main__":
    connectors = [MySQLConnector(host="db.prod"), APIConnector(base_url="https://api")]
    for c in connectors:
        c.connect()
        print("Fetched:", c.fetch())
        c.close()
