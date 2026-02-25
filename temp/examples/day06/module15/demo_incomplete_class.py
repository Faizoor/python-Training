from datasource.base import DataSource


class BrokenReader(DataSource):
    def connect(self):
        pass

    def read(self):
        return []

    # close() is intentionally missing to demonstrate ABC enforcement


if __name__ == "__main__":
    try:
        reader = BrokenReader()
    except TypeError as e:
        print(f"Caught expected error: {e}")
