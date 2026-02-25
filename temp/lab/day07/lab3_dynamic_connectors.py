class BaseConnector:
    def __init__(self, config=None):
        self.config = config or {}

    def connect(self):
        raise NotImplementedError

    def read(self):
        raise NotImplementedError


def make_connector(name, source_type):
    return type(name, (BaseConnector,), {
        'source_type': source_type,
        'connect': lambda self: f"connected to {self.source_type}",
        'read': lambda self: [1, 2, 3]
    })


if __name__ == '__main__':
    MySQLConnector = make_connector('MySQLConnector', 'mysql')
    APIConnector = make_connector('APIConnector', 'api')
    FileConnector = make_connector('FileConnector', 'file')

    for C in (MySQLConnector, APIConnector, FileConnector):
        inst = C()
        print(C.__name__, inst.connect(), inst.read())
