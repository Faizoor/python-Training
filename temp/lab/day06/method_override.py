class BaseConnection:
    def connect(self):
        print("Base: initializing connection")

class SubConnection(BaseConnection):
    def connect(self):
        print("Sub: pre-connection steps")
        super().connect()
        print("Sub: post-connection steps")


def main():
    conn = SubConnection()
    conn.connect()


if __name__ == '__main__':
    main()
