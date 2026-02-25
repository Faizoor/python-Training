class LogFileIterator:
    """
    Streams lines from a log file one at a time.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._file = None

    def __iter__(self):
        self._file = open(self.filepath, "r")
        return self

    def __next__(self):
        line = self._file.readline()
        if not line:
            self._file.close()
            raise StopIteration
        return line.strip()
