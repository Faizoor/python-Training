from readers import CSVReader, MockDBReader
from datasource import DataSource


def run_pipeline(source: DataSource):
    data = source.read()
    for row in data:
        print(row)


if __name__ == '__main__':
    # Ensure lab/data/sample.csv exists with header id,name and a couple rows
    csv_reader = CSVReader('data/sample.csv')
    mock_reader = MockDBReader()

    print("CSVReader output:")
    run_pipeline(csv_reader)

    print("\nMockDBReader output:")
    run_pipeline(mock_reader)
