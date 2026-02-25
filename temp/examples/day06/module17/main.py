import os
import sys

sys.path.insert(0, str(os.path.dirname(os.path.dirname(__file__))))

os.environ.setdefault("ENV", "dev")

from config.loader import load_config
from config.validator import ConfigValidationError
from pipeline.reader import OrderReader
from pipeline.transformer import RecordTransformer
from pipeline.writer import OrderWriter


def run_pipeline() -> None:
    try:
        config = load_config()
    except (FileNotFoundError, ValueError, ConfigValidationError) as e:
        print(f"[FATAL] Pipeline startup failed: {e}")
        sys.exit(1)

    reader = OrderReader(config)
    transformer = RecordTransformer(config)
    writer = OrderWriter(config)

    print("\n[Pipeline] Starting ingestion...")
    records = reader.read()

    print("\n[Pipeline] Starting transformation...")
    transformed = transformer.transform(records)

    print("\n[Pipeline] Starting write...")
    writer.write(transformed)

    print("\n[Pipeline] Completed successfully.")


if __name__ == "__main__":
    run_pipeline()
