import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# boto3 clients are created at import time in the handler. Give them a
# region so the import works without AWS configuration. No calls are made.
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")
