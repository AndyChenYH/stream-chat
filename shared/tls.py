import base64
import os
from pathlib import Path


def pem(name: str) -> bytes:
    """Use injected base64 secrets in cloud, or explicitly configured local files."""
    if value := os.getenv(name + '_B64'):
        return base64.b64decode(value, validate=True)
    return Path(os.environ[name + '_FILE']).read_bytes()
