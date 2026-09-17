"""Resolve Runpod's host model cache without redownloading the weights."""
import os
from pathlib import Path


def resolve(model, root=Path('/runpod-volume/huggingface-cache/hub')):
    cache = Path(root) / ('models--' + model.replace('/', '--'))
    ref = cache / 'refs/main'
    if ref.is_file():
        candidate = cache / 'snapshots' / ref.read_text().strip()
        if (candidate / 'config.json').is_file():
            return str(candidate)
    candidates = list((cache / 'snapshots').glob('*/config.json'))
    if len(candidates) == 1:
        return str(candidates[0].parent)
    if os.environ.get('REQUIRE_MODEL_CACHE', '1') == '1':
        raise RuntimeError('Configure the endpoint cached model before starting a worker')
    return model


if __name__ == '__main__':
    print(resolve(os.environ.get('MODEL_NAME', 'Qwen/Qwen3-4B-Instruct-2507')))
