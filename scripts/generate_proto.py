from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
subprocess.run([sys.executable, '-m', 'grpc_tools.protoc', '-Iproto',
    '--python_out=shared', '--grpc_python_out=shared', 'proto/model.proto'], cwd=root, check=True)
path = root / 'shared/model_pb2_grpc.py'
path.write_text(path.read_text().replace('import model_pb2 as', 'from . import model_pb2 as'))
