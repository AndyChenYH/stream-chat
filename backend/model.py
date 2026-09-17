import os
import grpc
from shared import model_pb2 as pb, model_pb2_grpc as rpc
from shared.tls import pem


class Model:
    def __init__(self):
        credentials = grpc.ssl_channel_credentials(pem('TLS_CA'), pem('TLS_CLIENT_KEY'), pem('TLS_CLIENT_CERT'))
        # The certificate SAN is model-worker; verify it against our private CA even
        # when Runpod assigns a numeric IP and a dynamically forwarded port.
        self.channel = grpc.aio.secure_channel(os.environ['WORKER_ADDRESS'], credentials,
            options=[('grpc.ssl_target_name_override', 'model-worker'), ('grpc.max_receive_message_length', 1048576)])
        self.stub = rpc.ModelWorkerStub(self.channel)

    async def close(self):
        await self.channel.close()

    async def ready(self):
        result = await self.stub.Ready(pb.ReadyRequest(), timeout=10)
        return {'ready': result.ready, 'model': result.model}

    async def generate(self, run_id, messages):
        call = self.stub.Generate(pb.GenerateRequest(run_id=str(run_id),
            messages=[pb.Message(**m) for m in messages], max_tokens=1024), timeout=180)
        try:
            async for chunk in call:
                yield chunk
        finally:
            call.cancel()
