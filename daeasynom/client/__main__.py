import logging
import threading
from dataclasses import asdict
import zmq

from ..utils import DataPacket


logger = logging.getLogger(__name__)


class Client:
    def __init__(self, ident=None, port=5555) -> None:
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.DEALER)
        if ident is not None:
            self.socket.setsockopt(zmq.IDENTITY, ident)
        self.socket.connect(f"tcp://localhost:{port}")
 
    async def loop(self):
        requests = [f"Req #{i}" for i in range(5)]

        for request in requests:
            req = asdict(DataPacket(data=request))
            self.socket.send_json(req)
            logger.info(f"Client sent request: {request}")

        response = DataPacket()
        while response.data != "REQ #9":
            response = DataPacket.from_json_str(self.socket.recv())
            print(f"Client received response: {response}")
            if response.data == "REQ #9":
                break

    def stop(self):
        self.socket.close()
        self.ctx.term()

class ClientThread(threading.Thread):
    def __init__(self, port=5555):
        super().__init__()
        self.client = None
        self.port = port

    def run(self):
        import asyncio

        asyncio.set_event_loop(asyncio.new_event_loop())
        self.client = Client(port=self.port)

        try:
            asyncio.run(self.client.loop())
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.exception(e)
        finally:
            self.client.stop()