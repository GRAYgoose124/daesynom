import logging
import signal
import threading
from dataclasses import asdict
import zmq

from .utils import DataPacket


logger = logging.getLogger(__name__)


class ClientThread(threading.Thread):
    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        socket.setsockopt(zmq.IDENTITY, b"Client1")
        socket.connect("tcp://localhost:5555")

        try:
            requests = [f"Req #{i}" for i in range(5)]

            for request in requests:
                req = asdict(DataPacket(data=request))
                socket.send_json(req)
                logger.info(f"Client sent request: {request}")

            response = DataPacket()
            while response.data != "REQ #9":
                response = DataPacket.from_json_str(socket.recv())
                print(f"Client received response: {response}")
                if response.data == "REQ #9":
                    break
        except KeyboardInterrupt:
            pass
        finally:
            socket.close()
            context.term()