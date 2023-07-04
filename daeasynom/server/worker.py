from abc import ABCMeta, abstractmethod
import time
import zmq
import zmq.asyncio
import threading
import logging
import random

from daeasynom.utils import DataPacket

from ..utils import DataPacket


logger = logging.getLogger(__name__)


class AbstractWorkerThread(threading.Thread, metaclass=ABCMeta):
    def __init__(self, port=5555):
        super().__init__()
        self.port = port

    def run(self):
        context = zmq.Context()
        worker_socket = context.socket(zmq.DEALER)
        worker_socket.connect(
            f"tcp://localhost:{self.port}"
        )  # Connect to the main server thread

        while True:
            request = DataPacket.from_json_str(worker_socket.recv_string())
            logger.debug(
                f"WorkerThread(ident={self.ident}) received request: {request}"
            )

            response = self.process_request(request)

            worker_socket.send(response.to_json_str().encode("utf-8"))

    def process_request(self, request: DataPacket) -> DataPacket:
        response = DataPacket(id=request.id, ty="response")

        self.request_handler(request, response)
        if response.id != request.id:
            logger.warning(
                f"Response and request id mismatch: {response.id} != {request.id}, tampering?"
            )

        response.add_result("processed_by", f"Worker-{self.ident}")
        return response
   
    @abstractmethod
    def request_handler(self, request: DataPacket, response: DataPacket):
        pass


class SlothfulWorkerThread(AbstractWorkerThread):
    def request_handler(self, request: DataPacket, response: DataPacket):
        time.sleep(random.random() * 5)