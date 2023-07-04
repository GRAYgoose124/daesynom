from abc import ABCMeta, abstractmethod
import time
import zmq
import zmq.asyncio
import threading
import logging

from daeasynom.utils import DataPacket

from ..utils import DataPacket


logger = logging.getLogger(__name__)


class AbstractWorkerThread(threading.Thread, metaclass=ABCMeta):
    def __init__(self, work_queue, port=5555):
        super().__init__()
        self.work_queue = work_queue
        self.port = port

    def run(self):
        context = zmq.Context()
        worker_socket = context.socket(zmq.DEALER)
        worker_socket.connect(
            f"tcp://localhost:{self.port}"
        )  # Connect to the main server thread

        while True:
            request = DataPacket.from_json_str(worker_socket.recv_string())

            self.work_queue.put((time.process_time(), self.ident, request.id))
            response = self.process_request(request)
            self.work_queue.put((time.process_time(), self.ident, request.id))

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
