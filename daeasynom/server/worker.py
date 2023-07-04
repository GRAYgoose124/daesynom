from abc import ABCMeta, abstractmethod
import time
import zmq
import zmq.asyncio
import logging


from ..utils import DataPacket, StoppableThread


logger = logging.getLogger(__name__)


class AbstractWorkerThread(StoppableThread, metaclass=ABCMeta):
    def __init__(self, work_queue, port=5555):
        super().__init__()
        self.work_queue = work_queue
        self.port = port

    def run(self):
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.DEALER)
        self.socket.connect(
            f"tcp://localhost:{self.port}"
        )  # Connect to the main server thread

        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        while not self.stopped:
            # timeout recv
            try:
                socks = dict(
                    poller.poll(timeout=1000)
                )  # Set a timeout value (in milliseconds)
            except zmq.error.ZMQBaseError as e:
                break

            if self.socket in socks and socks[self.socket] == zmq.POLLIN:
                try:
                    request = DataPacket.from_json_str(self.socket.recv_string())
                except zmq.error.ZMQBaseError:
                    break
                self.work_queue.put((time.process_time(), self.ident, request.id))
                response = self.process_request(request)
                self.work_queue.put((time.process_time(), self.ident, request.id))

                try:
                    self.socket.send(response.to_json_str().encode("utf-8"))
                except zmq.error.ZMQBaseError as e:
                    break

    def stop(self):
        self.socket.close()
        self.ctx.term()
        super().stop()

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
