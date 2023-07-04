import signal
import time
import zmq
import zmq.asyncio
import threading
import logging
import random

from .utils import DataPacket


logger = logging.getLogger(__name__)


class WorkerThread(threading.Thread):
    def run(self):
        context = zmq.Context()
        worker_socket = context.socket(zmq.DEALER)
        worker_socket.connect(
            "tcp://localhost:5556"
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

        time.sleep(random.random() * 5)

        response.add_result("processed_by", f"WorkerThread(ident={self.ident})")
        return response


class Server:
    def __init__(self):
        super().__init__()
        self.ctx = zmq.asyncio.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.bind("tcp://*:5555")
        self.worker_socket = self.ctx.socket(zmq.DEALER)
        self.worker_socket.bind("tcp://*:5556")
        self.poller = zmq.asyncio.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.poller.register(self.worker_socket, zmq.POLLIN)

        self.workers = None
        self._init_workers(10)

        self.running = False

    def _init_workers(self, n=3):
        self.workers = []
        for i in range(n):
            worker = WorkerThread()
            worker.start()
            self.workers.append(worker)

    async def run(self):
        # Start listening for client requests
        self.running = True
        while self.running:
            socks = dict(await self.poller.poll())
            if self.socket in socks:
                # Receive client identity and request
                client_identity, request = await self.socket.recv_multipart()
                logger.debug(
                    f"Server received request from client {client_identity}: {request}"
                )

                # Send the request to an available worker
                await self.worker_socket.send(request)
            if self.worker_socket in socks:
                # Receive response from worker
                response = await self.worker_socket.recv()
                logger.debug(f"Server received response from worker: {response}")

                # Send the response back to the client
                await self.socket.send_multipart([client_identity, response])

    def stop(self):
        self.running = False
        self.worker_socket.close()
        self.socket.close()
        self.ctx.term()

class ServerThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.server = None

    def run(self):
        import asyncio

        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        self.server = Server()

        try:
            asyncio.run(self.server.run())
        except KeyboardInterrupt:
            pass
        finally:
            self.server.stop()







