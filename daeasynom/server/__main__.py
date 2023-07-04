import time
import zmq
import zmq.asyncio
import threading
import logging

from .worker import WorkerThread

logger = logging.getLogger(__name__)


class Server:
    def __init__(self, port=5555):
        super().__init__()
        self.ctx = zmq.asyncio.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.bind(f"tcp://*:{port}")
        self.worker_socket = self.ctx.socket(zmq.DEALER)
        self.worker_port = self.worker_socket.bind_to_random_port("tcp://*")
        self.poller = zmq.asyncio.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.poller.register(self.worker_socket, zmq.POLLIN)

        self.workers = None
        self._init_workers(10)

        self.running = False

    def _init_workers(self, n=3):
        self.workers = []
        for i in range(n):
            worker = WorkerThread(port=self.worker_port)
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
    def __init__(self, port=5555):
        super().__init__()
        self.server = None
        self.port = port

    def run(self, port=5555):
        import asyncio

        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        self.server = Server(port=self.port)

        try:
            asyncio.run(self.server.run())
        except KeyboardInterrupt:
            pass
        finally:
            self.server.stop()







