import asyncio
import time
import zmq
import zmq.asyncio
import threading
import logging

from ..utils import DataPacket
from .worker import SlothfulWorkerThread

logger = logging.getLogger(__name__)


class Server:
    def __init__(self, port=5555, workers=10):
        super().__init__()
        self.ctx = zmq.asyncio.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.bind(f"tcp://*:{port}")
        self.worker_socket = self.ctx.socket(zmq.DEALER)
        self.worker_port = self.worker_socket.bind_to_random_port("tcp://*")
        self.poller = zmq.asyncio.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.poller.register(self.worker_socket, zmq.POLLIN)

        self.workers = []
        for i in range(workers):
            worker = SlothfulWorkerThread(port=self.worker_port)
            worker.start()
            self.workers.append(worker)

        self.running = False
        
        self.pending_requests = {}

    async def run(self):
        # Start listening for client requests
        self.running = True
        while self.running:
            socks = dict(await self.poller.poll())
            asyncio.create_task(self.handle_request(socks))
            asyncio.create_task(self.handle_response(socks))
            await asyncio.sleep(0)

    async def handle_request(self, socks):
        if self.socket in socks:
            # Receive request from client and send to a worker
            client_identity, req_bytes = await self.socket.recv_multipart()
            request = DataPacket.from_json_str(req_bytes.decode())
            request.set_status("pending")
            self.pending_requests[request.id] = [client_identity, request]

            request = request.to_json_str().encode("utf-8")
            await self.worker_socket.send(request)
    
    async def handle_response(self, socks):
        if self.worker_socket in socks:
            # Receive response from a worker and send to client
            res_bytes = await self.worker_socket.recv()
            response = DataPacket.from_json_str(res_bytes.decode())
            response.set_status("completed")
            
            client_identity, request = self.pending_requests[response.id]
            if request.id != response.id:
                logger.warning(
                    f"Response and request id mismatch: {response.id} != {request.id}, tampering?"
                )
                response.add_result("tampered", True)
                if response.id in self.pending_requests:
                    del self.pending_requests[response.id]
                    response.add_result("finished_request", response.id)
                if request.id in self.pending_requests:
                    del self.pending_requests[request.id]
                    response.add_result("finished_request", request.id)
            else:
                response.add_result("tampered", False)
                del self.pending_requests[response.id]

            response = response.to_json_str().encode("utf-8")
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
        except Exception as e:
            logger.exception(e)
        finally:
            self.server.stop()







