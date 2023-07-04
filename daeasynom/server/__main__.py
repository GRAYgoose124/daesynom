import asyncio
import queue
import random
import time
import zmq
import zmq.asyncio
import threading
import logging

from ..utils import DataPacket
from .worker import AbstractWorkerThread


logger = logging.getLogger(__name__)


class AbstractServer(threading.Thread):
    Worker = AbstractWorkerThread
    def __init__(self, port=5555, workers=25):
        super().__init__()
        self.ctx = zmq.asyncio.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.bind(f"tcp://*:{port}")
        self.worker_socket = self.ctx.socket(zmq.DEALER)
        self.worker_port = self.worker_socket.bind_to_random_port("tcp://*")
        self.poller = zmq.asyncio.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.poller.register(self.worker_socket, zmq.POLLIN)

        self.workers = {}
        self.occupied_workers = {}
        self.pending_requests = {}
        self.work_queue = queue.Queue()
        
        for _ in range(workers):
            worker = self.Worker(self.work_queue, port=self.worker_port)
            worker.daemon = True
            worker.start()
            self.workers[worker.ident] = worker

        self.running = False
        
    def run(self):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        try:
            asyncio.run(self.loop())
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.exception(e)
        finally:
            self.stop()

    async def loop(self):
        # Start listening for client requests
        self.running = True
        while self.running:
            socks = dict(await self.poller.poll())

            # TODO: abstractify task loops
            asyncio.create_task(self.handle_request(socks))
            asyncio.create_task(self.handle_response(socks))

            if not self.work_queue.empty() and socks:
                asyncio.create_task(self.handle_work_queue())

            await asyncio.sleep(0)

    async def handle_work_queue(self):
        while not self.work_queue.empty():
            try:
                proc_time, worker_id, req_id = self.work_queue.get(block=False, timeout=1)
            except queue.Empty:
                return
            
            if req_id:
                self.occupied_workers[worker_id] = proc_time
            else:
                start_time = self.occupied_workers.pop(worker_id, None)

                if start_time is None:
                    logger.warning(f"Worker {worker_id} not found in occupied workers")
                else:
                    diff = proc_time - start_time
                    logger.debug(f"Worker {worker_id} finished processing request {req_id} in {diff} seconds")
        
            self.work_queue.task_done()

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

            response.data = request.data

            response = response.to_json_str().encode("utf-8")
            await self.socket.send_multipart([client_identity, response])

    def stop(self):
        self.running = False
        self.worker_socket.close()
        self.socket.close()
        self.ctx.term()

        for worker in self.workers.values():
            worker.join()
        
        super().stop()











