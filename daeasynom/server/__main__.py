import asyncio
import queue
import time
import zmq
import zmq.asyncio
import logging

from ..utils import DataPacket, StoppableThread
from .worker import AbstractWorkerThread


logger = logging.getLogger(__name__)


class AbstractServer(StoppableThread):
    Worker = AbstractWorkerThread

    def __init__(self, port=5555, workers=25):
        super().__init__()
        self._loop = None
        self.tasks = []

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

    def run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        try:
            base_task = asyncio.ensure_future(self.loop())
            self.tasks.append(base_task)
            self._loop.run_until_complete(base_task)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.exception(e)

    async def loop(self):
        # Start listening for client requests
        while not self.stopped:
            for task in self.tasks:
                if task.done():
                    self.tasks.remove(task)

            socks = dict(await self.poller.poll())

            # TODO: abstractify task loops
            if self.socket in socks:
                self.tasks.append(asyncio.ensure_future(self.handle_request(socks)))

            if self.worker_socket in socks:
                self.tasks.append(asyncio.ensure_future(self.handle_response(socks)))

            if not self.work_queue.empty() and socks:
                self.tasks.append(asyncio.ensure_future(self.handle_work_queue()))

            await asyncio.sleep(0)

    async def handle_work_queue(self):
        while not self.work_queue.empty():
            try:
                proc_time, worker_id, req_id = self.work_queue.get(
                    block=False, timeout=1
                )
            except queue.Empty:
                req_id = None

            logger.debug(f"Worker {worker_id} processing request {req_id}")

            if req_id and worker_id not in self.occupied_workers:
                self.occupied_workers[worker_id] = proc_time
            else:
                start_time = self.occupied_workers.pop(worker_id, None)

                if start_time is None:
                    logger.warning(f"Worker {worker_id} not found in occupied workers")
                else:
                    diff = time.process_time() - start_time
                    logger.info(
                        f"Worker {worker_id} processing request {req_id} for {diff} seconds"
                    )

            self.work_queue.task_done()

    async def handle_request(self, socks):
        # Receive request from client and send to a worker
        client_identity, req_bytes = await self.socket.recv_multipart()
        request = DataPacket.from_json_str(req_bytes.decode())
        request.set_status("pending")
        self.pending_requests[request.id] = [client_identity, request]

        request = request.to_json_str().encode("utf-8")
        await self.worker_socket.send(request)

    async def handle_response(self, socks):
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
        super().stop()
        self.poller.unregister(self.socket)
        self.poller.unregister(self.worker_socket)
        self.socket.close()
        self.worker_socket.close()
        self.ctx.term()

        # destroy all pending tasks and stop the loop
        for task in self.tasks:
            task.cancel()

        self._loop.stop()

        # stop all workers
        for worker in self.workers.values():
            worker.stop()
            worker.join()
