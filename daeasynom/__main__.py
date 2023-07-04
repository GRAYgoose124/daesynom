import logging
import random
import sys
import time
import zmq

from .server import AbstractServer, AbstractWorkerThread
from .client import AbstractClient
from .utils import DataPacket

logger = logging.getLogger(__name__)


class DumbClient(AbstractClient):
    async def loop(self):
        n = 10
        requests = [f"Req #{i}" for i in range(n)]

        for request in requests:
            self.send(DataPacket(data=request))

        try:
            for _ in range(n):
                response = self.recv()
        except zmq.error.ZMQBaseError:
            pass

        logger.debug("Client stopped")


class LazyServer(AbstractServer):
    class Worker(AbstractWorkerThread):
        def request_handler(self, request: DataPacket, response: DataPacket):
            time.sleep(random.random() * 5)
            logger.debug(f"Worker-{self.ident} processed request {request}")


def main():
    logging.basicConfig(level=logging.DEBUG)

    port = 5555 + random.randint(0, 1000)
    server_thread = LazyServer(port=port)
    client_thread = DumbClient(port=port)

    server_thread.start()
    client_thread.start()

    try:
        while server_thread.is_alive():
            # Only killing server for demo purposes.
            if not client_thread.is_alive():
                logger.debug("Stopping client-server daemon")
                break
            server_thread.join(2)
    except KeyboardInterrupt:
        logger.debug("KeyboardInterrupt")
    except Exception as e:
        logging.exception(e)
    finally:
        if client_thread.is_alive():
            client_thread.stop()
        if server_thread.is_alive():
            server_thread.stop()

        client_thread.join()

        tries_remaining = 5
        while server_thread.is_alive() and tries_remaining:
            logger.debug(
                f"Waiting for server to stop ({tries_remaining} tries remaining)"
            )
            server_thread.join(2)
            tries_remaining -= 1

        if tries_remaining == 0:
            logger.warning("Server did not stop after 10 seconds")
            sys.exit(1)
