import logging
import random
import time

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

        for _ in range(n):
            response = self.recv()

        self.stop()
        logger.debug("Client stopped")


class LazyServer(AbstractServer):
    class Worker(AbstractWorkerThread):
        def request_handler(self, request: DataPacket, response: DataPacket):
            time.sleep(random.random() * 5)
            logger.debug(f"Worker {self.ident} processed request {request}")


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
                server_thread.stop()
                break
            server_thread.join(timeout=2)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.exception(e)
    finally:
        if server_thread.is_alive():
            server_thread.stop()
        if client_thread.is_alive():
            client_thread.stop()

        server_thread.join()
        client_thread.join()
