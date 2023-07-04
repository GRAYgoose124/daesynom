import logging
import random

from .server import Server
from .client import ClientThread

def main():
    logging.basicConfig(level=logging.DEBUG)
    
    port = 5555 + random.randint(0, 1000)
    server_thread = Server(port=port)
    client_thread = ClientThread(port=port)

    server_thread.start()
    client_thread.start()

    try:
        while server_thread.is_alive():
            server_thread.join(timeout=3)
    except KeyboardInterrupt:
        server_thread.stop()
    except Exception as e:
        logging.exception(e)
    finally:
        server_thread.join()
        client_thread.join()

    