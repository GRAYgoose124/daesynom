import logging
import random

from .server import ServerThread
from .client import ClientThread

def main():
    logging.basicConfig(level=logging.INFO)
    
    port = 5555 + random.randint(0, 1000)
    server_thread = ServerThread(port=port)
    client_thread = ClientThread(port=port)

    server_thread.start()
    client_thread.start()

    try:
        while server_thread.is_alive():
            server_thread.join(timeout=3)
    except KeyboardInterrupt:
        server_thread.server.stop()
    finally:
        server_thread.join()
        client_thread.join()

    