import logging
import signal

from .server import ServerThread
from .client import ClientThread

def main():
    logging.basicConfig(level=logging.INFO)
    
    server_thread = ServerThread()
    client_thread = ClientThread()

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

    