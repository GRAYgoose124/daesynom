from abc import ABCMeta, abstractmethod
import asyncio
import logging
import zmq

from dataclasses import asdict

from ..utils import DataPacket, StoppableThread


logger = logging.getLogger(__name__)


class AbstractClient(StoppableThread, metaclass=ABCMeta):
    def __init__(self, ident=None, port=5555) -> None:
        super().__init__()
        self.client = None
        self.port = port

        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.DEALER)
        if ident is not None:
            self.socket.setsockopt(zmq.IDENTITY, ident)

        self.socket.connect(f"tcp://localhost:{port}")

    def run(self):
        try:
            asyncio.run(self.loop())
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.exception(e)
        finally:
            self.stop()

    @abstractmethod
    async def loop(self):
        pass

    def stop(self):
        self.socket.close()
        self.ctx.term()
        super().stop()

    def send(self, request: DataPacket):
        self.socket.send_json(asdict(request))
        logger.info(f"Client sent request: {request}")

    def recv(self) -> DataPacket:
        response = DataPacket.from_json_str(self.socket.recv())
        logger.info(f"Client received response: {response}")
        return response
