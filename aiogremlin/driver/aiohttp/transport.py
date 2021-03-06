from aiogremlin.typehints import *

import asyncio
import aiohttp

from gremlin_python.driver import transport
from yarl import URL

from autologging import logged, traced

MaybeSSLContext = Optional[aiohttp.connector.SSLContext]


@logged
@traced
class AiohttpTransport(transport.AbstractBaseTransport):
    _client_session: aiohttp.ClientSession
    _ws: aiohttp.client.ClientWebSocketResponse

    def __init__(self, loop: MaybeLoop = None) -> None:
        self._loop = loop
        self._connected = False

    async def connect(self, url: Union[URL, str], *, ssl_context: MaybeSSLContext = None) -> None:
        await self.close()
        connector = aiohttp.TCPConnector(
            ssl_context=ssl_context, loop=self._loop)
        self._client_session = aiohttp.ClientSession(
            loop=self._loop, connector=connector)
        # _task = self._loop.create_task(self._client_session.ws_connect(url))
        # await _task
        # self._ws = _task.result()
        self._ws = await self._client_session.ws_connect(url)
        self._connected = True

    async def write(self, message: bytes):
        coro = self._ws.send_bytes(message)
        if asyncio.iscoroutine(coro):
            await coro

    async def read(self):
        data = await self._ws.receive()
        if data.type == aiohttp.WSMsgType.close:
            await self._transport.close()
            raise RuntimeError("Connection closed by server")
        elif data.type == aiohttp.WSMsgType.error:
            # This won't raise properly, fix
            raise data.data
        elif data.type == aiohttp.WSMsgType.closed:
            # Hmm
            raise RuntimeError("Connection closed by server")
        elif data.type == aiohttp.WSMsgType.text:
            # Should return bytes
            data = data.data.strip().encode('utf-8')
        else:
            data = data.data
        return data

    async def close(self):
        if self._connected:
            if not self._ws.closed:
                await self._ws.close()
            if not self._client_session.closed:
                await self._client_session.close()

    @property
    def closed(self):
        return self._ws.closed or self._client_session.closed
