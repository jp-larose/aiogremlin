import asyncio
import functools
from autologging import logged, traced
import logging

from aiogremlin import exception


def error_handler(fn):
    @functools.wraps(fn)
    async def wrapper(self):
        msg = await fn(self)
        if msg:
            if msg.status_code not in [200, 206]:
                self.close()
                raise exception.GremlinServerError(
                    msg.status_code,
                    "{0}: {1}".format(msg.status_code, msg.message))
            if isinstance(msg.data, list) and msg.data:
                msg = msg.data[0]
            else:
                msg = msg.data
        return msg
    return wrapper


@traced
@logged
class ResultSet:
    """Gremlin Server response implementated as an async iterator."""
    def __init__(self, request_id, timeout, loop=None):
        self._response_queue = asyncio.Queue(loop=loop)
        self._request_id = request_id
        self._loop = loop
        self._timeout = timeout
        self._done = asyncio.Event(loop=self._loop)
        self._aggregate_to = None

    @property
    def request_id(self):
        return self._request_id

    @property
    def stream(self):
        return self._response_queue

    def queue_result(self, result):
        if result is None:
            self.close()
        self._response_queue.put_nowait(result)

    @property
    def done(self):
        """
        Readonly property.

        :returns: `asyncio.Event` object
        """
        return self._done

    @property
    def aggregate_to(self):
        return self._aggregate_to

    @aggregate_to.setter
    def aggregate_to(self, val):
        self._aggregate_to = val

    def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self.one()
        if not msg:
            raise StopAsyncIteration
        return msg

    def close(self):
        self.done.set()
        self._loop = None

    @error_handler
    async def one(self):
        """Get a single message from the response stream"""
        if not self._response_queue.empty():
            self.__log.debug("Response queue not empty")
            msg = self._response_queue.get_nowait()
        elif self.done.is_set():
            self.__log.debug("'done' condition is set")
            msg = None
        else:
            self.__log.debug(f"Trying to get from response queue.  {self._response_queue=} {self._timeout=} {self._loop=}")
            try:
                msg = await asyncio.wait_for(self._response_queue.get(),
                                             timeout=self._timeout,
                                             loop=self._loop)
                self._response_queue.task_done()
            except asyncio.TimeoutError:
                self.close()
                raise exception.ResponseTimeoutError('Response timed out')
        self.__log.debug(f"{msg=} {type(msg)=}")
        return msg

    async def all(self):
        results = []
        async for result in self:
            results.append(result)
        return results
