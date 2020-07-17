import asyncio
import base64
import collections
from autologging import traced, logged

try:
    import ujson as json
except ImportError:
    import json

from gremlin_python.driver import protocol, request, serializer
from aiogremlin.driver.resultset import ResultSet


__author__ = 'David M. Brown (davebshow@gmail.com)'


Message = collections.namedtuple(
    "Message",
    ["status_code", "data", "message"])


@traced
@logged
class GremlinServerWSProtocol(protocol.AbstractBaseProtocol):
    """Implemenation of the Gremlin Server Websocket protocol"""
    def __init__(self, message_serializer, username='', password=''):
        if isinstance(message_serializer, type):
            message_serializer = message_serializer()
        self._message_serializer = message_serializer
        self._username = username
        self._password = password

    def connection_made(self, transport):
        self._transport = transport

    async def write(self, request_id, request_message):
        message = self._message_serializer.serialize_message(
            request_id, request_message)
        func = self._transport.write(message)
        if asyncio.iscoroutine(func):
            await func

    async def data_received(self, data, results_dict=None):
        if data is None:
            raise protocol.GremlinServerError({'code': 500, 'message': 'Server disconnected', 'attributes': {}})
        if results_dict is None:
            results_dict = {}

        self.__log.debug(f"{data=}")
        message = self._message_serializer.deserialize_message(data)
        self.__log.debug(f"{message=}")
        request_id = message['requestId']
        status_code = message['status']['code']
        result_data = message['result']['data']
        msg = message['status']['message']

        if request_id in results_dict:
            self.__log.debug(f"{request_id=} is in {results_dict=}")
            result_set = results_dict[request_id]
        else:
            result_set = ResultSet(None, None)

        aggregate_to = message['result']['meta'].get('aggregateTo', 'list')
        result_set.aggregate_to = aggregate_to

        if status_code == 407:
            auth = b''.join([b'\x00', self._username.encode('utf-8'),
                             b'\x00', self._password.encode('utf-8')])
            request_message = request.RequestMessage(
                'traversal', 'authentication',
                {'sasl': base64.b64encode(auth).decode()})
            await self.write(request_id, request_message)
        elif status_code == 204:
            self.__log.debug(f"{status_code=} Queuing None to ResultSet")
            result_set.queue_result(None)
        else:
            self.__log.debug(f"{status_code=}")
            if result_data:
                self.__log.debug(f"{result_data=}")
                for result in result_data:
                    self.__log.debug(f"{result=}")
                    message = Message(status_code, result, msg)
                    self.__log.debug(f"Queuing {message=}")
                    result_set.queue_result(message)
                #message = Message(status_code, result_data, msg)
                #result_set.queue_result(message)
            else:
                message = Message(status_code, data, msg)
                result_set.queue_result(message)
            if status_code != 206:
                result_set.queue_result(None)
