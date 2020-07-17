# Copyright 2016 David M. Brown
#
# This file is part of Goblin.
#
# Goblin is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Goblin is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Goblin.  If not, see <http://www.gnu.org/licenses/>.
import asyncio
import pytest
import logging

from aiogremlin.structure.graph import Graph
from gremlin_python.process.traversal import T
from aiogremlin import driver
from aiogremlin.driver.provider import TinkerGraph
from gremlin_python.driver import serializer
from aiogremlin.remote.driver_remote_connection import DriverRemoteConnection

log = logging.getLogger(__name__)

# def pytest_generate_tests(metafunc):
#     if 'cluster' in metafunc.fixturenames:
#         metafunc.parametrize("cluster", ['c1', 'c2'], indirect=True)


def pytest_addoption(parser):
    parser.addoption('--provider', default='tinkergraph',
                     choices=('tinkergraph', 'dse',))
    parser.addoption('--gremlin-host', default='gremlin-server')
    parser.addoption('--gremlin-port', default='8182')


@pytest.fixture
def provider(request):
    provider = request.config.getoption('provider')
    if provider == 'tinkergraph':
        return TinkerGraph
    elif provider == 'dse':
        try:
            import goblin_dse
        except ImportError:
            raise RuntimeError("Couldn't run tests with DSEGraph provider: the goblin_dse package "
                               "must be installed")
        else:
            return goblin_dse.DSEGraph


@pytest.fixture
def aliases(request):
    if request.config.getoption('provider') == 'tinkergraph':
        return {'g': 'g'}
    elif request.config.getoption('provider') == 'dse':
        return {'g': 'testgraph.g'}


@pytest.fixture
def gremlin_server():
    return driver.GremlinServer


@pytest.fixture
def unused_server_url(unused_tcp_port):
    return 'https://localhost:{}/gremlin'.format(unused_tcp_port)


@pytest.fixture
def gremlin_host(request):
    return request.config.getoption('gremlin_host')


@pytest.fixture
def gremlin_port(request):
    return request.config.getoption('gremlin_port')


@pytest.fixture
def gremlin_url(gremlin_host, gremlin_port):
    return "ws://{}:{}/gremlin".format(gremlin_host, gremlin_port)


@pytest.fixture
async def connection(gremlin_url, provider):
    log.debug('starting fixture: connection')
    try:
        conn = await driver.Connection.open(
                url=gremlin_url,
                message_serializer=serializer.GraphSONMessageSerializer,
                provider=provider
            )
    except OSError:
        pytest.skip('Gremlin Server is not running')
    log.debug(f"{conn=}")
    yield conn

    log.debug('tearing down fixture: connection')
    if not conn.closed:
        await conn.close()


@pytest.fixture
async def connection_pool(gremlin_url, provider):
    pool = await driver.ConnectionPool.open(
        url=gremlin_url, max_conns=4, min_conns=1, max_times_acquired=16,
        max_inflight=64, response_timeout=10000, message_serializer=serializer.GraphSONMessageSerializer, provider=provider)

    yield pool

    await pool.close()


@pytest.fixture
async def cluster(request, gremlin_host, gremlin_port, provider, aliases):
    # if request.param == 'c1':
    cluster = await driver.Cluster.open(
        hosts=[gremlin_host],
        port=gremlin_port,
        aliases=aliases,
        message_serializer=serializer.GraphSONMessageSerializer,
        provider=provider
    )
    # elif request.param == 'c2':
    #     cluster = driver.Cluster(
    #         hosts=[gremlin_host],
    #         port=gremlin_port,
    #         aliases=aliases,
    #         message_serializer=serializer.GraphSONMessageSerializer,
    #         provider=provider
    #     )
    yield cluster
    await cluster.close()


# TOOO FIX
# @pytest.fixture
# def remote_graph():
#      return driver.AsyncGraph()


# Class fixtures
@pytest.fixture
def cluster_class():
    return driver.Cluster


@pytest.fixture
async def remote_connection(gremlin_url):
    try:
        remote_conn = await DriverRemoteConnection.open(url=gremlin_url, aliases='g')
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        yield remote_conn
        await remote_conn.close()


@pytest.fixture(autouse=True)
async def run_around_tests(remote_connection):
    g = Graph().traversal().withRemote(remote_connection)

    async def create_graph():
        await g.V().drop().iterate()
        software1 = await g.addV("software").property("name", "lop").property("lang", "java").property(T.id, 3).next()
        software2 = await g.addV("software").property("name", "ripple").property("lang", "java").property(T.id, 5).next()
        person1 = await g.addV("person").property("name", "marko").property("age", "29").property(T.id, 1).next()
        person2 = await g.addV("person").property("name", "vadas").property("age", "27").property(T.id, 2).next()
        person3 = await g.addV("person").property("name", "josh").property("age", "32").property(T.id, 4).next()
        person4 = await g.addV("person").property("name", "peter").property("age", "35").property(T.id, 6).next()

        knows1 = await g.addE("knows").from_(person1).to(person2).property("weight", 0.5).property(T.id, 7).next()
        knows2 = await g.addE("knows").from_(person1).to(person3).property("weight", 1,0).property(T.id, 8).next()
        created1 = await g.addE("created").from_(person1).to(software1).property("weight", 0.4).property(T.id, 9).next()
        created2 = await g.addE("created").from_(person3).to(software2).property("weight", 1.0).property(T.id, 10).next()
        created3 = await g.addE("created").from_(person3).to(software1).property("weight", 1.0).property(T.id, 11).next()
        created4 = await g.addE("created").from_(person4).to(software1).property("weight", 0.2).property(T.id, 12).next()

    #loop = asyncio.get_event_loop()
    #loop.run_until_complete(create_graph())
    await create_graph()
    yield
