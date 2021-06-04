import pytest

from yaaredis.exceptions import RedisClusterException
from yaaredis.pool import ClusterConnectionPool


async def get_pool(connection_kwargs=None, max_connections=None,
                   startup_nodes=None):
    startup_nodes = startup_nodes or [{'host': '127.0.0.1', 'port': 7000}]
    connection_kwargs = connection_kwargs or {}
    pool = ClusterConnectionPool(
        max_connections=max_connections,
        startup_nodes=startup_nodes,
        readonly=True,
        **connection_kwargs)
    await pool.initialize()
    return pool


@pytest.mark.asyncio()
async def test_repr_contains_db_info_readonly():
    """
    Note: init_slot_cache must be set to false otherwise it will try to query
    the test server for data and then it can't be predicted reliably
    """
    pool = await get_pool(
        startup_nodes=[{'host': '127.0.0.1', 'port': 7000},
                       {'host': '127.0.0.2', 'port': 7001}],
    )
    assert 'ClusterConnection<host=127.0.0.2,port=7001>' in repr(pool)
    assert 'ClusterConnection<host=127.0.0.1,port=7000>' in repr(pool)


@pytest.mark.asyncio()
async def test_max_connections():
    pool = await get_pool(max_connections=2)
    pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7000})
    pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7001})
    with pytest.raises(RedisClusterException):
        pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7000})


@pytest.mark.asyncio()
async def test_get_node_by_slot():
    """
    We can randomly get all nodes in readonly mode.
    """
    pool = await get_pool(connection_kwargs={})

    expected_ports = set(range(7000, 7006))
    actual_ports = set()
    for i in range(0, 16383):
        node = pool.get_node_by_slot(i)
        actual_ports.add(node['port'])
    assert actual_ports == expected_ports
