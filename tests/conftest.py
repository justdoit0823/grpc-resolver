
from concurrent import futures

import grpc
import pytest

from grpcresolver import EtcdServiceResolver, EtcdServiceRegistry
import hello_pb2_grpc
import rpc


_TEST_GRPC_PORT = (
    '127.0.0.1:50031', '127.0.0.1:50032', '127.0.0.1:50033', '127.0.0.1:50034')


@pytest.fixture(scope='module')
def grpc_addr():
    return _TEST_GRPC_PORT


@pytest.fixture(scope='module')
def grpc_server(grpc_addr):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    hello_pb2_grpc.add_HelloServicer_to_server(rpc.HelloGRpcServer(), server)
    for addr in grpc_addr:
        server.add_insecure_port(addr)

    server.start()

    yield server

    server.stop(0)


@pytest.fixture(scope='function')
def grpc_resolver(mocker, grpc_addr):
    client = mocker.Mock()
    resolver = EtcdServiceResolver(etcd_client=client, start_listener=False)

    def resolve(service_name):
        return grpc_addr

    old_resolve = resolver.resolve
    setattr(resolver, 'resolve', resolve)
    yield resolver

    setattr(resolver, 'resolve', old_resolve)


@pytest.fixture(scope='function')
def etcd_registry(mocker):
    client = mocker.Mock()
    registry = EtcdServiceRegistry(etcd_client=client)

    return registry
