
# -*- encoding: utf-8 -*-

import pytest

from grpcresolver import RoundrobinChannel
import hello_pb2
import hello_pb2_grpc


@pytest.mark.parametrize(
    'message, future', (
        ('heslewle', False),
        ('sfwefewfwef', True),
        ('你好', False))
)
def test_unary_unary(grpc_server, grpc_resolver, message, future):
    channel = RoundrobinChannel('grpcresolver.hello.Hello', grpc_resolver)
    stub = hello_pb2_grpc.HelloStub(channel)

    request = hello_pb2.HelloRequest(message=message)
    if future:
        response = stub.Greeter.future(request).result()
    else:
        response = stub.Greeter(request)

    assert response.message


@pytest.mark.parametrize(
    'message', (
        'heslewle',
        'sfwefewfwef',
        '你好')
)
def test_unary_stream(grpc_server, grpc_resolver, message):
    channel = RoundrobinChannel('grpcresolver.hello.Hello', grpc_resolver)
    stub = hello_pb2_grpc.HelloStub(channel)

    request = hello_pb2.HelloRequest(message=message)
    response_iter = stub.GreeterResponseStream(request)

    for response in response_iter:
        assert response.message


@pytest.mark.parametrize(
    'messages, future', (
        (('heslewle', 'sfwewef'), False),
        (('sfwefewfwef', 'sfwefwef'), True),
        (('shewfwefwef', '你好'), False))
)
def test_stream_unary(grpc_server, grpc_resolver, messages, future):
    channel = RoundrobinChannel('grpcresolver.hello.Hello', grpc_resolver)
    stub = hello_pb2_grpc.HelloStub(channel)

    request = (hello_pb2.HelloRequest(message=message) for message in messages)
    if future:
        response = stub.StreamGreeter.future(request).result()
    else:
        response = stub.StreamGreeter(request)

    assert response.message


@pytest.mark.parametrize(
    'messages', (
        ('heslewle', 'sfwewef'),
        ('sfwefewfwef', 'sfwefwef'),
        ('shewfwefwef', '你好'))
)
def test_stream_stream(grpc_server, grpc_resolver, messages):
    channel = RoundrobinChannel('grpcresolver.hello.Hello', grpc_resolver)
    stub = hello_pb2_grpc.HelloStub(channel)

    request = (hello_pb2.HelloRequest(message=message) for message in messages)
    response_iter = stub.StreamGreeterResponseStream(request)

    for response in response_iter:
        assert response.message


def test_roundrobin_channel(grpc_resolver, grpc_addr):
    channel = RoundrobinChannel('grpcresolver.hello.Hello', grpc_resolver)
    addr_num = len(grpc_addr)
    loop_count = 10
    for addr_idx in range(addr_num * loop_count):
        assert channel.get_channel().target == grpc_addr[addr_idx % addr_num]
