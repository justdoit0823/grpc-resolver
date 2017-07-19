
==============
grpc-resolver
==============

.. image:: https://travis-ci.org/justdoit0823/grpc-resolver.svg?branch=master
    :target: https://travis-ci.org/justdoit0823/grpc-resolver


A simple Python gRPC service registry and resolver, is compatible for Python 2 and 3.

Now it only supports Etcd as configuration storage backend, but actually can support any storage backend.


--------------
Requirements
--------------

- grpcio

- etcd3


---------
Install
---------

.. code-block:: shell

  $ pip install git+https://github.com/justdoit0823/grpc-resolver


-----------
How to use
-----------

Here we go:


Service Resolve
=================

.. code-block:: python

   >>> from grpcresolver import EtcdServiceResolver, RoundrobinChannel

   >>> import hello_pb2

   >>> import hello_pb2_grpc

   >>> resolver = EtcdServiceResolver(etcd_host='10.30.141.251', etcd_port=2376)

   >>> channel = RoundrobinChannel('your_service_name', resolver)

   >>> stub = hello_pb2_grpc.HelloStub(channel)

   >>> # do gRPC call as usual


Service Registry
==================

.. code-block:: python

   >>> from grpcresolver import EtcdServiceRegistry

   >>> registry = EtcdServiceResolver(etcd_host='10.30.33.11', etcd_port=2376)

   >>> registry.register(('your_grpc_service',), '192.168.10.20:11111', 360)


--------------
How it works
--------------

The ``grpcresolver.channel.LbChannel`` has implemented ``unary_unary``, ``unary_stream``, ``stream_unary`` and ``stream_stream`` four gRPC operations, but without ``subscribe`` and ``unsubscribe``.

Meanwhile, also implementing the relative multi-callable operations.

When calling gRPC service method, the channel will return a ``grpcresolver.channel.Channel`` object from it's local cache or create a new one after resolving from it's resolver.


---------
Features
---------


- Easy to integrate while using standard Python gRPC channel

- Supporting gRPC service discovery with random and roundrobin algorithms

- Automatic etcd node selection and retry when etcd client gRPC request failed
