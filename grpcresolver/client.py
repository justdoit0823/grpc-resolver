
"""Etcd client proxy module."""

import etcd3
import grpc
from grpc._channel import _Rendezvous


__all__ = ['EtcdClient']


class EtcdClient(object):
    """A etcd client proxy."""

    _suffer_status_code = (
        grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.ABORTED,
        grpc.StatusCode.RESOURCE_EXHAUSTED)

    def __init__(self, etcd_host, etcd_port):
        self._host = etcd_host
        self._port = etcd_port
        self._client_idx = 0
        self._cluster = None

    def call(self, method, *args, **kwargs):
        """Etcd operation gateway method."""
        if self._cluster is None:
            # Lazy initialize etcd client
            client = etcd3.client(self._host, self._port)
            self._cluster = tuple(
                member._etcd_client for member in client.members)

        try_count = len(self._cluster)
        while try_count > 0:
            client = self._cluster[self._client_idx]
            try:
                ret = getattr(client, method)(*args, **kwargs)
            except _Rendezvous as e:
                if e.code() in self._suffer_status_code:
                    self._client_idx = (self._client_idx + 1) % self._cluster
                    try_count -= 1
                    continue

                raise

            return ret

    def get(self, key):
        return self.call('get', key)

    def get_all(self):
        return self.call('get_all')

    def get_prefix(self, key_prefix, sort_order=None, sort_target='key'):
        return self.call(
            'get_prefix', key_prefix, sort_order=sort_order,
            sort_target=sort_target)

    def put(self, key, value, lease=None):
        return self.call('put', key, value, lease=lease)

    def lease(self, ttl, lease_id=None):
        return self.call('lease', ttl, lease_id=lease_id)

    def delete(self, key):
        return self.call('delete', key)
