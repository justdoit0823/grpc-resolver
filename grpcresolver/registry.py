
"""gRPC service registry module."""

import abc
import six

from grpcresolver.address import PlainAddress
from grpcresolver.client import EtcdClient


__all__ = ['EtcdServiceRegistry']


class ServiceRegistry(six.with_metaclass(abc.ABCMeta)):
    """A service registry."""

    @abc.abstractmethod
    def register(self, service_names, service_addr, service_ttl):
        """Register services with the same address."""
        raise NotImplementedError

    @abc.abstractmethod
    def heartbeat(self, service_addr=None):
        """Service registry heartbeat."""
        raise NotImplementedError

    @abc.abstractmethod
    def unregister(self, service_names, service_addr):
        """Unregister services with the same address."""
        raise NotImplementedError


class EtcdServiceRegistry(ServiceRegistry):
    """gRPC service registry based on etcd."""

    def __init__(self, etcd_host=None, etcd_port=None, etcd_client=None):
        """Initialize etcd service registry.

        :param etcd_host: (optional) etcd node host for :class:`client.EtcdClient`.
        :param etcd_port: (optional) etcd node port for :class:`client.EtcdClient`.
        :param etcd_client: (optional) A :class:`client.EtcdClient` object.

        """
        self._client = etcd_client if etcd_client else EtcdClient(
            etcd_host, etcd_port)
        self._leases = {}
        self._services = {}

    def get_lease(self, service_addr, service_ttl):
        """Get a gRPC service lease from etcd.

        :param service_addr: gRPC service address.
        :param service_ttl: gRPC service lease ttl(seconds).
        :rtype `etcd3.lease.Lease`

        """
        lease = self._leases.get(service_addr)
        if lease and lease.remaining_ttl > 0:
            return lease

        lease_id = hash(service_addr)
        lease = self._client.lease(service_ttl, lease_id)
        self._leases[service_addr] = lease
        return lease

    def _form_service_key(self, service_name, service_addr):
        """Return service's key in etcd."""
        return '/'.join((service_name, service_addr))

    def register(self, service_names, service_addr, service_ttl, addr_cls=None):
        """Register gRPC services with the same address.

        :param service_names: A collection of gRPC service name.
        :param service_addr: gRPC server address.
        :param service_ttl: gRPC service ttl(seconds).
        :param addr_cls: format class of gRPC service address.

        """
        lease = self.get_lease(service_addr, service_ttl)
        addr_cls = addr_cls or PlainAddress
        for service_name in service_names:
            key = self._form_service_key(service_name, service_addr)
            addr_val = addr_cls(service_addr).add_value()
            self._client.put(key, addr_val, lease=lease)
            try:
                self._services[service_addr].add(service_name)
            except KeyError:
                self._services[service_addr] = {service_name}

    def heartbeat(self, service_addr=None):
        """gRPC service heartbeat."""
        if service_addr:
            lease = self.get_lease(service_addr)
            leases = ((service_addr, lease),)
        else:
            leases = tuple(self._leases.items())

        for service_addr, lease in leases:
            ret = lease.refresh()[0]
            if ret.TTL == 0:
                self.register(
                    self._services[service_addr], service_addr, lease.ttl)

    def unregister(self, service_names, service_addr, addr_cls=None):
        """Unregister gRPC services with the same address.

        :param service_names: A collection of gRPC service name.
        :param service_addr: gRPC server address.

        """
        addr_cls = addr_cls or PlainAddress
        etcd_delete = True
        if addr_cls != PlainAddress:
            etcd_delete = False

        for service_name in service_names:
            key = self._form_service_key(service_name, service_addr)
            if etcd_delete:
                self._client.delete(key)
            else:
                self._client.put(addr_cls(service_addr).delete_value())

            self._services.get(service_addr, {}).discard(service_name)
