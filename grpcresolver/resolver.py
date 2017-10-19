
"""Etcd resolver module."""

import abc
import six
import threading
import time

from grpcresolver.address import PlainAddress
from grpcresolver.client import EtcdClient


__all__ = ['EtcdServiceResolver']


class ServiceResolver(six.with_metaclass(abc.ABCMeta)):
    """gRPC service Resolver class."""

    @abc.abstractmethod
    def resolve(self, name):
        raise NotADirectoryError

    @abc.abstractmethod
    def update(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def listen(self):
        raise NotImplementedError


class EtcdServiceResolver(ServiceResolver):
    """gRPC service resolver based on Etcd."""

    def __init__(
            self, etcd_host=None, etcd_port=None, etcd_client=None,
            start_listener=True, listen_timeout=5, addr_cls=None):
        """Initialize etcd service resolver.

        :param etcd_host: (optional) etcd node host for :class:`client.EtcdClient`.
        :param etcd_port: (optional) etcd node port for :class:`client.EtcdClient`.
        :param etcd_client: (optional) A :class:`client.EtcdClient` object.
        :param start_listener: (optional) Indicate whether starting the resolver listen thread.
        :param listen_timeout: (optional) Resolver thread listen timeout.
        :param addr_cls: (optional) address format class.

        """
        self._listening = False
        self._stopped = False
        self._listen_thread = None
        self._listen_timeout = listen_timeout
        self._lock = threading.Lock()
        self._client = etcd_client if etcd_client else EtcdClient(
            etcd_host, etcd_port)
        self._names = {}
        self._addr_cls = addr_cls or PlainAddress

        if start_listener:
            self.start_listener()

    def resolve(self, name):
        """Resolve gRPC service name.

        :param name: gRPC service name.
        :rtype list: A collection gRPC server address.

        """
        with self._lock:
            try:
                return self._names[name]
            except KeyError:
                addrs = self.get(name)
                self._names[name] = addrs
                return addrs

    def get(self, name):
        """Get values from Etcd.

        :param name: Etcd key prefix name.
        :rtype list: A collection of Etcd values.

        """
        keys = self._client.get_prefix(name)
        vals = []
        plain = True
        if not isinstance(self._addr_cls, PlainAddress):
            plain = False

        for val, metadata in keys:
            if plain:
                vals.append(self._addr_cls.from_value(val))
            else:
                add, addr = self._addr_cls.from_value(val)
                if add:
                    vals.append(addr)

        return vals

    def update(self, **kwargs):
        """Add or delete service address.

        :param kwargs: Dictionary of ``'service_name': ((add-address, delete-address)).``

        """
        with self._lock:
            for name, (add, delete) in kwargs.items():
                try:
                    self._names[name].extend(add)
                except KeyError:
                    self._names[name] = add

                for del_item in delete:
                    try:
                        self._names[name].remove(del_item)
                    except ValueError:
                        continue

    def listen(self):
        """Listen for change about gRPC service address."""
        while not self._stopped:
            for name in self._names:
                try:
                    vals = self.get(name)
                except:
                    continue
                else:
                    with self._lock:
                        self._names[name] = vals

            time.sleep(self._listen_timeout)

    def start_listener(self, daemon=True):
        """Start listen thread.

        :param daemon: Indicate whether start thread as a daemon.

        """
        if self._listening:
            return

        thread_name = 'Thread-resolver-listener'
        self._listen_thread = threading.Thread(
            target=self.listen, name=thread_name)
        self._listen_thread.daemon = daemon
        self._listen_thread.start()
        self._listening = True

    def stop(self):
        """Stop service resolver."""
        if self._stopped:
            return

        self._stopped = True

    def __del__(self):
        self.stop()
