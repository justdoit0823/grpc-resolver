
"""gRPC load balance channel module."""

import random

import grpc
from grpc import _common
from grpc._channel import (
    _UNARY_UNARY_INITIAL_DUE, _UNARY_STREAM_INITIAL_DUE,
    _STREAM_UNARY_INITIAL_DUE, _STREAM_STREAM_INITIAL_DUE, _EMPTY_FLAGS)
from grpc._channel import (
    _RPCState, _ChannelCallState, _ChannelConnectivityState, _Rendezvous)
from grpc._channel import (
    _start_unary_request, _end_unary_response_blocking,
    _consume_request_iterator, _channel_managed_call_management,
    _call_error_set_RPCstate, _handle_event, _event_handler,
    _check_call_error, _options, _deadline)
from grpc._cython import cygrpc


__all__ = ['RandomChannel', 'RoundrobinChannel']


class _UnaryUnaryMultiCallable(grpc.UnaryUnaryMultiCallable):

    def __init__(
            self, channel, method, request_serializer,
            response_deserializer):
        self._channel = channel
        self._method = method
        self._request_serializer = request_serializer
        self._response_deserializer = response_deserializer

    def _prepare(self, request, timeout, metadata):
        deadline, deadline_timespec, serialized_request, rendezvous = (
            _start_unary_request(request, timeout, self._request_serializer))
        if serialized_request is None:
            return None, None, None, None, rendezvous
        else:
            state = _RPCState(_UNARY_UNARY_INITIAL_DUE, None, None, None, None)
            operations = (
                cygrpc.operation_send_initial_metadata(
                    _common.to_cygrpc_metadata(metadata), _EMPTY_FLAGS),
                cygrpc.operation_send_message(serialized_request, _EMPTY_FLAGS),
                cygrpc.operation_send_close_from_client(_EMPTY_FLAGS),
                cygrpc.operation_receive_initial_metadata(_EMPTY_FLAGS),
                cygrpc.operation_receive_message(_EMPTY_FLAGS),
                cygrpc.operation_receive_status_on_client(_EMPTY_FLAGS),)
            return state, operations, deadline, deadline_timespec, None

    def _blocking(self, channel, request, timeout, metadata, credentials):
        state, operations, deadline, deadline_timespec, rendezvous = self._prepare(
            request, timeout, metadata)
        if rendezvous:
            raise rendezvous
        else:
            completion_queue = cygrpc.CompletionQueue()
            call = channel.create_call(None, 0, completion_queue,
                                       self._method, None,
                                       deadline_timespec)
            if credentials is not None:
                call.set_credentials(credentials._credentials)
            call_error = call.start_client_batch(
                cygrpc.Operations(operations), None)
            _check_call_error(call_error, metadata)
            _handle_event(completion_queue.poll(), state,
                          self._response_deserializer)
            return state, call, deadline

    def __call__(self, request, timeout=None, metadata=None, credentials=None):
        channel = self._channel.get_channel()
        state, call, deadline = self._blocking(
            channel.channel, request, timeout, metadata, credentials)
        return _end_unary_response_blocking(state, call, False, deadline)

    def with_call(self, request, timeout=None, metadata=None, credentials=None):
        channel = self._channel.get_channel()
        state, call, deadline = self._blocking(
            channel.channel, request, timeout, metadata, credentials)
        return _end_unary_response_blocking(state, call, True, deadline)

    def future(self, request, timeout=None, metadata=None, credentials=None):
        state, operations, deadline, deadline_timespec, rendezvous = self._prepare(
            request, timeout, metadata)
        if rendezvous:
            return rendezvous
        else:
            channel = self._channel.get_channel()
            call, drive_call = channel.managed_call(
                None, 0, self._method, None, deadline_timespec)

            if credentials is not None:
                call.set_credentials(credentials._credentials)

            event_handler = _event_handler(
                state, call, self._response_deserializer)
            with state.condition:
                call_error = call.start_client_batch(
                    cygrpc.Operations(operations), event_handler)
                if call_error != cygrpc.CallError.ok:
                    _call_error_set_RPCstate(state, call_error, metadata)
                    return _Rendezvous(state, None, None, deadline)
                drive_call()
            return _Rendezvous(state, call, self._response_deserializer,
                               deadline)


class _UnaryStreamMultiCallable(grpc.UnaryStreamMultiCallable):

    def __init__(self, channel, method, request_serializer,
                 response_deserializer):
        self._channel = channel
        self._method = method
        self._request_serializer = request_serializer
        self._response_deserializer = response_deserializer

    def __call__(self, request, timeout=None, metadata=None, credentials=None):
        deadline, deadline_timespec, serialized_request, rendezvous = (
            _start_unary_request(request, timeout, self._request_serializer))
        if serialized_request is None:
            raise rendezvous
        else:
            state = _RPCState(_UNARY_STREAM_INITIAL_DUE, None, None, None, None)
            channel = self._channel.get_channel()
            call, drive_call = channel.managed_call(
                None, 0, self._method, None, deadline_timespec)
            if credentials is not None:
                call.set_credentials(credentials._credentials)
            event_handler = _event_handler(
                state, call, self._response_deserializer)
            with state.condition:
                call.start_client_batch(
                    cygrpc.Operations((
                        cygrpc.operation_receive_initial_metadata(_EMPTY_FLAGS),
                    )), event_handler)
                operations = (
                    cygrpc.operation_send_initial_metadata(
                        _common.to_cygrpc_metadata(metadata),
                        _EMPTY_FLAGS), cygrpc.operation_send_message(
                            serialized_request, _EMPTY_FLAGS),
                    cygrpc.operation_send_close_from_client(_EMPTY_FLAGS),
                    cygrpc.operation_receive_status_on_client(_EMPTY_FLAGS),)
                call_error = call.start_client_batch(
                    cygrpc.Operations(operations), event_handler)
                if call_error != cygrpc.CallError.ok:
                    _call_error_set_RPCstate(state, call_error, metadata)
                    return _Rendezvous(state, None, None, deadline)
                drive_call()
            return _Rendezvous(state, call, self._response_deserializer,
                               deadline)


class _StreamUnaryMultiCallable(grpc.StreamUnaryMultiCallable):

    def __init__(
            self, channel, method, request_serializer, response_deserializer):
        self._channel = channel
        self._method = method
        self._request_serializer = request_serializer
        self._response_deserializer = response_deserializer

    def _blocking(
            self, channel, request_iterator, timeout, metadata, credentials):
        deadline, deadline_timespec = _deadline(timeout)
        state = _RPCState(_STREAM_UNARY_INITIAL_DUE, None, None, None, None)
        completion_queue = cygrpc.CompletionQueue()
        call = channel.create_call(
            None, 0, completion_queue, self._method, None, deadline_timespec)
        if credentials is not None:
            call.set_credentials(credentials._credentials)
        with state.condition:
            call.start_client_batch(
                cygrpc.Operations(
                    (cygrpc.operation_receive_initial_metadata(_EMPTY_FLAGS),)),
                None)
            operations = (
                cygrpc.operation_send_initial_metadata(
                    _common.to_cygrpc_metadata(metadata), _EMPTY_FLAGS),
                cygrpc.operation_receive_message(_EMPTY_FLAGS),
                cygrpc.operation_receive_status_on_client(_EMPTY_FLAGS),)
            call_error = call.start_client_batch(
                cygrpc.Operations(operations), None)
            _check_call_error(call_error, metadata)
            _consume_request_iterator(request_iterator, state, call,
                                      self._request_serializer)
        while True:
            event = completion_queue.poll()
            with state.condition:
                _handle_event(event, state, self._response_deserializer)
                state.condition.notify_all()
                if not state.due:
                    break
        return state, call, deadline

    def __call__(
            self, request_iterator, timeout=None, metadata=None,
            credentials=None):
        channel = self._channel.get_channel()
        state, call, deadline = self._blocking(
            channel.channel, request_iterator, timeout, metadata,
            credentials)
        return _end_unary_response_blocking(state, call, False, deadline)

    def with_call(
            self, request_iterator, timeout=None, metadata=None,
            credentials=None):
        channel = self._channel.get_channel()
        state, call, deadline = self._blocking(
            channel.channel, request_iterator, timeout, metadata, credentials)
        return _end_unary_response_blocking(state, call, True, deadline)

    def future(self,
               request_iterator,
               timeout=None,
               metadata=None,
               credentials=None):
        deadline, deadline_timespec = _deadline(timeout)
        state = _RPCState(_STREAM_UNARY_INITIAL_DUE, None, None, None, None)
        channel = self._channel.get_channel()
        call, drive_call = channel.managed_call(
            None, 0, self._method, None, deadline_timespec)
        if credentials is not None:
            call.set_credentials(credentials._credentials)
        event_handler = _event_handler(state, call, self._response_deserializer)
        with state.condition:
            call.start_client_batch(
                cygrpc.Operations(
                    (cygrpc.operation_receive_initial_metadata(_EMPTY_FLAGS),)),
                event_handler)
            operations = (
                cygrpc.operation_send_initial_metadata(
                    _common.to_cygrpc_metadata(metadata), _EMPTY_FLAGS),
                cygrpc.operation_receive_message(_EMPTY_FLAGS),
                cygrpc.operation_receive_status_on_client(_EMPTY_FLAGS),)
            call_error = call.start_client_batch(
                cygrpc.Operations(operations), event_handler)
            if call_error != cygrpc.CallError.ok:
                _call_error_set_RPCstate(state, call_error, metadata)
                return _Rendezvous(state, None, None, deadline)
            drive_call()
            _consume_request_iterator(request_iterator, state, call,
                                      self._request_serializer)
        return _Rendezvous(state, call, self._response_deserializer, deadline)


class _StreamStreamMultiCallable(grpc.StreamStreamMultiCallable):

    def __init__(
            self, channel, method, request_serializer, response_deserializer):
        self._channel = channel
        self._method = method
        self._request_serializer = request_serializer
        self._response_deserializer = response_deserializer

    def __call__(self,
                 request_iterator,
                 timeout=None,
                 metadata=None,
                 credentials=None):
        deadline, deadline_timespec = _deadline(timeout)
        state = _RPCState(_STREAM_STREAM_INITIAL_DUE, None, None, None, None)
        channel = self._channel.get_channel()
        call, drive_call = channel.managed_call(
            None, 0, self._method, None, deadline_timespec)
        if credentials is not None:
            call.set_credentials(credentials._credentials)
        event_handler = _event_handler(state, call, self._response_deserializer)
        with state.condition:
            call.start_client_batch(
                cygrpc.Operations(
                    (cygrpc.operation_receive_initial_metadata(_EMPTY_FLAGS),)),
                event_handler)
            operations = (
                cygrpc.operation_send_initial_metadata(
                    _common.to_cygrpc_metadata(metadata), _EMPTY_FLAGS),
                cygrpc.operation_receive_status_on_client(_EMPTY_FLAGS),)
            call_error = call.start_client_batch(
                cygrpc.Operations(operations), event_handler)
            if call_error != cygrpc.CallError.ok:
                _call_error_set_RPCstate(state, call_error, metadata)
                return _Rendezvous(state, None, None, deadline)
            drive_call()
            _consume_request_iterator(request_iterator, state, call,
                                      self._request_serializer)
        return _Rendezvous(state, call, self._response_deserializer, deadline)


class Channel(object):
    """An object communicates between `LbChannel` and gRPC request."""

    __slots__ = ('target', 'channel', 'managed_call', 'connectivity_state')

    def __init__(self, target, options=None, credentials=None):
        options = options if options is not None else ()
        self.target = target
        self.channel = channel = cygrpc.Channel(
            _common.encode(target),
            _common.channel_args(_options(options)), credentials)
        self.managed_call = _channel_managed_call_management(
            _ChannelCallState(channel))
        self.connectivity_state = _ChannelConnectivityState(channel)


class LbChannel(grpc.Channel):
    """A gRPC load balance channel."""

    def __init__(self, service_name, resolver):
        self._service_name = service_name
        self._resolver = resolver
        self._channels = {}

    def select_target(self):
        raise NotImplementedError

    def get_channel(self):
        addr = self.select_target()
        try:
            return self._channels[addr]
        except KeyError:
            channel = Channel(addr)
            self._channels[addr] = channel
            return channel

    def release_channel(self, channel):
        name = self._service_name
        items = {name: ((), (channel.target,))}
        self._resolver.update(**items)

    def subscribe(self, callback, try_to_connect=False):
        raise NotImplementedError

    def unsubscribe(self, callback):
        raise NotImplementedError

    def unary_unary(
            self, method, request_serializer=None, response_deserializer=None):
        return _UnaryUnaryMultiCallable(
            self, _common.encode(method), request_serializer,
            response_deserializer)

    def unary_stream(
            self, method, request_serializer=None, response_deserializer=None):
        return _UnaryStreamMultiCallable(
            self, _common.encode(method), request_serializer,
            response_deserializer)

    def stream_unary(
            self, method, request_serializer=None, response_deserializer=None):
        return _StreamUnaryMultiCallable(
            self, _common.encode(method), request_serializer,
            response_deserializer)

    def stream_stream(
            self, method, request_serializer=None, response_deserializer=None):
        return _StreamStreamMultiCallable(
            self, _common.encode(method), request_serializer,
            response_deserializer)

    def __del__(self):
        del self._resolver


class RandomChannel(LbChannel):
    """Random gRPC load balance channel."""

    def select_target(self):
        addrs = self._resolver.resolve(self._service_name)
        addr_idx = random.randint(0, len(addrs) - 1)
        addr = addrs[addr_idx]

        return addr


class RoundrobinChannel(LbChannel):
    """Roundrobin gRPC load balance channel."""

    def __init__(self, service_name, resolver):
        super(RoundrobinChannel, self).__init__(service_name, resolver)
        self._cur_index = 0

    def select_target(self):
        addrs = self._resolver.resolve(self._service_name)
        addr_num = len(addrs)
        if addr_num == 0:
            raise ValueError('No channel.')

        addr = addrs[self._cur_index % addr_num]
        self._cur_index = (self._cur_index + 1) % addr_num

        return addr
