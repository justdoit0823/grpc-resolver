
import hello_pb2
import hello_pb2_grpc


__all__ = ['HelloGRpcServer']


class HelloGRpcServer(hello_pb2_grpc.HelloServicer):

    def Greeter(self, request, context):
        reply_msg = 'Hi, ' + request.message
        return hello_pb2.HelloResponse(message=reply_msg)

    def GreeterResponseStream(self, request, context):
        for msg in request.message:
            reply_msg = 'Hi, ' + msg
            yield hello_pb2.HelloResponse(message=reply_msg)

    def StreamGreeter(self, request_iterator, context):
        reply_msg = 'Hi, ' + ''.join(
            request.message for request in request_iterator)
        return hello_pb2.HelloResponse(message=reply_msg)

    def StreamGreeterResponseStream(self, request_iterator, context):
        for request in request_iterator:
            reply_msg = 'Hi, ' + request.message
            yield hello_pb2.HelloResponse(message=reply_msg)
