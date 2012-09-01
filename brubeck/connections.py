import ujson as json
from uuid import uuid4
import cgi
import re
import logging
import Cookie

from request import to_bytes, to_unicode, parse_netstring, Request, Response
from request_handling import brubeck_response
from request_handling import http_response
from request_handling import MESSAGE_TYPES
from request_handling import coro_spawn

###
### Connection Classes
###

class Connection(object):
    """This class is an abstraction for how Brubeck sends and receives
    messages. The idea is that Brubeck waits to receive messages for some work
    and then it responds. Therefore each connection should essentially be a
    mechanism for reading a message and a mechanism for responding, if a
    response is necessary.
    """

    def __init__(self, incoming=None, outgoing=None):
        """The base `__init__()` function configures a unique ID and assigns
        the incoming and outgoing mechanisms to a name.

        `in_sock` and `out_sock` feel like misnomers at this time but they are
        preserved for a transition period.
        """
        self.sender_id = uuid4().hex
        self.in_sock = incoming
        self.out_sock = outgoing

    def _unsupported(self, name):
        """Simple function that raises an exception.
        """
        error_msg = 'Subclass of Connection has not implemented `%s()`' % name
        raise NotImplementedError(error_msg)


    def recv(self):
        """Receives a raw mongrel2.handler.Request object that you
        can then work with.
        """
        self._unsupported('recv')

    def _recv_forever_ever(self, fun_forever):
        """Calls a handler function that runs forever. The handler can be
        interrupted with a ctrl-c, though.
        """
        try:
            fun_forever()
        except KeyboardInterrupt, ki:
            # Put a newline after ^C
            print '\nBrubeck going down...'

    def send(self, uuid, conn_id, msg):
        """Function for sending a single message.
        """
        self._unsupported('send')
 
    def reply(self, req, msg):
        """Does a reply based on the given Request object and message.
        """
        self.send(req.sender, req.conn_id, msg)

    def reply_bulk(self, uuid, idents, data):
        """This lets you send a single message to many currently
        connected clients.  There's a MAX_IDENTS that you should
        not exceed, so chunk your targets as needed.  Each target
        will receive the message once by Mongrel2, but you don't have
        to loop which cuts down on reply volume.
        """
        self._unsupported('reply_bulk')
        self.send(uuid, ' '.join(idents), data)

    def close(self):
        """Close the connection.
        """
        self._unsupported('close')

    def close_bulk(self, uuid, idents):
        """Same as close but does it to a whole bunch of idents at a time.
        """
        self._unsupported('close_bulk')
        self.reply_bulk(uuid, idents, "")


###
### ZeroMQ
###

def load_zmq():
    """This function exists to determine where zmq should come from and then
    cache that decision at the module level.
    """
    if not hasattr(load_zmq, '_zmq'):
        from request_handling import CORO_LIBRARY
        if CORO_LIBRARY == 'gevent':
            from gevent_zeromq import zmq
        elif CORO_LIBRARY == 'eventlet':
            from eventlet.green import zmq
        load_zmq._zmq = zmq

    return load_zmq._zmq


def load_zmq_ctx():
    """This function exists to contain the namespace requirements of generating
    a zeromq context, while keeping the context at the module level. If other
    parts of the system need zeromq, they should use this function for access
    to the existing context.
    """
    if not hasattr(load_zmq_ctx, '_zmq_ctx'):
        zmq = load_zmq()
        zmq_ctx = zmq.Context()
        load_zmq_ctx._zmq_ctx = zmq_ctx

    return load_zmq_ctx._zmq_ctx


###
### ZMQ Connection 
###

class ZMQConnection(Connection):
    """This is an abstraction of a ZMQ connection
    and needs to be extended based on the message format.
    """
    MAX_IDENTS = 100

    def __init__(self, pull_addr, pub_addr):
        """sender_id = uuid.uuid4() or anything unique
        pull_addr = pull socket used for incoming messages
        pub_addr = publish socket used for outgoing messages

        The class encapsulates socket type by referring to it's pull socket
        as in_sock and it's publish socket as out_sock.
        """
        zmq = load_zmq()
        ctx = load_zmq_ctx()

        in_sock = ctx.socket(zmq.PULL)
        out_sock = ctx.socket(zmq.PUB)

        super(ZMQConnection, self).__init__(in_sock, out_sock)
        self.in_addr = pull_addr
        self.out_addr = pub_addr

        in_sock.connect(pull_addr)
        out_sock.setsockopt(zmq.IDENTITY, self.sender_id)
        out_sock.connect(pub_addr)

    def recv(self):
        """Receives a raw mongrel2.handler.Request object that you from the
        zeromq socket and return whatever is found.
        """
        zmq_msg = self.in_sock.recv()
        return zmq_msg

    def recv_forever_ever(self, application):
        """Defines a function that will run the primary connection Brubeck uses
        for incoming jobs. This function should then call super which runs the
        function in a try-except that can be ctrl-c'd.
        """
        def fun_forever():
            while True:
                request = self.recv()
                self.process_message(application, request)
        self._recv_forever_ever(fun_forever)

    def reply(self, req, msg):
        """Does a reply based on the given Request object and message.
        """
        self.send(req.sender, req.conn_id, msg)

    def process_message(self, application, message):
        """It is up to the implementation to process the message.
        """
        self._unsupported('process_message')

    def send(self, uuid, conn_id, msg):
        """Raw send to the given connection ID at the given uuid, mostly used
        internally. It is up to the implementation to format the message to send.
        """
        self._unsupported('send')

    def close(self):
        """close a connection
        """
        self._unsupported('close')
        pass


###
### Mongrel2
###
# this is just for testing, should be in class
def mongrel2_process_message(application, message):
    """This coroutine looks at the message, determines which handler will
    be used to process it, and then begins processing.
    
    The application is responsible for handling misconfigured routes.
    """
    request = Request.parse_msg(message)
    if request.is_disconnect():
        return  # Ignore disconnect msgs. Dont have areason to do otherwise
    handler = application.route_message(request)
    result = handler()

    http_content = http_response(result['body'], result['status_code'],
                                 result['status_msg'], result['headers'])

    application.msg_conn.reply(request, http_content)

class Mongrel2Connection(ZMQConnection):
    """This class is specific to handling messages from Mongrel2.
    """

    def process_message(self, application, message):
        #without coroutine
        mongrel2_process_message(application, message)

        # with coroutine
        #coro_spawn(mongrel2_process_message, application, message)

    def send(self, uuid, conn_id, msg):
        """Raw send to the given connection ID at the given uuid, mostly used
        internally.
        """
        header = "%s %d:%s," % (uuid, len(str(conn_id)), str(conn_id))
        msg = header + ' ' + to_bytes(msg)
        self.out_sock.send(msg)

    def close(self):
        """Tells mongrel2 to explicitly close the HTTP connection.
        """
        pass

    def reply_bulk(self, uuid, idents, data):
        """This lets you send a single message to many currently
        connected clients.  There's a MAX_IDENTS that you should
        not exceed, so chunk your targets as needed.  Each target
        will receive the message once by Mongrel2, but you don't have
        to loop which cuts down on reply volume.
        """
        self.send(uuid, ' '.join(idents), data)


    def close_bulk(self, uuid, idents):
        """Same as close but does it to a whole bunch of idents at a time.
        """
        self.reply_bulk(uuid, idents, "")

###
### WSGI 
###

class WSGIConnection(Connection):
    """
    """

    def __init__(self, port=6767):
        super(WSGIConnection, self).__init__()
        self.port = port

    def process_message(self, application, environ, callback):
        request = Request.parse_wsgi_request(environ)
        handler = application.route_message(request)
        result = handler()

        wsgi_status = ' '.join([str(result['status_code']), result['status_msg']])
        headers = [(k, v) for k,v in result['headers'].items()]
        callback(str(wsgi_status), headers)

        return [result['body']]

    def recv_forever_ever(self, application):
        """Defines a function that will run the primary connection Brubeck uses
        for incoming jobs. This function should then call super which runs the
        function in a try-except that can be ctrl-c'd.
        """
        def fun_forever():
            from brubeck.request_handling import CORO_LIBRARY
            print "Serving on port %s..." % (self.port)

            def proc_msg(environ, callback):
                return self.process_message(application, environ, callback)

            if CORO_LIBRARY == 'gevent':
                from gevent import wsgi
                server = wsgi.WSGIServer(('', self.port), proc_msg)
                server.serve_forever()

            elif CORO_LIBRARY == 'eventlet':
                import eventlet
                server = eventlet.wsgi.server(eventlet.listen(('', self.port)),
                                              proc_msg)

        self._recv_forever_ever(fun_forever)


###
### Brubeck service connections (service and client)
###

# this is just for testing, should be in class
def service_process_message(application, message):
    """This coroutine looks at the message, determines which handler will
    be used to process it, and then begins processing.
    
    The application is responsible for handling misconfigured routes.
    """

    request = Request.parse_brubeck_request(message, application.msg_conn.passphrase)
    if request.is_disconnect():
        return  # Ignore disconnect msgs. Dont have areason to do otherwise
    request.message_type = MESSAGE_TYPES[application.msg_conn._BRUBECK_MESSAGE_TYPE]

    handler = application.route_message(request)
    result = handler()

    brubeck_content = brubeck_response(result['body'], result['status_code'],
                                 result['status_msg'], result['headers'])
    msg = ""
    if result is not None and result is not "":
        msg = json.dumps(result)
    application.msg_conn.send(request.sender, request.conn_id,
        request.origin_uuid, request.origin_conn_id,
        request.origin_out_addr, msg, request.path)

class ServiceConnection(ZMQConnection):
    """This class is specific to handling communication with a ServiceClientConnection.
    """
    _BRUBECK_MESSAGE_TYPE = 1
    
    def __init__(self, svc_addr, passphrase):
        """sender_id = uuid.uuid4() or anything unique
        pull_addr = pull socket used for incoming messages
        pub_addr = publish socket used for outgoing messages

        The class encapsulates socket type by referring to it's pull socket
        as in_sock and it's publish socket as out_sock.
        """
        zmq = load_zmq()
        ctx = load_zmq_ctx()
        # yes, in and out are the same
        # the response (out_sock) is routed to the original client
        in_sock = ctx.socket(zmq.ROUTER)
        out_sock = in_sock

        in_sock.bind(svc_addr)
        in_sock.connect(svc_addr)

        super(ZMQConnection, self).__init__(in_sock, out_sock)

        self.in_addr = svc_addr
        self.out_addr = svc_addr

        self.zmq = zmq
        self.passphrase = passphrase

        #out_sock.setsockopt(zmq.IDENTITY, self.sender_id)

        #in_sock.connect(pull_addr)

    def process_message(self, application, message):
        #without coroutine
        service_process_message(application, message)

        # with coroutine
        #coro_spawn(service_process_message, application, message)

    def send(self, sender_uuid, conn_id, 
        origin_sender_id, origin_conn_id,
        origin_out_addr, msg, path):
        """uuid = unique ID that both the client and server need to match
           conn_id = connection id from this request needed to wake up handler on response
           origin_uuid = unique ID from the original request
           origin_conn_id = the connection id from the original request
           origin_out_addr = the socket address that expects the final result
           msg = the payload (a JSON object)
           path = the path used to route to the proper response handler
        """

        header = "%s %d:%s %d:%s %d:%s %d:%s %d:%s %d:%s" % ( sender_uuid,
            len(str(conn_id)), str(conn_id),
            len(self.passphrase), self.passphrase,
            len(origin_sender_id), origin_sender_id,
            len(str(origin_conn_id)), str(origin_conn_id),
            len(origin_out_addr), origin_out_addr,
            len(path), path
        )
        msg = '%s %d:%s' % (header, len(msg), to_bytes(msg))

        self.out_sock.send(sender_uuid, self.zmq.SNDMORE)
        self.out_sock.send("", self.zmq.SNDMORE)
        self.out_sock.send(msg)
        return
        
    def recv(self):
        """Receives a message from a ServiceClientConnection.
        """

        # blocking recv call
        zmq_msg = self.in_sock.recv()
        # if we are multipart, keep getting our message until we are done
        while self.in_sock.getsockopt(self.zmq.RCVMORE):
            zmq_msg += self.in_sock.recv()

        return zmq_msg


# this is just for testing, should be in class
def service_client_process_message(application, message, passphrase, service_client, service_addr, handle=True):
    """This coroutine looks at the message, determines which handler will
    be used to process it, and then begins processing.
    Since this is a reply, not a request,
    we simply call the handler and are done
    returns a tuple containing 1) the response object created 
        from parsing the message and 2) the handlers return value
    """
    response = Response.parse_brubeck_response(message, passphrase)
    
    if handle:
        handler = application.route_message(response)
        handler.set_status(response.status_code,  response.status_msg)
        result = handler()
        service_client._notify(service_addr, response, result)

class ServiceClientConnection(ServiceConnection):
    """This class is specific to communicating with a ServiceConnection.
    """

    def __init__(self, svc_addr, passphrase, async=False):
        """ passphrase = unique ID that both the client and server need to match
                for security purposed

            svc_addr = address of the Brubeck Service we are connecting to
            This socket is used for both inbound and outbound messages

            async = should the message be async or not
                If async is True then send() returns the response and
                the same client is guaranteed to receive the response
    
                If async is False then send() returns immediately and a
                DEALER socket is used, meaning the response may be handled 
                by any connected clients using fair-queuing
                
                The default is to make a syncronouse service request which
                enables you to offload processor intensive actions 
                so the initial brubeck instance is as non-blocking as possible
                for the request.
        """

        self.passphrase = passphrase
        self.sender_id = str(uuid4())
        
        self.async = async

        zmq = load_zmq()
        ctx = load_zmq_ctx()

        if async:
            in_sock = ctx.socket(zmq.DEALER)
        else:
            in_sock = ctx.socket(zmq.REQ)
        out_sock = in_sock

        out_sock.setsockopt(zmq.IDENTITY, self.sender_id)
        out_sock.connect(svc_addr)

        super(ZMQConnection, self).__init__(in_sock, out_sock)

        self.in_addr = svc_addr
        self.out_addr = svc_addr

        self.zmq = zmq

    def process_message(self, application, message, service_client, service_addr):
        #without coroutine
        service_client_process_message(application, message, self.passphrase, service_client, service_addr)

        # with coroutine
        #coro_spawn(service_client_process_message, application, message, self.passphrase, service_client, service_addr)

    def send(self, service_req):
        """Send will wait for a response and return it if async is False
        """
        service_req.conn_id = str(uuid4())


        header = "%d:%s %d:%s %d:%s %d:%s %d:%s %d:%s" % (
            len(str(service_req.conn_id)), str(service_req.conn_id),
            len(str(self.passphrase)), str(self.passphrase),
            len(service_req.origin_sender_id),service_req.origin_sender_id,
            len(str(service_req.origin_conn_id)), str(service_req.origin_conn_id),
            len(service_req.origin_out_addr), service_req.origin_out_addr,
            len(service_req.path), service_req.path,
        )

        msg = ' %s %d:%s%d:%s' % (header, len(service_req.headers), service_req.headers, len(service_req.body), to_bytes(service_req.body))
        #logging.debug("ServiceClientConnection send: %s" % msg)
        self.out_sock.send(msg)

        return service_req
        
        #if self.async:
        #    return

        #return self.recv()
