import logging
import os
import sys
import ujson as json

from connections import (
    load_zmq,
    load_zmq_ctx,
    Mongrel2Connection,
    ZMQConnection,
)
from request import Request, to_bytes, to_unicode
from request_handling import (
    MessageHandler,
    http_response,
    MESSAGE_TYPES,
    coro_spawn
)
from dictshield.document import Document
from dictshield.fields import (StringField,
                               BooleanField,
                               URLField,
                               EmailField,
                               LongField,
                               DictField,
                               IntField)
from uuid import uuid4
from brubeck.request_handling import parse_msgstring


_DEFAULT_SERVICE_REQUEST_METHOD = 'request'
_DEFAULT_SERVICE_RESPONSE_METHOD = 'response'


#################################
## Request and Response stuff 
#################################

def parse_service_request(msg, passphrase):
    """Static method for constructing a Request instance out of a
    message read straight off a zmq socket from a ServiceClientConnection.
    """
    #logging.debug("parse_service_request: %s" % msg)
    sender, conn_id, msg_passphrase, origin_sender_id, origin_conn_id, origin_out_addr, path, rest = msg.split(' ', 7)

    conn_id = parse_msgstring(conn_id)[0]
    msg_passphrase = parse_msgstring(msg_passphrase)[0]
    origin_sender_id = parse_msgstring(origin_sender_id)[0]
    origin_conn_id = parse_msgstring(origin_conn_id)[0]
    origin_out_addr = parse_msgstring(origin_out_addr)[0]
    path = parse_msgstring(path)[0]
    
    if msg_passphrase != passphrase:
        raise Exception('Unknown service identity! (%s != %s)' % (str(msg_passphrase),str(passphrase)))

    headers, body = parse_msgstring(rest)
    body = parse_msgstring(body)[0]

    headers = json.loads(headers)
    body = json.loads(body)

    r = ServiceRequest(**{
            "sender": sender,
            "conn_id": conn_id,
            "origin_sender_id": sender,
            "origin_conn_id": origin_conn_id,
            "origin_out_addr": origin_out_addr,
            "path": path,
            "headers": headers,
            "body": body,
    })

    return r

def create_service_response(service_request, handler, msg={}, headers={"METHOD": _DEFAULT_SERVICE_RESPONSE_METHOD}):

    if not isinstance(headers, dict):
        headers = json.loads(headers)
    if not isinstance(msg, dict):
        msg = json.loads(msg)

    service_response = ServiceResponse(**{
        "sender": service_request.sender,
        "conn_id": service_request.conn_id,
        "origin_sender_id": service_request.origin_sender_id,
        "origin_conn_id": service_request.origin_conn_id,
        "origin_out_addr": service_request.origin_out_addr,
        "path": service_request.path,
        "headers": headers,
        "body": msg,
        "status_code": handler.status_code,
        "status_msg": handler.status_msg,
    })

    return service_response

def parse_service_response(msg, passphrase):
    """Static method for constructing a Reponse instance out of a
    message read straight off a zmq socket from a ServiceConnection.
    """
    #logging.debug("parse_service_response: %s" % msg)

    sender, conn_id, msg_passphrase, origin_sender_id, origin_conn_id, origin_out_addr, path, rest = msg.split(' ', 7)
    
    conn_id = parse_msgstring(conn_id)[0]
    msg_passphrase = parse_msgstring(msg_passphrase)[0]
    origin_sender_id = parse_msgstring(origin_sender_id)[0]
    origin_conn_id = parse_msgstring(origin_conn_id)[0]
    origin_out_addr = parse_msgstring(origin_out_addr)[0]
    path = parse_msgstring(path)[0]
    
    if msg_passphrase != passphrase:
        raise Exception('Unknown service identity! (%s != %s)' % (str(msg_passphrase),str(passphrase)))

    (status_code, rest) = parse_msgstring(rest)
    (status_msg, rest) = parse_msgstring(rest)
    (headers, rest) = parse_msgstring(rest)
    (body, rest) = parse_msgstring(rest)

    headers = json.loads(headers)
    body = json.loads(body)

    service_response = ServiceResponse(**{
        "sender": sender, 
        "conn_id": conn_id, 
        "path": path, 
        "origin_conn_id": origin_conn_id, 
        "origin_out_addr": origin_out_addr, 
        "status_code": int(status_code), 
        "status_msg": status_msg,
        "headers": headers, 
        "body": body, 
    })
    return service_response

class ServiceRequest(Document):
    """used to construct a Brubeck service request message.
    Both the client and the server use this.
    """
    # this is used internally and should never change
    message_type = MessageHandler._SERVICE_MESSAGE_TYPE

    @property
    def method(self):
        return self.headers["METHOD"]

    # this is used internally and should never change
    sender = StringField(required=True)
    # this is set by the send call in the client connection
    sender = StringField(required=True)
    # this is set by the send call in the client connection
    conn_id = StringField(required=True)
    # Not sure if this is the socket_id, but it is used to return the message to the originator
    origin_sender_id = StringField(required=True)
    # This is the connection id used by the originator and is needed for Mongrel2
    origin_conn_id  = StringField(required=True)
    # This is the socket address for the reply to the client
    origin_out_addr  = StringField(required=True)
    # used to route the request
    path = StringField(required=True)
    # a dict, right now only METHOD is required and must be one of: ['get', 'post', 'put', 'delete','options', 'connect', 'response', 'request']
    headers = DictField(required=False)
    # a dict, this can be whatever you need it to be to get the job done.
    body = DictField(required=True)

class ServiceResponse(ServiceRequest):
    """used to construct a Brubeck service response message.
    """
    status_code = IntField(required=True)
    status_message = StringField()

######################################################################################
## Brubeck service connections (service, client and mongrel2 with greenlet handlers)
######################################################################################

# this is outside the class in case we want to run with coro_spawn

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
        """This coroutine looks at the message, determines which handler will
        be used to process it, and then begins processing.
        
        The application is responsible for handling misconfigured routes.
        """
    
        service_request = parse_service_request(message, application.msg_conn.passphrase)
        service_request.message_type = MESSAGE_TYPES[application.msg_conn._BRUBECK_MESSAGE_TYPE]
    
        handler = application.route_message(service_request)
        result = handler()
    
        msg = {}
    
        if result is not None and result is not "":
            msg = json.dumps(result)
    
        service_response = create_service_response(service_request, handler, msg)
            
        application.msg_conn.send(service_response)

    def send(self, service_response):
        """uuid = unique ID that both the client and server need to match
           conn_id = connection id from this request needed to wake up handler on response
           origin_uuid = unique ID from the original request
           origin_conn_id = the connection id from the original request
           origin_out_addr = the socket address that expects the final result
           msg = the payload (a JSON object)
           path = the path used to route to the proper response handler
        """

        header = "%s %d:%s %d:%s %d:%s %d:%s %d:%s %d:%s" % ( service_response.sender,
            len(str(service_response.conn_id)), str(service_response.conn_id),
            len(self.passphrase), self.passphrase,
            len(service_response.origin_sender_id), service_response.origin_sender_id,
            len(str(service_response.origin_conn_id)), str(service_response.origin_conn_id),
            len(service_response.origin_out_addr), service_response.origin_out_addr,
            len(service_response.path), service_response.path
        )
        headers = to_bytes(json.dumps(service_response.headers))
        body = to_bytes(json.dumps(service_response.body))
        msg = '%s %d:%s%d:%s%d:%s%d:%s' % (header,
            len(str(service_response.status_code)), to_bytes(str(service_response.status_code)),
            len(service_response.status_msg), to_bytes(service_response.status_msg),
            len(headers), headers,
            len(body), body,
        )
        
        #logging.debug("ServiceConnection send: %s" % msg)
        
        self.out_sock.send(service_response.sender, self.zmq.SNDMORE)
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


# this is outside the class in case we want to run with coro_spawn
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

    def process_message(self, application, message, service_client, service_addr, handle=True):
        """This coroutine looks at the message, determines which handler will
        be used to process it, and then begins processing.
        Since this is a reply, not a request,
        we simply call the handler and are done
        returns a tuple containing 1) the response object created 
            from parsing the message and 2) the handlers return value
        """
        logging.debug("service_client_process_message")
        service_response = parse_service_response(message, self.passphrase)
    
        logging.debug("service_client_process_message service_response: %s" % service_response)
        
        logging.debug("service_client_process_message handle: %s" % handle)
        if handle:
            handler = application.route_message(service_response)
            handler.set_status(service_response.status_code,  service_response.status_msg)
            result = handler()
            #service_client._notify(service_addr, service_response, result)
            logging.debug("service_client_process_message service_response: %s" % service_response)
            logging.debug("service_client_process_message result: %s" % result)
            return (service_response, result)
    
        return (service_response, None)

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
        headers = to_bytes(json.dumps(service_req.headers))
        body = to_bytes(json.dumps(service_req.body))

        msg = ' %s %d:%s%d:%s' % (header, len(headers), headers, len(body), body)
        #logging.debug("ServiceClientConnection send: %s" % msg)
        self.out_sock.send(msg)

        return service_req

    def close(self):
        # we only have one socket, close it
        self.out_sock.close()

##########################################
## Handler stuff
##########################################
class ServiceMessageHandler(MessageHandler):
    """This class is the simplest implementation of a message handlers. 
    Intended to be used for Service inter communication.
    """
    def __init__(self, application, message, *args, **kwargs):
        self.headers = {}
        super(ServiceMessageHandler, self).__init__(application, message, *args, **kwargs)
        self.message_type = MESSAGE_TYPES[self._SERVICE_MESSAGE_TYPE]
        
    def render(self, status_code=None, status_msg=None, headers = None, **kwargs):
        if status_code is not None:
            self.set_status(status_code, status_msg)

        if headers is not None:
            self.headers = headers

        body = self._payload
        
        logging.info('%s %s %s (%s:%s) for (%s:%s)' % (self.status_code, self.message.method,
                                        self.message.path,
                                        self.message.sender,
                                        self.message.conn_id,
                                        self.message.origin_out_addr,
                                        self.message.origin_conn_id,
                                        ))

        return body

class ServiceClientMixin(object):
    """Adds the functionality to any handler to send messages to a ServiceConnection
    This must be used with a handler or something that has the following attributes:
        self.application
    """

    @property
    def zmq(self):
        if not hasattr(self, '_zmq'):
            setattr(self, '_zmq', load_zmq())
        return self._zmq


    @property
    def zmq_ctx(self):
        if not hasattr(self, '_zmq_ctx'):
            setattr(self, '_zmq_ctx', load_zmq_ctx())
        return self._zmq_ctx

    def _wait(self, service_addr, conn_id):
        # use an inproc socket, naturally non-blocking blocking
        # in memory sending, cheap
        zmq = self.zmq
        ctx = self.zmq_ctx
        conn_id = str(conn_id)
        rep_sock = ctx.socket(zmq.REP)
        logging.debug("starting internal reply socket %s" % conn_id)

        req_sock = ctx.socket(zmq.REQ)
        req_sock.bind('inproc://%s' % conn_id)

        rep_sock.connect('inproc://%s' % conn_id)

        waiting_sockets = self.application.get_waiting_sockets(service_addr)
        waiting_sockets[conn_id] = req_sock

        poller = zmq.Poller()
        poller.register(rep_sock, zmq.POLLIN)

        # Loop and accept messages from both channels, acting accordingly

        
        #sockets = poller.poll(30000)
        raw_response = None

        if False:
            #logging.debug("sockets: %s" % sockets)
            #try:
            #    if rep_sock in sockets:
            try:
                max_retries = 20
                retries = 0
                while retries < max_retries:
                    logging.debug("poller start")
                    sockets = dict(poller.poll(500))
                    logging.debug("poller stop")
                    retries += 1
                    logging.debug("sockets: %s" % sockets)
                    if rep_sock in sockets:
                        if sockets[rep_sock] == zmq.POLLIN:
                            raw_response = sockets[rep_sock].recv(zmq.NOWAIT)
            finally:
                rep_sock.close()
                logging.debug("closed internal reply socket %s" % conn_id)
                req_sock.close()
                logging.debug("closed internal request socket %s" % conn_id)

        raw_response = rep_sock.recv()

        if raw_response is not None:
            service_conn = self.application.get_service_conn(service_addr)
            logging.debug("process_message %s,%s,%s,%s" % (self.application, raw_response, self, service_addr))
            results = service_conn.process_message( self.application, raw_response, 
                self, service_addr
                )
            #logging.debug("RESULTS: %s" % results)
            return results
        else:
            logging.debug("NO RESULTS")
            return (None, None)
    ################################
    ## The public interface methods
    ## This is all your handlers should use
    ################################

    def create_service_request(self, path, headers, msg):
        """ path - string, used to route to proper handler
            headers - dict, contains the accepted method to call on handler
            msg - dict, the body of the message to process
        """
        if not isinstance(headers, dict):
            headers = json.loads(headers)
        if not isinstance(msg, dict):
            msg = json.loads(msg)

        data = {
            # Not sure if this is the socket_id, but it is used to return the message to the originator
            "origin_sender_id": self.message.sender,
            # This is the connection id used by the originator and is needed for Mongrel2
            "origin_conn_id": self.message.conn_id,
            # This is the socket address for the reply to the client
            "origin_out_addr": self.application.msg_conn.out_addr,
            # used to route the request
            "path": path,
            # a dict, right now only METHOD is required and must be one of: ['get', 'post', 'put', 'delete','options', 'connect', 'response', 'request']
            "headers": headers,
            # a dict, this can be whatever you need it to be to get the job done.
            "body": msg,
        }
        return ServiceRequest(**data)
    
    def register_service(self, service_addr, service_passphrase):
        """just a wrapper for our application level register_service method right now"""
        if not self.application.service_is_registered(service_addr):
            service_conn = ServiceClientConnection(
                service_addr, service_passphrase, async=True
            )
            return self.application.register_service(service_addr, service_conn)
        logging.debug("Service %s already registered" % service_addr)
        return True

    def forward(self, service_addr, service_req):
        """give up any responsability for the request, someone else will respond to the client
        non-blocking, returns immediately.
        """
        raise NotImplemented("forward is not yet implemented, use send_nowait instead")


    def send(self, service_addr, service_req):
        """do some work and wait for the results of the response to handle the response from the service
        blocking, waits for handled result.
        """
        service_req = self.application.send_service_request(service_addr, service_req)
        conn_id = service_req.conn_id
        (response, handler_response) = self._wait(service_addr, conn_id)

        return (response, handler_response)

    def send_nowait(self, service_addr, service_req):
        """defer some work, but still handle the response yourself
        non-blocking, returns immediately.
        """
        self.application.send_service_request(service_addr, service_req)
        return
