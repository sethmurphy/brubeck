### Attempt to setup gevent
try:
    from gevent import monkey
    monkey.patch_all()
    import gevent
    from gevent import pool
    from gevent import Greenlet
    from gevent.event import AsyncResult

    coro_pool = pool.Pool
    coro_thread = Greenlet

    def coro_spawn(function, app, message, *a, **kw):
        app.pool.spawn(function, app, message, *a, **kw)

    CORO_LIBRARY = 'gevent'

### Fallback to eventlet
except ImportError:
    raise EnvironmentError(
        'brubeck service connections do not support the eventlet concurrency model.'
    )
    # TODO: support eventlet
    try:
        import eventlet
        import eventlet.greenthread
        eventlet.patcher.monkey_patch(all=True)

        coro_pool = eventlet.GreenPool
        coro_thread = greenthread

        def coro_spawn(function, app, message, *a, **kw):
            app.pool.spawn_n(function, app, message, *a, **kw)

        CORO_LIBRARY = 'eventlet'

    except ImportError:
        raise EnvironmentError('You need to install eventlet or gevent')


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
from request_handling import MessageHandler, http_response, MESSAGE_TYPES
from dictshield.document import Document
from dictshield.fields import (StringField,
                               BooleanField,
                               URLField,
                               EmailField,
                               LongField,
                               DictField,
                               IntField)
from uuid import uuid4


_DEFAULT_SERVICE_REQUEST_METHOD = 'request'
_DEFAULT_SERVICE_RESPONSE_METHOD = 'response'

##########################################
## Handler stuff
##########################################
class ServiceClientListener(Greenlet):
    """Adds the functionality to any handler to send messages to a ServiceConnection
    """
    def __init__(self, application, service_addr, service_conn, service_client, *args, **kwargs):
        """handler is a ServiceClientMixin"""
        super(ServiceClientListener, self).__init__()
        self.service_addr = service_addr
        self.service_conn = service_conn
        self.service_client = service_client
        self.application = application
        self._keep_running = True
        gevent.sleep(0)

    def _run(self):
        logging.debug("ServiceClientListener run()")
        while self._keep_running:
            raw_response = self.service_conn.recv()
            (response, handler_response) = self.service_conn.process_message(application, response)
            self.service_client._notify(service_addr, response, handler_response)

def service_response_listener(application, service_addr, service_conn, service_client):
    logging.debug("Starting start_service_response_listener");
    while True:
        logging.debug("service_response_listener waiting");
        raw_response = service_conn.recv()
        logging.debug("service_response_listener waiting received response: %s" % raw_response);
        service_conn.process_message( application, raw_response, 
            service_client, service_addr
            )

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
        
        logging.info('%s %s %s (%s:%s) for (%s:%s)' % (status_code, self.message.method,
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

    # Dicts to hold our registered connections
    # There may be one per adress
    # Maybe they should go at the application level, 
    # but I am treading lightly...

    # holds our registered service connections and corresponding listeners
    __services = {}  

    def _register_service(self, service_addr, service_passphrase):
        """ Create and store a connection and it's listener and events queue.
        To be safe, for now there is no unregister.
        """ 
        if service_addr not in self.__services:
            # create our service connection
            logging.debug("_register_service creating service_conn: %s" % service_addr)
            service_conn = ServiceClientConnection(
                service_addr, service_passphrase, async=True
            )

            # create and start our listener
            logging.debug("_register_service starting listener: %s" % service_addr)
            service_listener = Greenlet(service_response_listener, self.application, service_addr, service_conn, self)
            service_listener.start()
            # give above process a chance to start
            gevent.sleep(0)


            #service_listener = ServiceClientListener(self.application, service_addr, service_conn, self)
            #service_listener.start()
            #service_listener.join()

            # add us to the list
            self.__services[service_addr] = {
                'conns': service_conn,
                #'service_listener': service_listener ,
                'events': {},
            }
            logging.debug("_register_service success: %s" % service_addr)
        else:
            logging.debug("_register_service ignored: %s already registered" % service_addr)

    def _get_conn_info(self, service_addr):
        if service_addr in self.__services:
            return self.__services[service_addr]
        else:
            raise Exception("%s service not registered" % service_addr)

    def _get_conn(self, service_addr):
        return self._get_conn_info(service_addr)['conns']

    def _send(self, service_addr, service_req):
        """send our message, used internally only"""
        conn = self._get_conn(service_addr)
        return conn.send(service_req)

    def _get_events(self, service_addr):
        return self._get_conn_info(service_addr)['events']

    def _wait(self, service_addr, conn_id):
        events = self._get_events(service_addr)
        if conn_id not in events:
            event = AsyncResult()
            logging.debug("registering event for conn_id %s" % conn_id)
            events[conn_id] = event
            
            # this is a kludge
            if False:
                loop = True
                value = None
                # I don't like this
                # seems like the only way I can keep 
                # one handler job waiting though
                # event.wait blocks everything
    
                while loop:
                    try:
                        value = event.get_nowait()
                        loop = False
                    except:
                        gevent.sleep(0)
                return value
            else:
                # this is how I want to do it
                # works only if each request is processed in it's own coroutine
                event.wait()
                return event.value

        else:
            raise exception('%s is already waiting on %s!' % (conn_id, service_addr))
            
    def _notify(self, service_addr, service_response, handler_response):
        events = self._get_events(service_addr)

        for key, value in events.iteritems() :
            logging.debug("events[%s] = %s" % (key, value))

        conn_id = service_response.conn_id
        
        if conn_id in events:
            try:
                # wake up and pass value
                events[conn_id].set((service_response, handler_response))
                
                # make sure our event is received before we delete it
                gevent.sleep(0) 
            except:
                pass
            finally:
                try:
                    del events[conn_id]
                except:
                    pass
        else:
            logging.debug("conn_id %s not found to notify." % conn_id)
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
        """just a wrapper for our internal_register_service method right now"""
        return self._register_service(service_addr, service_passphrase)

    def punt(self, service_addr, service_req):
        """give up any responsability for the request, someone else will respond to the client
        """
        raise NotImplemented("punt is not yet implemented, use run instead")


    def run(self, service_addr, service_req):
        """do some work, but wait for the response to handle the response from the service
        """
        service_req = self._send(service_addr, service_req)
        conn_id = service_req.conn_id
        (response, handler_response) = self._wait(service_addr, conn_id)

        return (response, handler_response)

    def execute(self, service_addr, service_req):
        """defer some work, but still handle the response yourself"""
        self._send(service_addr, service_req)
        return


#################################
## Request and Response stuff 
#################################

def parse_msgstring(field_text):
    """ field_value - a value in n:data format where n is the data length
            and data is the text to get the first n chars from
        returns the a tuple containing the value and whatever remains
    """
    field_data = field_text.split(':', 1)
    expected_len = int(field_data[0])
    field_value = field_data[1]
    value = field_value[0:expected_len]
    rest = field_value[expected_len:] if len(field_value) > expected_len else ''
    return (value, rest)

def parse_service_request(msg, passphrase):
    """Static method for constructing a Request instance out of a
    message read straight off a zmq socket from a ServiceClientConnection.
    """
    logging.debug("parse_service_request: %s" % msg)
    sender, conn_id, msg_passphrase, origin_sender_id, origin_conn_id, origin_out_addr, path, rest = msg.split(' ', 7)

    conn_id = parse_msgstring(conn_id)[0]
    msg_passphrase = parse_msgstring(msg_passphrase)[0]
    origin_sender_id = parse_msgstring(origin_sender_id)[0]
    origin_conn_id = parse_msgstring(origin_conn_id)[0]
    origin_out_addr = parse_msgstring(origin_out_addr)[0]
    path = parse_msgstring(path)[0]
    
    if msg_passphrase != passphrase:
        raise Exception('Unknown service identity! (%s != %s)' % (str(msg_passphrase),str(passphrase)))

    logging.debug("parse_service_request: rest: %s" % rest)
    headers, body = parse_msgstring(rest)
    logging.debug("parse_service_request: headers: %s" % headers)
    logging.debug("parse_service_request: body: %s" % body)
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
    logging.debug("parse_service_response: %s" % msg)

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
def service_process_message(application, message):
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
        #service_process_message(application, message)

        # with coroutine
        coro_spawn(service_process_message, application, message)

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
        
        logging.debug("ServiceConnection send: %s" % msg)
        
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
def service_client_process_message(application, message, passphrase, service_client, service_addr, handle=True):
    """This coroutine looks at the message, determines which handler will
    be used to process it, and then begins processing.
    Since this is a reply, not a request,
    we simply call the handler and are done
    returns a tuple containing 1) the response object created 
        from parsing the message and 2) the handlers return value
    """
    service_response = parse_service_response(message, passphrase)
    
    if handle:
        handler = application.route_message(service_response)
        handler.set_status(service_response.status_code,  service_response.status_msg)
        result = handler()
        service_client._notify(service_addr, service_response, result)

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
        headers = to_bytes(json.dumps(service_req.headers))
        body = to_bytes(json.dumps(service_req.body))

        msg = ' %s %d:%s%d:%s' % (header, len(headers), headers, len(body), body)
        logging.debug("ServiceClientConnection send: %s" % msg)
        self.out_sock.send(msg)

        return service_req
        
        #if self.async:
        #    return

        #return self.recv()

def mongrel2co_process_message(application, message):
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

class Mongrel2CoConnection(Mongrel2Connection):
    """This class is specific to handling messages from Mongrel2 and running in request in it's own greenlet.
    """

    def process_message(self, application, message):
        """process a message in a coroutine so we can have handlers safely use blocking gevent function"""
        coro_spawn(mongrel2co_process_message, application, message)