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

from connections import ServiceClientConnection
from request import Response, to_bytes, to_unicode
from dictshield.document import Document
from dictshield.fields import (StringField,
                               BooleanField,
                               URLField,
                               EmailField,
                               LongField)

class ServiceRequest(Document):
    """used to construct a Brubeck service request message"""
    # this is set by the send call in the connection
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
    headers = StringField(required=False)
    # a dict, this can be whatever you need it to be to get the job done.
    body = StringField(required=True)

                #gevent.joinall([
                #    gevent.spawn(upload_to_S3, self.message, self.uploader_settings, image_hash, self.company_image_queryset, self.application)
                #    ])
                #logging.debug("spawned upload for %s" % image_hash)

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
            #gevent.joinall([
            #    gevent.spawn(start_service_response_listener, self.application, service_addr, service_conn, self)
            #    ])

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
                    gevent.sleep(1)
            return value

            # this is how I want to do it
            # works only if each request is processed in it's own coroutine
            #event.wait()
            #return event.value

        else:
            raise exception('%s is already waiting on %s!' % (conn_id, service_addr))
            
    def _notify(self, service_addr, response, handler_response):
        events = self._get_events(service_addr)

        for key, value in events.iteritems() :
            logging.debug("events[%s] = %s" % (key, value))

        conn_id = response.conn_id
        
        if conn_id in events:
            try:
                # wake up and pass value
                events[conn_id].set((response, handler_response))
                
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
            "headers": json.dumps(headers),
            # a dict, this can be whatever you need it to be to get the job done.
            "body": json.dumps(msg),
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
