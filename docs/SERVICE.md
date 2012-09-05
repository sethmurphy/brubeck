
# Service

Brubeck uses ZMQ (ZeroMQ) to receive messages when using Mongrel2 as a web front end. Services allow you to use ZMQ to also make requests to Brubeck Services. A Brubeck Service is a special instance that listens for requests from other Brubeck Instances instead of Mongrel2 (or a wsgi server). 

Distributing all or part of a request to a Bruebck Service allows CPU intensive processes to be run on other machines/processes. This then frees up the inital Brubeck Instance to work on other web requests, handling i/o intensive processes only.

## Service Patterns

Services are called from handler and can have the following patterns.

1.  Asynchronous Service Call
    
    A service is called and control is immediately returned to the calling service which is then responsible for responding to the initial the web request.
    A response is still sent back to the Brubeck Application Instance and handled as needed, however the client is no longer involved. 

2.  Synchronous Service Call
    
    A service is called and the result is returned when it is ready. This behaves the same as a blocking call in the handler code, but allows the pre-emptive concurrrency model to switch context until a result is received.

    A response is sent back to the Brubeck Application Instance and handled as needed. The caller then get's the initial request and the handled response returned to them in a tuple. 
    Using the returned ReplyRequest and response text(if needed) they are responsible for responding to the initial the web request.

## Brubeck Service

A brubeck service is a Brubeck instance that has been started with a `service.ServiceConnection` connection type and has been configured to handle routes with at least one `BrubeckMessageHandler`.

Here is the complete Brubeck Service from [demo_service.py](https://github.com/sethmurphy/brubeck/blob/brubeck-service/demos/demo_service.py)

    #!/usr/bin/env python
    import logging
    import time
    from brubeck.request_handling import Brubeck
    from brubeck.service import (
        ServiceConnection,
        ServiceMessageHandler,
    )
    from brubeck.templating import (
        Jinja2Rendering,
        load_jinja2_env,
    )
    
    class SlowEchoServiceHandler(ServiceMessageHandler):
        """A slow service"""
        
        def request(self):
            time.sleep(5)
            self.set_status(200, "Took a while, but I am back.")
            self.add_to_payload("RETURN_DATA", self.message.body["RETURN_DATA"])
            self.headers = {"METHOD": "response"}
            return self.render()
    
    
    ##
    ## runtime configuration
    ##
    config = {
        'msg_conn': ServiceConnection('ipc://run/slow', 'my_shared_secret'),
        'handler_tuples': [ ## Set up our routes
            # Handle our service responses
            (r'^/service/slow', SlowEchoServiceHandler),
        ],
        'cookie_secret': '51cRa%76fa^O9h$4cwl$!@_F%g9%l_)-6OO1!',
        'template_loader': load_jinja2_env('./templates/service'),
        'log_level': logging.DEBUG,
    }
    
    ##
    ## get us started!
    ##
    app = Brubeck(**config)
    ## start our server to handle requests
    if __name__ == "__main__":
        app.run()

Requested received are routed based on the messages `Path` and `METHOD` to corresponding handlers.

Here is a simple handler.

    class SlowEchoServiceHandler(ServiceMessageHandler):
        """A slow service"""
    
        def request(self):
            """do something and take too long"""
            time.sleep(5)
            self.add_to_payload("RETURN_DATA", self.message.body["RETURN_DATA"])
            self.headers = {"METHOD": "response"}
            self.set_status(200, "Took a while, but I am back.")
            return self.render()

Adding attributes that will be part of the returned JSON result is as easy as calling `add_to_payload(key, value)`.
We then set the headers with the only required attribute `METHOD` set to `response`. Our status is HTTP like and we set to `200` to indicate success.
Calling `self.render()` will create a JSON body from the payload and send a `ServiceResponse` to the client that made the initial request. The `path` of the reply will be the same as the request, and along with the `METHOD` will be used to map to the proper handler on the initiating client side.

## Brubeck Service Client

A brubeck service client is a Brubeck Instance that has been started with a `connection.Mongrel2CoConnection` and uses at least one `MessageHandler` instance that is a `service.ServiceClientMixin`.
The `connection.Mongrel2CoConnection` is only diffferent from a `connection.Mongrel2Connection` because each handler is run within it's own Greenlet context. This is important because there are calls that would block the main thread otherwise.

Here is the complete Brubeck Service Client from [demo_service_client.py](https://github.com/sethmurphy/brubeck/blob/brubeck-service/demos/demo_service.py)

    import logging
    from brubeck.connections import Mongrel2CoConnection
    from brubeck.request_handling import (
        JSONMessageHandler,
        WebMessageHandler, 
        Brubeck,
    )
    from brubeck.service import (
        ServiceClientMixin,
        ServiceMessageHandler,
    )
    from brubeck.templating import (
        Jinja2Rendering,
        load_jinja2_env,
    )
    
    # some static data for testing
    service_addr = "ipc://run/slow"
    service_passphrase = "my_shared_secret"
    service_path = '/service/slow'
    request_headers = {"METHOD":'request'}
    sync_request_body  = {"RETURN_DATA": 'I made a round trip, it took a while and I have a great story.'}
    async_request_body = {"RETURN_DATA": 'I made a round trip, it took so long and everyone is gone now.'}
    
    
    class DemoHandler(
            Jinja2Rendering,
            WebMessageHandler
        ):
    
        def get(self):
            # just return a page with some links
            return self.render_template('index.html')
    
    class CallServiceAsyncHandler(
            Jinja2Rendering,
            ServiceClientMixin,
            WebMessageHandler
        ):
    
        def get(self):
            # register our service, if exist nothing happens
            self.register_service(service_addr, service_passphrase)
            # create a servicerequest
            service_request = self.create_service_request(
                service_path, 
                request_headers, 
                async_request_body
            )
    
            ## Async
            self.send_nowait(service_addr, service_request)
            
            # now return to client whatever you want
            self.set_status(200)
            context = {
                'name': "Async is faster, but ... nothing to report on my trip.",
            }
            return self.render_template('success.html', **context)
    
    
    class CallServiceSyncHandler(
            Jinja2Rendering,
            ServiceClientMixin,
            WebMessageHandler
        ):
    
        def get(self):
            # register our service, if exist nothing happens
            self.register_service(service_addr, service_passphrase)
            # create a servicerequest
            service_request = self.create_service_request(
                service_path, 
                request_headers, 
                sync_request_body
            )
            
            
            ## Sync
            (response, handler_response) = self.send(service_addr, service_request)
    
            logging.debug("Took a while, but lot's to say now")
            logging.debug("response: %s" % response)
            logging.debug("handler_response: %s" % handler_response)
    
            # now return to client what you got back
            self.set_status(200)
            context = {
                'name': response.body["RETURN_DATA"],
            }
            return self.render_template('success.html', **context)
    
    class ServiceResponseHandler(ServiceMessageHandler):
        """handles the response from our service
        """
        
        def response(self):
            """On successfull upload by uploader BrubeckInstance"""
            if self.status_code == 200:
                logging.debug("Successfull %s:%s)!" % (self.status_code,self.status_msg))
            else:
                logging.debug("Failed (%s:%s)!" % (self.status_code,self.status_msg))
            return self.render()
    
    
    ##
    ## runtime configuration
    ##
    config = {
        # we need a Mongrel2CoConnection to run each requests handling  in a greenlet
        'msg_conn': Mongrel2CoConnection('tcp://127.0.0.1:9999', 
                                         'tcp://127.0.0.1:9998'),
        'handler_tuples': [ ## Set up our routes
            # Handle our service responses
            (r'^/service/slow', ServiceResponseHandler),
            (r'^/service/sync', CallServiceSyncHandler),
            (r'^/service/async', CallServiceAsyncHandler),
            (r'^/', DemoHandler),
        ],
        'cookie_secret': '51cRa%76fa^O9h$4cwl$!@_F%g9%l_)-6OO1!',
        'template_loader': load_jinja2_env('./templates/service'),
        'log_level': logging.DEBUG,
    }
    
    ##
    ## get us started!
    ##
    app = Brubeck(**config)
    ## start our server to handle requests
    if __name__ == "__main__":
        app.run()

Let's look at some of the more important things we need to do:

1.  Make sure your brubeck Instance is running a Connection handler that wraps each request in it's own Greenlet.

    'msg_conn': Mongrel2CoConnection('tcp://127.0.0.1:9999', 
                                     'tcp://127.0.0.1:9998'),

    Here in the `config` for Brubeck we make sure to use a `Mongrel2CoConnection`.

2.  Register a handler to handle responses from a Brubeck Service.

        config = {
            ...
            'handler_tuples': [ ## Set up our routes
                ...
                (r'^/service/slow', ServiceResponseHandler),
                ...
            ],
            ...
        }
    
    This handler can be as simple as the following extending `ServiceMessageHandler`:

        class ServiceResponseHandler(ServiceMessageHandler):
            def response(self):
                """On successfull upload by uploader BrubeckInstance"""
                if self.status_code == 200:
                    logging.debug("Successfull %s:%s)!" % (self.status_code,self.status_msg))
                else:
                    logging.debug("Failed (%s:%s)!" % (self.status_code,self.status_msg))
                return self.render()

    `response` in the default method that is caled on a ReponseHandler if no other is specified. Only the following methods are supported:

        'get', 'post', 'put', 'delete', 'options', 'connect', 'response', 'request'

    Notice how it is using the status_code as a return value from the service. The following `status_code`s are supported (from `request_handling.WebMessage`):

        _DEFAULT_STATUS = 500  # default to server error
        _SUCCESS_CODE = 200
        _UPDATED_CODE = 200
        _CREATED_CODE = 201
        _MULTI_CODE = 207
        _FAILED_CODE = 400
        _AUTH_FAILURE = 401
        _FORBIDDEN = 403
        _NOT_FOUND = 404
        _SERVER_ERROR = 500

3.  Extend `ServiceClientMixin` to be able to make calls to a service from any handlers.

        class CallServiceSyncHandler(
                Jinja2Rendering,
                ServiceClientMixin,
                WebMessageHandler
            ):
    
            def get(self):
                # register our service, if exist nothing happens
                self.register_service(service_addr, service_passphrase)
                # create a servicerequest
                service_request = self.create_service_request(
                    service_path, 
                    request_headers, 
                    sync_request_body
                )
            
            
                ## Sync
                (response, handler_response) = self.send(service_addr, service_request)
    
                logging.debug("Took a while, but lot's to say now")
                logging.debug("response: %s" % response)
                logging.debug("handler_response: %s" % handler_response)
    
                # now return to client what you got back
                self.set_status(200)
                context = {
                    'name': response.body["RETURN_DATA"],
                }
                return self.render_template('success.html', **context)

    Let's look at a typical synchronous request.

    *   Create a ServiceRequest.
    
            service_request = self.create_service_request(
                service_path, 
                request_headers, 
                sync_request_body
            )
    
    *   Send a syncronouse ServiceRequest to a service

            (response, handler_response) = self.send(service_addr, service_request)

    `service_addr` is a ZMQ protocol address such as `tcp://127.0.0.1:9999/my_service`

    *   To send an asyncronous request the above line would change to:

            self.send_nowait(service_addr, service_request)


## Examples
Brubeck comes with the above example complete as a demo.

* [Demo Service](https://github.com/sethmurphy/brubeck/blob/brubeck-service/demos/demo_service.py)
* [Demo Service Client](https://github.com/sethmurphy/brubeck/blob/brubeck-service/demos/demo_service_client.py)
