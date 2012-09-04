# Service

Brubeck uses ZMQ (ZeroMQ) to receive messages when using Mongrel2 as a web front end. Services allow you to use ZMQ to also make requests to Brubeck Services. A Brubeck Service is a special instance that listens for requests from other Brubeck Instances instead of Mongrel2 (or a wsgi server). 

Distributing all or part of a request to a Bruebck Service allows CPU intensive processes to be run on other machines/processes. This then frees up the inital BrubeckInstance to work on other web requests, handling i/o intensive processes only.

## Service Patterns

Services are called from handler and can have the following patterns.

1.  Asynchronous Service Call
    A service is called and controll is immedietally returned to the calling service which is then responsible for responding to teh initial the web request.
    A response is still sent back to the Brubeck Application Instance and handled as needed, however the client is no longert involved. 

2.  Synchronous Service Call
    A service is called and the result is returned when it is ready. This behaves as a blocking call in the handler code, but allows the pre-emptive concurrrency model to switch context until a result is received.    
    A response is sent back to the Brubeck Application Instance and handled as needed. The caller get's the initial request and the handled response returnd to them in a tupple. 
    Using the returned ReplyRequest and response text(if needed) they are responsible for responding to the initial the web request.

## Brubeck Service

A brubeck service is a Brubeck instance that has been started with a `service.ServiceConnection` connection type and has been configured to handle routes with at least one `BrubeckMessageHandler`.

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

## Brubeck Service Client

A brubeck service client is a `MessageHandler` instance that uses a `service.ServiceClientMixin`.

## Examples
Brubeck comes with the above example complete as a demo.

* [Service](https://github.com/sethmurphy/brubeck/blob/brubeck-service/demos/demo_service.py)
* [Service Client](https://github.com/sethmurphy/brubeck/blob/brubeck-service/demos/demo_service_client.py)

