#!/usr/bin/env python

"""A resource is an application level object to store arbitrary data in.

"""

import re
import time
import logging
import inspect
import Cookie
import base64
import hmac
import cPickle as pickle
from itertools import chain
import os, sys
import ujson as json
from dictshield.base import ShieldException
from dictshield.document import Document
from dictshield.fields import StringField, DictField
from request import Request, to_bytes, to_unicode


class Resource(Document):
    """used to construct a Brubeck Resource.
    A resource may implement the following methods few methods:
        on_registered(application)
            `on_registered` is called once when a `resource` is registered.
        on_unregistered(application)
            `on_unregistered` is called once when a `resource` is unregistered.

        set(resource)
            sets a resource for the instance
        get()
            retrun the last `resource` that was `set(resource)`


        init expects `resource` as a keyword parameter 
    """
    # The name of the resource. This is what it is looked up by.
    name = StringField(required=True)
    # a resource_type such as data_connection, queryset, service, anything really
    resource_type = StringField(required=True)
    # a list of categories
    category = DictField(required=True)

    def __init__(self, *args, **kwargs):
        self.resource = None # this is where we store the user defined resource
        self.start_timestamp = int(time.time() * 1000)
        self.end_timestamp = self.start_timestamp
        super(Resource, self).__init__(*args, **kwargs)


    def _on_register(self):
        self.on_unregister()

    def _on_unregister(self):
        self.on_unregister()

    def set(self, resource):
        self.resource = resource;

    def get(self):
        return self.resource;

    def key(self):
        """used as the key when a resource is stored"""
        return Resource.create_key(self.name, self.resource_type)

    @staticmethod
    def create_key(name, resource_type):
        """used as the key when a resource is stored"""
        return ((resource_type if resource_type is not None else '') +
            (name if name is not None else ''))

    ###########
    ## over-ride these in your app if you need 
    ## to do something durinsg registration/unregistration
    ###########
    def on_register(self):
        pass

    def on_unregister(self,):
        pass

