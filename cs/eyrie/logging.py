# -*- coding: utf-8 -*-
# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
from __future__ import absolute_import

import logging
import pickle
import socket

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, hmac

import zmq
from zmq.eventloop.zmqstream import ZMQStream

from cs.eyrie.config import LOGGING_ENDPOINT
from cs.eyrie.config import ZMQLogMessage


class HMACHandler(logging.Handler):

    def __init__(self, authkey):
        super(HMACHandler, self).__init__()
        self.authkey = authkey
        self.hostname = socket.gethostname()

    def emit(self, record):
        """Emit a log message on my socket."""
        try:
            pickled_record = pickle.dumps(record, pickle.HIGHEST_PROTOCOL)
        except (pickle.PicklingError,):
            return

        # The follow relies on the authkey being set to a value
        # shared between this process and the consuming SUB socket
        h = hmac.HMAC(self.authkey, hashes.SHA256(),
                      backend=default_backend())
        h.update(pickled_record)
        msg = ZMQLogMessage(
            record.name, self.hostname, h.finalize(), pickled_record,
        )

        self.send(msg)


class ZMQHandler(HMACHandler):
    """This is the blocking ZMQ publisher
    """

    def __init__(self, authkey, endpoint=LOGGING_ENDPOINT, context=None,
                 async=False, loop=None):
        super(ZMQHandler, self).__init__(authkey=authkey)
        if context is None:
            self.context = zmq.Context()
        else:
            self.context = context
        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect(endpoint)
        # TODO: support zmq.green (i.e., gevent vs. tornado)
        self.async = async
        if self.async:
            self.stream = ZMQStream(self.socket, io_loop=loop)

    def send(self, frames):
        """Emit a log message on my socket."""
        if self.async:
            if self.stream.closed():
                return
            try:
                self.stream.send_multipart(frames)
            except zmq.ZMQError as err:
                # Don't interfere further
                pass
        else:
            if self.socket.closed:
                return
            try:
                self.socket.send_multipart(frames)
            except zmq.ZMQError as err:
                # Don't interfere further
                pass


class HostnameFilter(logging.Filter):

    hostname = socket.gethostname()

    def filter(self, record):

        if not hasattr(record, 'hostname'):
            setattr(record, 'hostname', self.hostname)

        return True
