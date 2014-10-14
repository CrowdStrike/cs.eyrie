# -*- coding: utf-8 -*-
# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
from __future__ import absolute_import

import json
import logging
import multiprocessing
import os
import pickle
import socket

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers import algorithms
from cryptography.hazmat.primitives.ciphers import modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

import zmq
from zmq.eventloop.zmqstream import ZMQStream

from cs.eyrie.config import DEFAULT_ITERATIONS
from cs.eyrie.config import DEFAULT_IV_BITS
from cs.eyrie.config import LOGGING_ENDPOINT
from cs.eyrie.config import ZMQLogMessage


class ZMQHandler(logging.Handler):

    frame_class = ZMQLogMessage
    version = '1'

    def __init__(self, endpoint=LOGGING_ENDPOINT, context=None,
                 async=False, loop=None, serialization_format='json',
                 sanitize_log_records=True):
        self.hostname = socket.gethostname()
        self.serialization_format = serialization_format
        self.sanitize_log_records = sanitize_log_records
        self.logger_type = self.__class__.__name__
        super(ZMQHandler, self).__init__()
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

    def emit(self, record):
        serialized_record = self.serialize(record)
        self.send(logger_name=record.name, serialized_record=serialized_record)

    def send(self, **kwargs):
        """Emit a log message on my socket."""
        kwargs.setdefault('hostname', self.hostname)
        kwargs.setdefault('logger_type', self.logger_type)
        kwargs.setdefault('serialization_format', self.serialization_format)
        kwargs.setdefault('version', self.version)
        for fname in ('digest', 'iv', 'tag'):
            kwargs.setdefault(fname, b'')
        enc_kwargs = {
            k: v if isinstance(v, bytes) else v.encode('utf8')
            for k, v in kwargs.items()
        }
        frames = self.frame_class(**enc_kwargs)
        try:
            if self.async:
                if self.stream.closed():
                    return
                self.stream.send_multipart(frames)
            else:
                if self.socket.closed:
                    return
                self.socket.send_multipart(frames)
        except zmq.ZMQError:
            pass

    def serialize(self, record):
        if self.sanitize_log_records:
            # The following is to prevent any 3rd-party objects
            # from appearing in the remote side
            rdata = vars(record)
            msg = self.format(record)
            rdata['args'] = tuple()
            rdata['msg'] = msg
            rdata['message'] = msg
            rdata['exc_info'] = None
            rdata['exc_text'] = None
            if self.serialization_format == 'pickle':
                sanitized_record = logging.makeLogRecord(rdata)
                try:
                    return pickle.dumps(sanitized_record,
                                        pickle.HIGHEST_PROTOCOL)
                except (pickle.PicklingError,):
                    pass
            elif self.serialization_format == 'json':
                try:
                    return json.dumps(rdata)
                except ValueError:
                    pass
        else:
            if self.serialization_format == 'pickle':
                try:
                    return pickle.dumps(record, pickle.HIGHEST_PROTOCOL)
                except (pickle.PicklingError,):
                    pass
            elif self.serialization_format == 'json':
                try:
                    return json.dumps(vars(record))
                except ValueError:
                    pass


class FernetHandler(ZMQHandler):

    def __init__(self, authkey=None, **zmq_kwargs):
        super(FernetHandler, self).__init__(**zmq_kwargs)
        if authkey is None:
            curr_proc = multiprocessing.current_process()
            authkey = curr_proc.authkey
        self.fernet = Fernet(authkey)

    def emit(self, record):
        serialized_record = self.serialize(record)
        if serialized_record:
            token = self.fernet.encrypt(serialized_record.encode('utf8'))
            self.send(logger_name=record.name, serialized_record=token)


# https://cryptography.io/en/latest/hazmat/primitives/key-derivation-functions/#cryptography.hazmat.primitives.kdf.pbkdf2.PBKDF2HMAC
# https://cryptography.io/en/latest/hazmat/primitives/symmetric-encryption/#cryptography.hazmat.primitives.ciphers.modes.GCM
class GCMHandler(ZMQHandler):

    iv_bits = DEFAULT_IV_BITS

    def __init__(self,
                 authkey=None,
                 authkey_salt=None,
                 password_hash_alg=hashes.SHA256(),
                 iterations=DEFAULT_ITERATIONS,
                 key_bits=32,
                 backend=default_backend(),
                 **zmq_kwargs):
        super(GCMHandler, self).__init__(**zmq_kwargs)
        # The password used to generate the key
        if authkey is None:
            curr_proc = multiprocessing.current_process()
            authkey = curr_proc.authkey
        self.backend = backend
        self.algo = self.init_algo(authkey, authkey_salt, password_hash_alg,
                                   key_bits, iterations)

    def init_algo(self, key_password, key_salt, password_hash_alg,
                  key_bits, iterations):
        kdf = PBKDF2HMAC(
            algorithm=password_hash_alg,
            length=key_bits,
            salt=key_salt,
            iterations=iterations,
            backend=self.backend
        )
        return algorithms.AES(kdf.derive(key_password))

    def emit(self, record):
        serialized_record = self.serialize(record)
        if serialized_record:
            # Generate a random 96-bit IV.
            iv = os.urandom(self.iv_bits / 4)
            # Construct an AES-GCM Cipher object with the given key and a
            # randomly generated IV.
            encryptor = Cipher(
                self.algo,
                modes.GCM(iv),
                backend=self.backend,
            ).encryptor()

            # associated_data will be authenticated but not encrypted,
            # it must also be passed in on decryption.
            encryptor.authenticate_additional_data(record.name)

            # Encrypt the plaintext and get the associated ciphertext.
            # GCM does not require padding.
            ciphertext = encryptor.update(serialized_record)
            ciphertext = ciphertext + encryptor.finalize()
            self.send(logger_name=record.name, serialized_record=ciphertext,
                      iv=iv, tag=encryptor.tag)


class HMACHandler(ZMQHandler):

    def __init__(self,
                 authkey=None,
                 hash_alg=hashes.SHA256(),
                 backend=default_backend(),
                 **zmq_kwargs):
        super(HMACHandler, self).__init__(**zmq_kwargs)
        if authkey is None:
            curr_proc = multiprocessing.current_process()
            authkey = curr_proc.authkey
        self.hmac = hmac.HMAC(authkey, hash_alg, backend=backend)

    def emit(self, record):
        serialized_record = self.serialize(record)
        if serialized_record:
            h = self.hmac.copy()
            h.update(serialized_record)
            self.send(logger_name=record.name,
                      serialized_record=serialized_record,
                      digest=h.finalize())


class HostnameFilter(logging.Filter):

    hostname = socket.gethostname()

    def filter(self, record):

        if not hasattr(record, 'hostname'):
            setattr(record, 'hostname', self.hostname)

        return True
