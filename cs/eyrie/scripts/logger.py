# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
from collections import Counter
import json
import logging
import pickle
import time

import zmq

from cryptography.exceptions import InvalidTag
from cryptography.fernet import Fernet
from cryptography.fernet import InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers import algorithms
from cryptography.hazmat.primitives.ciphers import modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.exceptions import InvalidSignature

from tornado.ioloop import PeriodicCallback

from cs.eyrie import Vassal
from cs.eyrie import ZMQChannel
from cs.eyrie import script_main
from cs.eyrie.config import DEFAULT_ITERATIONS
from cs.eyrie.config import LOGGING_PORT
from cs.eyrie.config import LogMessageHandler
from cs.eyrie.config import ZMQLogMessage


class FernetHandler(object):

    def __init__(self, **kwargs):
        self.ttl = kwargs.pop('ttl', None)
        try:
            self.fernet = Fernet(kwargs['authkey'])
        except TypeError:
            txt = 'Invalid authkey. Must be the output of: %s'
            raise TypeError(txt % 'cryptography.fernet.Fernet.generate_key()')

    def handle(self, msg):
        return self.fernet.decrypt(msg.serialized_record, self.ttl)


# https://cryptography.io/en/latest/hazmat/primitives/symmetric-encryption/#cryptography.hazmat.primitives.ciphers.modes.GCM
class GCMHandler(object):

    def __init__(self, **kwargs):
        salt = kwargs['authkey_salt']
        self.backend = kwargs.get('backend', default_backend())
        kdf = PBKDF2HMAC(
            algorithm=kwargs.get('password_hash_alg', hashes.SHA256()),
            length=kwargs.get('key_bits', 32),
            salt=salt,
            iterations=kwargs.get('iterations', DEFAULT_ITERATIONS),
            backend=self.backend,
        )
        key = kdf.derive(kwargs['authkey'])
        self.algo = algorithms.AES(key)

    def handle(self, msg):
        # Construct a Cipher object, with the key, iv, and additionally the
        # GCM tag used for authenticating the message.
        decryptor = Cipher(
            self.algo,
            modes.GCM(msg.iv, msg.tag),
            backend=self.backend
        ).decryptor()

        # We put associated_data back in or the tag will fail to verify
        # when we finalize the decryptor.
        decryptor.authenticate_additional_data(msg.logger_name)

        # Decryption gets us the authenticated plaintext.
        # If the tag does not match an InvalidTag exception will be raised.
        plaintext = decryptor.update(msg.serialized_record)
        plaintext = plaintext + decryptor.finalize()

        return plaintext


class HMACHandler(object):

    def __init__(self, **kwargs):
        self.hmac = hmac.HMAC(kwargs['authkey'], hashes.SHA256(),
                              backend=kwargs.get('backend', default_backend()))

    def handle(self, msg):
        h = self.hmac.copy()
        h.update(msg.serialized_record)
        h.verify(msg.digest)
        return msg.serialized_record


# This is a stub to allow plaintext messages
class ZMQHandler(object):

    def __init__(self, **kwargs):
        pass

    def handle(self, msg):
        return msg.serialized_record


LogMessageHandler.register(FernetHandler)
LogMessageHandler.register(GCMHandler)
LogMessageHandler.register(HMACHandler)
LogMessageHandler.register(ZMQHandler)


class Scribe(Vassal):
    channels = dict(
        Vassal.channels,
        logger=ZMQChannel(
            # FIXME: make this configurable
            endpoint='tcp://*:%d' % LOGGING_PORT,
            socket_type=zmq.SUB,
            bind=True,
            subscription=[''],
        ),
    )
    title = "(eyrie:logger)"
    app_name = 'eyrie_logger'
    report_interval = 10
    handler_classes = (
        FernetHandler, GCMHandler, HMACHandler, ZMQHandler,
    )

    def __init__(self, **kwargs):
        self.stats = Counter()
        self.last_sample = time.time()
        self.report_interval = kwargs.pop('report_interval',
                                          self.report_interval)

        # We don't need any database access
        kwargs['init_db'] = False
        super(Scribe, self).__init__(**kwargs)
        if 'authkey' not in kwargs:
            txt = "No authkey provided; defaulting to %s"
            self.logger.info(txt, 'multiprocessing.current_process().authkey')
            kwargs['authkey'] = self.curr_proc.authkey
        if 'authkey_salt' not in kwargs:
            kwargs['authkey_salt'] = self.config.registry.settings['eyrie.authkey_salt']

        self.handlers = {}
        for klass in self.handler_classes:
            try:
                self.handlers[klass.__name__] = klass(**kwargs)
            except TypeError as err:
                self.logger.error(err.message)

        pc = PeriodicCallback(self.onReport, self.report_interval * 1000,
                              self.loop)
        # This won't actually start until the IOLoop itself is started
        # (which is done by script_main after instantiation)
        pc.start()

    def deserialize(self,
                    logger_name,
                    serialized_record, serialization_format):
        if serialization_format == 'pickle':
            return pickle.loads(serialized_record)
        elif serialization_format == 'json':
            rdata = json.loads(serialized_record)
            if 'args' in rdata and not isinstance(rdata['args'], basestring):
                rdata['args'] = tuple(rdata['args'])
            return logging.makeLogRecord(rdata)
        else:
            raise ValueError

    def handleLogRecord(self, record):
        logger = logging.getLogger(record.name)
        # N.B. EVERY record gets logged. This is because Logger.handle
        # is normally called AFTER logger-level filtering. If you want
        # to do filtering, do it at the client end to save wasting
        # cycles and network bandwidth!
        logger.handle(record)

    def onLogger(self, frames):
        self.stats['msg_count'] += 1

        try:
            msg = ZMQLogMessage(*frames)
            serialized_record = self.handlers[msg.logger_type].handle(msg)
            record = self.deserialize(msg.logger_name, serialized_record,
                                      msg.serialization_format)
        except KeyError:
            self.stats['invalid_type'] += 1
            txt = 'Incoming log message has invalid logger_type: %s'
            self.logger.error(txt, msg.logger_type)
            return
        except TypeError:
            self.stats['invalid_structure'] += 1
            txt = 'Incoming log message has wrong number of fields: %d'
            self.logger.error(txt, len(frames))
            return
        except ValueError:
            self.stats['invalid_format'] += 1
            txt = 'Incoming log message has invalid serialization_format: %s'
            self.logger.error(txt, msg.serialization_format)
            return
        except (InvalidToken, InvalidTag, InvalidSignature):
            self.stats['invalid_auth'] += 1
            txt = 'Incoming log message failed authentication: %s'
            self.logger.error(txt, msg.logger_name)
            return

        record.hostname = msg.hostname
        self.handleLogRecord(record)

    def onReport(self):
        curr_sample = time.time()
        txt = 'Current feed throughput: %.2f messages / second'
        mps = self.stats.pop('msg_count', 0) / (curr_sample - self.last_sample)
        self.logger.info(txt, mps)
        self.logger.info('Error stats: %s', self.stats)
        self.stats.clear()
        self.last_sample = curr_sample
        self.msg_count = 0


def main():
    script_main(Scribe, None)


if __name__ == "__main__":
    main()
