# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
import logging
import pickle

import zmq

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.exceptions import InvalidSignature

from cs.eyrie import Vassal
from cs.eyrie import ZMQChannel
from cs.eyrie import script_main
from cs.eyrie.config import LOGGING_PORT
from cs.eyrie.config import ZMQLogMessage


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

    def __init__(self, **kwargs):
        kwargs['init_db'] = False
        super(Scribe, self).__init__(**kwargs)

    def onLogger(self, frames):
        msg = ZMQLogMessage(*frames)
        h = hmac.HMAC(self.curr_proc.authkey, hashes.SHA256(),
                      backend=default_backend())
        h.update(msg.pickled_record)

        try:
            h.verify(msg.incoming_digest)
        except (InvalidSignature,):
            self.logger.error('Incoming log message failed authentication: %s',
                              msg.logger_name)
            return

        record = pickle.loads(msg.pickled_record)
        record.hostname = msg.hostname
        self.handleLogRecord(record)

    def handleLogRecord(self, record):
        logger = logging.getLogger(record.name)
        # N.B. EVERY record gets logged. This is because Logger.handle
        # is normally called AFTER logger-level filtering. If you want
        # to do filtering, do it at the client end to save wasting
        # cycles and network bandwidth!
        logger.handle(record)


def main():
    script_main(Scribe, None)


if __name__ == "__main__":
    main()
