# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
import logging
import random

from cs.eyrie import Vassal
from cs.eyrie import script_main


class Lawyer(Vassal):
    channels = dict(
        # Since the purpose of this is just to inject logging calls,
        # there are no additional ZMQ channels
        # (the logging handler sets up it's own)
        Vassal.channels,
    )
    title = "(eyrie:injector)"
    app_name = 'injector'
    levels = (
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.ERROR,
        logging.CRITICAL,
        # Used to trigger an exception
        #'invalid',
    )

    def __init__(self, **kwargs):
        # We don't need any database access
        kwargs['init_db'] = False
        super(Lawyer, self).__init__(**kwargs)
        self.loop.add_callback(self.onConfig)

    def onConfig(self):
        import this
        self.position = 0
        self.text = [
            line
            for line in this.s.decode('rot13').splitlines()
            if line
        ]
        self.loop.add_callback(self.onInject)

    def onInject(self):
        try:
            txt = self.text[self.position]
            self.logger.log(random.choice(self.levels), txt)
        except TypeError:
            self.logger.exception('Error encountered')
        finally:
            self.position += 1
            if self.position > len(self.text):
                self.position = 0
            self.loop.add_callback(self.onInject)


def main():
    script_main(Lawyer, None)


if __name__ == "__main__":
    main()
