# -*- coding: utf-8 -*-
# Copyright (C) 2014 CrowdStrike, Inc. and contributors
# This file is subject to the terms and conditions of the BSD License.
# See the file LICENSE in the main directory for details
__author__ = 'CrowdStrike, Inc.'
__email__ = 'csoc@crowdstrike.com'
__version__ = '0.1.0'

import sys
if '__pypy__' in sys.builtin_module_names:
    try:
        from psycopg2cffi import compat
    except ImportError:
        pass
    else:
        compat.register()

from cs.eyrie.config import ZMQChannel
from cs.eyrie.config import script_main
from cs.eyrie.vassal import Vassal
from cs.eyrie.vassal import BatchVassal


def main(global_config, **settings):
    pass
