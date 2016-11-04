# -*- coding: utf-8 -*-
import os

from unittest.loader import defaultTestLoader


def test_collector():
    curr_dir = os.path.abspath(os.path.dirname(__file__))
    suite = defaultTestLoader.discover(curr_dir)
    return suite
