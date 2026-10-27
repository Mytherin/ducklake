#!/usr/bin/env python3
"""Runs the interop tests of the Iceberg REST API: python3 test/iceberg/run_tests.py [unittest arguments]"""
import os
import sys
import unittest

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        unittest.main(module=None, argv=[sys.argv[0]] + sys.argv[1:])
    else:
        suite = unittest.defaultTestLoader.discover(here, pattern='test_*.py')
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        sys.exit(0 if result.wasSuccessful() else 1)
