#!/usr/bin/env python3
import argparse
import os
import unittest

from base import CRUDTestCase, SingleItemTestCase


def filter_tests(tests, skip=None):
    if isinstance(tests, unittest.TestCase):
        return tests
    rv = []
    for test in tests._tests:
        # Skip abstract test cases
        if test.__class__ in (CRUDTestCase, SingleItemTestCase):
            continue
        if isinstance(test, unittest.TestCase):
            if skip and test.__module__ in skip:
                continue
        rv.append(filter_tests(test, skip=skip))
    tests._tests = rv
    return tests


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--uri', required=True)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-s', '--skip', action='append')
    args = parser.parse_args()

    os.environ.setdefault('URI', args.uri)

    loader = unittest.TestLoader()
    tests = loader.discover('resources')
    tests = filter_tests(tests, skip=args.skip)

    testRunner = unittest.runner.TextTestRunner(verbosity=2 if args.verbose else 1)
    testRunner.run(tests)

if __name__ == '__main__':
    main()
