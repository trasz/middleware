#!/usr/bin/env python3
import argparse
import os
import unittest

from base import CRUDTestCase, SingleItemTestCase


def filter_tests(tests, only=None, skip=None, skip_class=None):
    if isinstance(tests, unittest.TestCase):
        return tests
    rv = []
    for test in tests._tests:
        # Skip abstract test cases
        if test.__class__ in (CRUDTestCase, SingleItemTestCase):
            continue
        if isinstance(test, unittest.TestCase):
            if only and test.__module__ not in only:
                continue
            if skip and test.__module__ in skip:
                continue
            if skip_class and test.__class__.__name__ in skip_class:
                continue
        rv.append(filter_tests(test, only=only, skip=skip, skip_class=skip_class))
    tests._tests = rv
    return tests


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--uri', required=True)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-s', '--skip', action='append')
    parser.add_argument('-sc', '--skip-class', action='append')
    parser.add_argument('-t', '--test', action='append')
    args = parser.parse_args()

    os.environ.setdefault('URI', args.uri)

    loader = unittest.TestLoader()
    tests = loader.discover('resources')
    tests = filter_tests(tests, only=args.test, skip=args.skip, skip_class=args.skip_class)

    testRunner = unittest.runner.TextTestRunner(verbosity=2 if args.verbose else 1)
    testRunner.run(tests)

if __name__ == '__main__':
    main()
