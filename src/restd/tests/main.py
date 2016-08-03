#!/usr/bin/env python3
import argparse
import os
import unittest

from base import CRUDTestCase, SingleItemTestCase


def remove_abstract_tests(tests):
    if isinstance(tests, unittest.TestCase):
        return tests
    rv = []
    for test in tests._tests:
        # Skip abstract test cases
        if test.__class__ in (CRUDTestCase, SingleItemTestCase):
            continue
        rv.append(remove_abstract_tests(test))
    tests._tests = rv
    return tests


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--uri', required=True)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    os.environ.setdefault('URI', args.uri)

    loader = unittest.TestLoader()
    tests = loader.discover('resources')
    tests = remove_abstract_tests(tests)

    testRunner = unittest.runner.TextTestRunner(verbosity=2 if args.verbose else 1)
    testRunner.run(tests)

if __name__ == '__main__':
    main()
