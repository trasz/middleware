#!/usr/bin/env python3
import unittest

from base import CRUDTestCase


def remove_abstract_tests(tests):
    if isinstance(tests, unittest.TestCase):
        return tests
    rv = []
    for test in tests._tests:
        # Skip CRUDTestCase
        if test.__class__ is CRUDTestCase:
            continue
        rv.append(remove_abstract_tests(test))
    tests._tests = rv
    return tests


def main():
    loader = unittest.TestLoader()
    tests = loader.discover('resources')
    tests = remove_abstract_tests(tests)

    testRunner = unittest.runner.TextTestRunner()
    testRunner.run(tests)

if __name__ == '__main__':
    main()
