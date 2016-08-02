from base import CRUDTestCase, SingleItemTestCase


class BootPoolTestCase(SingleItemTestCase):
    name = 'boot/pool'


class BootEnvironmentTestCase(CRUDTestCase):
    name = 'boot/environment'
