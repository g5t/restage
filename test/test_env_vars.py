import os
from unittest import TestCase, mock
from pathlib import Path


def restage_loaded():
    import sys
    return 'restage' in sys.modules or 'restage.config' in sys.modules


def first(test):
    import unittest
    @unittest.skipIf(restage_loaded(), reason="Environment variable patching must be done before restage is loaded")
    def first_test(*args, **kwargs):
        return test(*args, **kwargs)
    return first_test


class SettingsTests(TestCase):
    @first
    @mock.patch.dict(os.environ, {"RESTAGE_CACHE": "/tmp/some/location"})
    def test_restage_cache_config(self):
        from restage.config import config
        self.assertTrue(config['cache'].exists())
        self.assertEqual(config['cache'].as_path(), Path('/tmp/some/location'))

    @first
    @mock.patch.dict(os.environ, {"RESTAGE_FIXED": "/tmp/some/location"})
    def test_restage_single_fixed_config(self):
        from restage.config import config
        self.assertTrue(config['fixed'].exists())
        self.assertEqual(config['fixed'].as_path(), Path('/tmp/some/location'))

    @first
    @mock.patch.dict(os.environ, {'RESTAGE_FIXED': '/tmp/a /tmp/b /tmp/c'})
    def test_restage_multi_fixed_config(self):
        from restage.config import config
        self.assertTrue(config['fixed'].exists())
        more = config['fixed'].as_str_seq()
        self.assertEqual(len(more), 3)
        self.assertEqual(Path(more[0]), Path('/tmp/a'))
        self.assertEqual(Path(more[1]), Path('/tmp/b'))
        self.assertEqual(Path(more[2]), Path('/tmp/c'))
