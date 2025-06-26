import os
from unittest import TestCase
from unittest.mock import patch
from importlib import reload
import restage.config


class SettingsTests(TestCase):
    import os
    @patch.dict(os.environ, {"RESTAGE_CACHE": "/tmp/some/location"})
    def test_restage_cache_config(self):
        reload(restage.config)
        from restage.config import config
        self.assertTrue(config['cache'].exists())
        self.assertEqual(config['cache'].as_str(), '/tmp/some/location')

    @patch.dict(os.environ, {"RESTAGE_FIXED": "/tmp/some/location"})
    def test_restage_single_fixed_config(self):
        reload(restage.config)
        from restage.config import config
        self.assertTrue(config['fixed'].exists())
        self.assertEqual(config['fixed'].as_str(), '/tmp/some/location')

    @patch.dict(os.environ, {'RESTAGE_FIXED': '/tmp/a /tmp/b /tmp/c'})
    def test_restage_multi_fixed_config(self):
        reload(restage.config)
        from restage.config import config
        self.assertTrue(config['fixed'].exists())
        more = config['fixed'].as_str_seq()
        self.assertEqual(len(more), 3)
        self.assertEqual(more[0],'/tmp/a')
        self.assertEqual(more[1],'/tmp/b')
        self.assertEqual(more[2],'/tmp/c')

    @patch.dict(os.environ, {"RESTAGE_FIXED": ''})
    def test_restage_none_fixed_config(self):
        reload(restage.config)
        from restage.config import config
        from confuse.exceptions import ConfigTypeError
        self.assertTrue(config['fixed'].exists())
        self.assertTrue(config['fixed'].get() is None)
        self.assertRaises(ConfigTypeError, config['fixed'].as_str_seq)

    def test_missing_database_in_fixed_config(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        with TemporaryDirectory() as tmpdir:
            self.assertFalse((Path(tmpdir) / 'database.db').exists())
            with patch.dict(os.environ, {'RESTAGE_FIXED': tmpdir}):
                reload(restage.config)
                from restage.config import config
                self.assertTrue(config['fixed'].exists())
                self.assertEqual(config['fixed'].as_str(), tmpdir)
                from restage.cache import FILESYSTEM
                self.assertEqual(len(FILESYSTEM.db_fixed), 0)


    def test_restage_standard_config(self):
        from os import environ
        reload(restage.config)
        from restage.config import config
        if 'cache' in config:
            self.assertEqual(config['cache'].as_str(), environ['RESTAGE_CACHE'])
        if 'fixed' in config:
            self.assertEqual(config['fixed'].as_str(), environ['RESTAGE_FIXED'])

