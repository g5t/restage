from unittest import TestCase
from unittest.mock import patch
from pathlib import Path
from importlib import reload
import restage.config


class SettingsTests(TestCase):
    import os
    @patch.dict(os.environ, {"RESTAGE_CACHE": "/tmp/some/location"})
    def test_restage_cache_config(self):
        reload(restage.config)
        from restage.config import config
        self.assertTrue(config['cache'].exists())
        self.assertEqual(config['cache'].as_path(), Path('/tmp/some/location'))

    @patch.dict(os.environ, {"RESTAGE_FIXED": "/tmp/some/location"})
    def test_restage_single_fixed_config(self):
        reload(restage.config)
        from restage.config import config
        self.assertTrue(config['fixed'].exists())
        self.assertEqual(config['fixed'].as_path(), Path('/tmp/some/location'))

    @patch.dict(os.environ, {'RESTAGE_FIXED': '/tmp/a /tmp/b /tmp/c'})
    def test_restage_multi_fixed_config(self):
        reload(restage.config)
        from restage.config import config
        self.assertTrue(config['fixed'].exists())
        more = config['fixed'].as_str_seq()
        self.assertEqual(len(more), 3)
        self.assertEqual(Path(more[0]), Path('/tmp/a'))
        self.assertEqual(Path(more[1]), Path('/tmp/b'))
        self.assertEqual(Path(more[2]), Path('/tmp/c'))
