"""Instruments that find components through SEARCH, which restage cannot follow.

mccode-antlr resolves SEARCH while reading a .instr file but keeps no record of it on the
instrument, so restage -- compiling the instrument it was handed, or its split halves --
cannot find those components again. The failure used to surface deep in translation as
"lib-readout.h not found in registries: libc,mcstas"; restage now says why, and what to do.
"""
from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

INSTR = """\
/* SEARCH "commented-out" is not a statement */
DEFINE INSTRUMENT searching()
TRACE
// SEARCH SHELL "neither is this"
{search}
COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
END
"""


class SearchStatementsTest(unittest.TestCase):

    def write(self, search):
        path = Path(self._tmp.name) / 'searching.instr'
        path.write_text(INSTR.format(search=search))
        return path

    def setUp(self):
        self._tmp = TemporaryDirectory()

    def tearDown(self):
        self._tmp.cleanup()

    def test_both_forms_are_found_and_comments_are_not(self):
        from restage.instr import search_statements
        path = self.write('SEARCH "."\nSEARCH SHELL "echo ."')
        self.assertEqual(search_statements(path), ['SEARCH "."', 'SEARCH SHELL "echo ."'])

    def test_loading_a_searching_instr_warns(self):
        from zenlog import log
        from restage.instr import load_instr
        warnings = []
        with patch.object(log, 'warning', warnings.append):
            load_instr(self.write('SEARCH "."'))
        self.assertEqual(len(warnings), 1)
        self.assertIn('SEARCH', warnings[0])
        self.assertIn('save_json', warnings[0])

    def test_loading_without_search_is_quiet(self):
        from zenlog import log
        from restage.instr import load_instr
        warnings = []
        with patch.object(log, 'warning', warnings.append):
            load_instr(self.write(''))
        self.assertEqual(warnings, [])


class CompileHintTest(unittest.TestCase):
    """A component missing from the registries is explained; other failures are not."""

    def compile_with(self, error):
        from restage import cache
        instr = SimpleNamespace(name='split_second',
                                registries=[SimpleNamespace(name='libc'), SimpleNamespace(name='mcstas')])
        entry = SimpleNamespace()

        def fail(*args, **kwargs):
            raise error

        with TemporaryDirectory() as tmp, \
                patch('mccode_antlr.compiler.c.compile_instrument', fail), \
                patch.object(cache, 'directory_under_module_data_path', lambda *a, **k: Path(tmp)):
            cache._compile_instr(entry, instr)

    def test_a_missing_component_says_why(self):
        missing = RuntimeError('lib-readout.h not found in registries: libc,mcstas')
        with self.assertRaises(RuntimeError) as raised:
            self.compile_with(missing)
        message = str(raised.exception)
        self.assertIn('lib-readout.h not found', message)
        self.assertIn('libc, mcstas', message)
        self.assertIn('SEARCH', message)
        self.assertIs(raised.exception.__cause__, missing)

    def test_other_failures_are_not_touched(self):
        other = RuntimeError('Compilation failed')
        with self.assertRaises(RuntimeError) as raised:
            self.compile_with(other)
        self.assertIs(raised.exception, other)


if __name__ == '__main__':
    unittest.main()
