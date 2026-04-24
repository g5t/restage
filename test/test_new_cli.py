"""Tests for the --progress flag and nosplitrun entrypoint."""
from __future__ import annotations

import unittest


class SplitrunProgressFlagTest(unittest.TestCase):
    """The --progress flag is present on the splitrun parser."""

    def test_progress_flag_exists(self):
        from restage.splitrun import make_splitrun_parser
        parser = make_splitrun_parser()
        # --progress should be a known optional action
        action_dests = {a.dest for a in parser._actions}
        self.assertIn('progress', action_dests)

    def test_progress_default_false(self):
        from restage.splitrun import make_splitrun_parser
        parser = make_splitrun_parser()
        args = parser.parse_args(['dummy.instr'])
        self.assertFalse(args.progress)

    def test_progress_enabled(self):
        from restage.splitrun import make_splitrun_parser
        parser = make_splitrun_parser()
        args = parser.parse_args(['dummy.instr', '--progress'])
        self.assertTrue(args.progress)

    def test_splitrun_accepts_progress_kwarg(self):
        """The splitrun() function signature includes a progress parameter."""
        import inspect
        from restage.splitrun import splitrun
        sig = inspect.signature(splitrun)
        self.assertIn('progress', sig.parameters)
        self.assertFalse(sig.parameters['progress'].default)


class NosplitrunParserTest(unittest.TestCase):
    """nosplitrun parser is derived from splitrun with compatible args."""

    def test_parser_prog_name(self):
        from restage.nosplitrun import make_nosplitrun_parser
        parser = make_nosplitrun_parser()
        self.assertEqual(parser.prog, 'nosplitrun')

    def test_inherits_splitrun_args(self):
        """nosplitrun parser should have the same core flags as splitrun."""
        from restage.nosplitrun import make_nosplitrun_parser
        parser = make_nosplitrun_parser()
        dests = {a.dest for a in parser._actions}
        for expected in ('instrument', 'parameters', 'ncount', 'mesh', 'seed',
                         'dir', 'trace', 'gravitation', 'dryrun', 'parallel',
                         'gpu', 'progress', 'split_at'):
            self.assertIn(expected, dests, f'Expected dest {expected!r} in nosplitrun parser')

    def test_progress_default_false(self):
        from restage.nosplitrun import make_nosplitrun_parser
        parser = make_nosplitrun_parser()
        args = parser.parse_args(['dummy.instr'])
        self.assertFalse(args.progress)

    def test_progress_flag(self):
        from restage.nosplitrun import make_nosplitrun_parser
        parser = make_nosplitrun_parser()
        args = parser.parse_args(['dummy.instr', '--progress'])
        self.assertTrue(args.progress)

    def test_nosplitrun_accepts_progress_kwarg(self):
        import inspect
        from restage.nosplitrun import nosplitrun
        sig = inspect.signature(nosplitrun)
        self.assertIn('progress', sig.parameters)
        self.assertFalse(sig.parameters['progress'].default)

    def test_entrypoint_is_callable(self):
        from restage.nosplitrun import entrypoint
        self.assertTrue(callable(entrypoint))


class ArgsParsDirectTest(unittest.TestCase):
    """_args_pars_direct builds the right command string."""

    def test_empty(self):
        from restage.splitrun import _args_pars_direct
        result = _args_pars_direct({}, {})
        self.assertEqual(result, '')

    def test_params_only(self):
        from restage.splitrun import _args_pars_direct
        result = _args_pars_direct({}, {'a': 1, 'b': 2})
        self.assertIn('a=1', result)
        self.assertIn('b=2', result)
        self.assertNotIn('mcpl_filename', result)


if __name__ == '__main__':
    unittest.main()
