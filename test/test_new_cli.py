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



class NosplitrunEnergyParametersTest(unittest.TestCase):
    """nosplitrun turns a point's ei= and t= into chopper settings, as splitrun does.

    It used to translate them and then copy the raw command-line values back in, which a
    BIFROST that declares only its chopper knobs refused: 'Parameter ei is not a valid
    parameter name'. A scanned value would have come back as the whole scan, too.
    """
    CHOPPERS = tuple(f'{name}_chopper_{no}' for name in ('pulse_shaping', 'frame_overlap', 'bandwidth')
                     for no in (1, 2))
    KNOBS = tuple(f'{c}{k}' for c in CHOPPERS for k in ('_rotation_speed', '_delay'))

    def instrument(self, *extra):
        from mccode_antlr.loader import parse_mcstas_instr
        declared = ', '.join(f'{n}=0' for n in (*self.KNOBS, 'sample_rotation', *extra))
        return parse_mcstas_instr(f"""DEFINE INSTRUMENT bifrost_nosplitrun_test({declared})
            TRACE
            COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
            END""")

    def point(self, instr, point):
        from restage.energy import (energy_to_chopper_translator, get_energy_parameter_names,
                                    declared_parameter_names)
        from restage.instr import collect_parameter_dict
        from restage.nosplitrun import point_parameters
        translate = energy_to_chopper_translator(instr.name, declared_parameter_names(instr))
        pars = point_parameters(instr, translate, get_energy_parameter_names(instr.name), point)
        return collect_parameter_dict(instr, pars)

    def test_energy_parameters_become_chopper_settings(self):
        pars = self.point(self.instrument(), {'ei': 4.5, 't': 1e-4, 'sample_rotation': 3.0})
        self.assertNotIn('ei', pars)
        self.assertNotIn('t', pars)
        self.assertEqual(pars['sample_rotation'].value, 3.0)
        self.assertEqual(pars['pulse_shaping_chopper_1_rotation_speed'].value, 196.0)
        self.assertNotEqual(pars['bandwidth_chopper_1_delay'].value, 0)

    def test_an_energy_parameter_the_instrument_declares_is_passed_on(self):
        pars = self.point(self.instrument('ei'), {'ei': 4.5, 't': 1e-4})
        self.assertEqual(pars['ei'].value, 4.5)
        self.assertEqual(pars['pulse_shaping_chopper_1_rotation_speed'].value, 196.0)

    def test_the_point_is_left_as_it_was(self):
        point = {'ei': 4.5, 't': 1e-4, 'sample_rotation': 3.0}
        self.point(self.instrument(), point)
        self.assertEqual(point, {'ei': 4.5, 't': 1e-4, 'sample_rotation': 3.0})


if __name__ == '__main__':
    unittest.main()
