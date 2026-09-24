"""The per-point hooks: what they are handed, and when they fire."""
from __future__ import annotations

import unittest


class InvokeCallbackTest(unittest.TestCase):
    """`_invoke_callback` is the one place the hook arguments are assembled."""

    def setUp(self):
        self.seen = []

    def record(self, **kwargs):
        self.seen.append(kwargs)

    def invoke(self, mapping):
        from restage.splitrun import _invoke_callback
        _invoke_callback(self.record, mapping, ['a', 'b'], (1, 2), 3, 7,
                         {'a': 1, 'b': 2}, 'somewhere', {'ncount': 10})

    def test_no_callback_is_not_an_error(self):
        from restage.splitrun import _invoke_callback
        _invoke_callback(None, {'number': 'point'}, ['a'], (1,), 0, 1, {}, '.', {})
        self.assertEqual(self.seen, [])

    def test_the_mapping_renames(self):
        self.invoke({'number': 'point', 'dir': 'root'})
        self.assertEqual(self.seen, [{'point': 3, 'root': 'somewhere'}])

    def test_the_mapping_also_filters(self):
        """Every internal name is available, but only the asked-for ones are passed."""
        self.invoke({'a': 'a'})
        self.assertEqual(self.seen, [{'a': 1}])

    def test_scan_parameters_are_available_by_name(self):
        self.invoke({'a': 'a', 'b': 'b', 'pars': 'pars', 'n_pts': 'n_pts',
                     'arguments': 'arguments'})
        self.assertEqual(self.seen, [{'a': 1, 'b': 2, 'pars': {'a': 1, 'b': 2},
                                      'n_pts': 7, 'arguments': {'ncount': 10}}])

    def test_no_mapping_means_no_arguments(self):
        """Not a mistake: a hook that wants nothing is a legitimate thing to register."""
        self.invoke(None)
        self.assertEqual(self.seen, [{}])


class HookSignatureTest(unittest.TestCase):
    """Both entry points accept the pre-point hook."""

    def test_splitrun_accepts_pre_callback(self):
        import inspect
        from restage.splitrun import splitrun
        sig = inspect.signature(splitrun)
        for name in ('pre_callback', 'pre_callback_arguments'):
            self.assertIn(name, sig.parameters)
            self.assertIsNone(sig.parameters[name].default)

    def test_nosplitrun_accepts_pre_callback(self):
        import inspect
        from restage.nosplitrun import nosplitrun
        sig = inspect.signature(nosplitrun)
        for name in ('pre_callback', 'pre_callback_arguments'):
            self.assertIn(name, sig.parameters)
            self.assertIsNone(sig.parameters[name].default)


class _FakeInstr:
    """Enough of an Instr for `splitrun_combined` to partition parameters."""
    def __init__(self, name):
        self.name = name

    def has_parameter(self, name):
        return True


class HookOrderTest(unittest.TestCase):
    """The pre-point hook fires before its point is simulated; the other one after.

    Simulating for real needs a compiler and MCPL, so the point loop is run with its
    simulation, cache and summary collaborators replaced. What is being checked is the
    order of three calls, which is exactly what the substitutes can still observe.
    """

    def test_pre_fires_before_the_simulation_and_post_after(self):
        from unittest.mock import patch
        from mccode_antlr.run.range import parse_scan_parameters
        from restage import splitrun as module

        order = []
        pre = lambda point: order.append(('pre', point))
        post = lambda point: order.append(('post', point))
        mapping = {'number': 'point'}

        def simulate(*args, **kwargs):
            order.append(('run', kwargs.get('_point')))

        instr = _FakeInstr('fake')
        parameters = parse_scan_parameters(['a=1:3'])

        with (patch.object(module, 'do_secondary_simulation', simulate),
              patch.object(module, 'SimulationEntry', lambda *a, **k: None),
              patch('restage.cache.cache_get_simulation', lambda *a: []),
              patch('restage.tables.best_simulation_entry_match', lambda *a: None),
              patch('restage.instr.collect_parameter_dict', lambda *a, **k: {}),
              patch('restage.energy.energy_to_chopper_translator', lambda name: dict)):
            module.splitrun_combined(
                None, None, instr, instr, parameters, {}, False, {},
                summary=False, dry_run=True,
                callback=post, callback_arguments=mapping,
                pre_callback=pre, pre_callback_arguments=mapping,
                dir=self.output,
            )

        self.assertEqual(order, [('pre', 0), ('run', None), ('post', 0),
                                 ('pre', 1), ('run', None), ('post', 1),
                                 ('pre', 2), ('run', None), ('post', 2)])

    def test_a_string_output_directory_is_accepted(self):
        """`splitrun -d DIR` hands the directory over as a str; each point's is joined on it."""
        from pathlib import Path
        from unittest.mock import patch
        from mccode_antlr.run.range import parse_scan_parameters
        from restage import splitrun as module

        point_dirs = []

        def simulate(sim_entry, post_entry, pars, runtime_arguments, **kwargs):
            point_dirs.append(runtime_arguments['dir'])

        instr = _FakeInstr('fake')
        with (patch.object(module, 'do_secondary_simulation', simulate),
              patch.object(module, 'SimulationEntry', lambda *a, **k: None),
              patch('restage.cache.cache_get_simulation', lambda *a: []),
              patch('restage.tables.best_simulation_entry_match', lambda *a: None),
              patch('restage.instr.collect_parameter_dict', lambda *a, **k: {}),
              patch('restage.energy.energy_to_chopper_translator', lambda name: dict)):
            module.splitrun_combined(
                None, None, instr, instr, parse_scan_parameters(['a=1:2']), {}, False, {},
                summary=False, dry_run=True, dir=str(self.output),
            )

        self.assertEqual(point_dirs, [self.output / '0', self.output / '1'])
        self.assertTrue(self.output.is_dir())

    def setUp(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        self._tmp = TemporaryDirectory()
        self.output = Path(self._tmp.name).joinpath('scan')

    def tearDown(self):
        self._tmp.cleanup()


if __name__ == '__main__':
    unittest.main()


class DroppedParameterWarningTest(unittest.TestCase):
    """The secondary stage filters silently; say what it dropped.

    Unlike the primary stage, which hands the unfiltered dict to a strict
    `collect_parameter_dict`, `splitrun_combined` keeps only what a half declares and drops
    the rest without a word. A chopper setting lost there leaves the disk at its default,
    which reads as a working scan answering a different question.
    """

    class _Half:
        def __init__(self, names):
            self._names = names
            self.parameters = [type('P', (), {'name': n})() for n in names]

        def has_parameter(self, name):
            return name in self._names

    def warnings_from(self, pre, post, pars, warned=None):
        from unittest.mock import patch
        from restage.splitrun import _warn_dropped_parameters
        with patch('zenlog.log.warning') as logged:
            _warn_dropped_parameters(pre, post, pars, warned if warned is not None else set())
        return [c.args[0] for c in logged.call_args_list]

    def test_a_dropped_parameter_is_named(self):
        pre, post = self._Half({'a'}), self._Half({'b'})
        said = self.warnings_from(pre, post, {'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(len(said), 1)
        self.assertIn('c', said[0])

    def test_nothing_is_said_when_every_parameter_lands(self):
        pre, post = self._Half({'a'}), self._Half({'b'})
        self.assertEqual(self.warnings_from(pre, post, {'a': 1, 'b': 2}), [])

    def test_a_name_is_reported_once_not_once_per_point(self):
        """A scan is thousands of points and drops the same name at every one."""
        pre, post = self._Half({'a'}), self._Half(set())
        warned = set()
        first = self.warnings_from(pre, post, {'a': 1, 'c': 3}, warned)
        second = self.warnings_from(pre, post, {'a': 1, 'c': 3}, warned)
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])

    def test_the_delay_rename_is_diagnosed_rather_than_just_reported(self):
        pre = self._Half({'psc1speed', 'psc1phase'})
        post = self._Half(set())
        said = self.warnings_from(pre, post, {'psc1speed': 14.0, 'psc1delay': 0.005})
        self.assertEqual(len(said), 1)
        self.assertIn('psc1delay', said[0])
        self.assertIn('Rename the instrument', said[0])
