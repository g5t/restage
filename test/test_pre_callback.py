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

    def setUp(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        self._tmp = TemporaryDirectory()
        self.output = Path(self._tmp.name).joinpath('scan')

    def tearDown(self):
        self._tmp.cleanup()


if __name__ == '__main__':
    unittest.main()
