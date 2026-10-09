"""`--pools`: secondary points simulated several at a time."""
from __future__ import annotations

import threading
import time
import unittest
from unittest import mock


class SimulatePooledTest(unittest.TestCase):
    def run_pooled(self, points, pools):
        from restage import splitrun
        lock = threading.Lock()
        state = {'running': 0, 'most': 0, 'done': [], 'capture': set()}

        def fake(sim_entry, post_entry, pars, point_arguments, dry_run, process_count,
                 capture_output):
            with lock:
                state['running'] += 1
                state['most'] = max(state['most'], state['running'])
                state['capture'].add(capture_output)
            time.sleep(0.05)
            with lock:
                state['running'] -= 1
                state['done'].append(point_arguments['dir'])

        pooled = [({'dir': n}, None, {}, (n,)) for n in range(points)]
        with mock.patch.object(splitrun, 'do_secondary_simulation', fake):
            splitrun._simulate_pooled(pooled, None, pools, False, 2, False)
        return state

    def test_every_point_runs_at_most_pools_at_once(self):
        state = self.run_pooled(points=9, pools=3)
        self.assertEqual(sorted(state['done']), list(range(9)))
        self.assertEqual(state['most'], 3)
        self.assertEqual(state['capture'], {True})

    def test_a_failure_raises_and_cancels_the_queued_points(self):
        from restage import splitrun
        started = []

        def fake(sim_entry, post_entry, pars, point_arguments, **kwargs):
            started.append(point_arguments['dir'])
            time.sleep(0.05)
            if point_arguments['dir'] == 0:
                raise RuntimeError('simulation failed')

        pooled = [({'dir': n}, None, {}, (n,)) for n in range(20)]
        with mock.patch.object(splitrun, 'do_secondary_simulation', fake):
            with self.assertRaises(RuntimeError):
                splitrun._simulate_pooled(pooled, None, 2, False, 2, False)
        # which points had started depends on scheduling, but not all 20
        self.assertLess(len(started), 20)


class PoolsOptionTest(unittest.TestCase):
    def test_default_is_one(self):
        from restage.splitrun import make_splitrun_parser
        args = make_splitrun_parser().parse_args(['x.instr'])
        self.assertEqual(args.pools, 1)

    def test_splitrun_accepts_pools(self):
        import inspect
        from restage.splitrun import splitrun, splitrun_combined
        for f in (splitrun, splitrun_combined):
            self.assertEqual(inspect.signature(f).parameters['pools'].default, 1)


if __name__ == '__main__':
    unittest.main()
