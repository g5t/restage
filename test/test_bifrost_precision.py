"""Tests verifying that BIFROST chopper settings are correctly distinguished
by the 0.01 % default parameter precision (value / 10000).

Background
----------
``SimulationEntry.__post_init__`` sets an absolute tolerance of
``abs(value / 10000)`` (0.01 %) for each numeric parameter when no explicit
precision dict is supplied.  The old behaviour used ``value / 100`` (1 %).

Two parameter dimensions are exercised:

* **Minimum incident energy** — 2.00 to 20.00 meV in 0.01 meV steps (1 800
  adjacent pairs).  All phases change by 0.025–0.29 % per step: detectable
  at 0.01 % but *not* at 1 %.

* **Pulse-shaping chopper opening time** — 0.10 to 3.00 ms in 0.01 ms steps
  (290 adjacent pairs).  PS phases change by 0.043–0.91 % per step; below
  ~2.41 ms the chopper speeds are constant so the PS phases alone carry the
  distinction.  At 2.41 ms / 2.60 ms / 2.82 ms the frequency order is
  stepped down, causing a large speed jump that is detectable at any
  precision.  With 1 % tolerance, only pairs in the constant-speed region
  (t < 2.41 ms) are mistakenly treated as cache hits.

Frame-overlap (fo1/fo2) and bandwidth (bw1/bw2) chopper phases depend only
on energy, not on opening time, so they contribute to energy-sweep
distinction but not to time-sweep distinction.
"""
from __future__ import annotations

import io
import contextlib
import unittest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NAMES = ('ps1', 'ps2', 'fo1', 'fo2', 'bw1', 'bw2')
_ORDER = 14
# 2 ms is comfortably below the ~2.41 ms frequency-order-reduction threshold
# (360 * 0.002 * 14 * 14 = 141 < 170 deg) so no stdout noise during setup.
_FIXED_TIME = 0.002   # s  — used for energy sweep
_FIXED_ENERGY = 5.0   # meV — used for time sweep


def _calc(order: int, time: float, energy: float) -> dict[str, float]:
    """Compute chopper settings, suppressing the 'order reduced' print."""
    from restage.bifrost_choppers import calculate
    with contextlib.redirect_stdout(io.StringIO()):
        return calculate(order, time, energy, _NAMES)


def _entry(params: dict[str, float]):
    """``SimulationEntry`` with default 0.01 % precision."""
    from mccode_antlr.common import Expr
    from restage.tables import SimulationEntry
    return SimulationEntry({k: Expr.best(v) for k, v in params.items()})


def _entry_1pct(params: dict[str, float]):
    """``SimulationEntry`` with 1 % absolute tolerance (old behaviour)."""
    from mccode_antlr.common import Expr
    from restage.tables import SimulationEntry
    precision = {k: abs(v) / 100.0 for k, v in params.items() if v != 0.0}
    return SimulationEntry(
        {k: Expr.best(v) for k, v in params.items()},
        precision=precision,
    )


# ---------------------------------------------------------------------------
# Energy sweep
# ---------------------------------------------------------------------------

class BifrostEnergyPrecisionTestCase(unittest.TestCase):
    """Adjacent 0.01 meV energy steps must be distinguishable."""

    @classmethod
    def setUpClass(cls):
        cls.energies = [round(2.0 + i * 0.01, 6) for i in range(1801)]
        cls.settings = [_calc(_ORDER, _FIXED_TIME, e) for e in cls.energies]

    def test_all_adjacent_energies_distinct_default_precision(self):
        """0.01 % precision distinguishes all 1 800 adjacent 0.01 meV steps.

        All chopper phases change by at least 0.025 % per step, which exceeds
        the abs(value / 10 000) tolerance for each parameter.
        """
        for i in range(len(self.settings) - 1):
            entry = _entry(self.settings[i])
            candidate = _entry(self.settings[i + 1])
            self.assertFalse(
                entry.matches_candidate(candidate),
                f'Settings at {self.energies[i]:.2f} meV and '
                f'{self.energies[i + 1]:.2f} meV were incorrectly matched '
                f'at default (0.01 %) precision',
            )

    def test_all_adjacent_energies_indistinct_one_percent_precision(self):
        """1 % precision fails to distinguish *any* adjacent 0.01 meV step.

        All chopper phases change by less than 1 % per step, so every
        adjacent energy pair is a false cache hit under the old tolerance.
        """
        for i in range(len(self.settings) - 1):
            entry = _entry_1pct(self.settings[i])
            candidate = _entry(self.settings[i + 1])
            self.assertTrue(
                entry.matches_candidate(candidate),
                f'Settings at {self.energies[i]:.2f} meV and '
                f'{self.energies[i + 1]:.2f} meV should be indistinguishable '
                f'at 1 % precision but were incorrectly separated',
            )


# ---------------------------------------------------------------------------
# Opening-time sweep
# ---------------------------------------------------------------------------

class BifrostTimePrecisionTestCase(unittest.TestCase):
    """Adjacent 0.01 ms opening-time steps must be distinguishable."""

    @classmethod
    def setUpClass(cls):
        # 0.10 ms to 3.00 ms in 0.01 ms steps → 291 points, 290 pairs
        cls.times = [round(0.0001 + i * 0.00001, 7) for i in range(291)]
        cls.times_ms = [t * 1000 for t in cls.times]
        cls.settings = [_calc(_ORDER, t, _FIXED_ENERGY) for t in cls.times]

        # Pairs where the PS speed is unchanged between adjacent steps (i.e. the
        # frequency order has not yet been reduced) — these are the "hard" cases
        # where only the phases distinguish entries.
        cls.constant_speed_pairs = [
            i for i in range(len(cls.settings) - 1)
            if cls.settings[i]['ps1speed'] == cls.settings[i + 1]['ps1speed']
        ]

    def test_all_adjacent_times_distinct_default_precision(self):
        """0.01 % precision distinguishes all 290 adjacent 0.01 ms steps.

        Below 2.41 ms PS phases change by ≥ 0.043 % per step; above that
        threshold the chopper frequency order is stepped down, causing a
        large speed jump (≥ 7 %) that is trivially distinct.
        """
        for i in range(len(self.settings) - 1):
            entry = _entry(self.settings[i])
            candidate = _entry(self.settings[i + 1])
            self.assertFalse(
                entry.matches_candidate(candidate),
                f'Settings at t={self.times_ms[i]:.3f} ms and '
                f't={self.times_ms[i + 1]:.3f} ms were incorrectly matched '
                f'at default (0.01 %) precision',
            )

    def test_constant_speed_adjacent_times_indistinct_one_percent_precision(self):
        """1 % precision cannot distinguish adjacent 0.01 ms steps below 2.41 ms.

        In the constant-speed region (t < 2.41 ms) FO and BW phases do not
        change with time, and PS phases change by less than 0.1 % per step —
        well within the 1 % tolerance, giving false cache hits for every pair.
        """
        pairs = self.constant_speed_pairs
        self.assertGreater(len(pairs), 0, 'No constant-speed pairs found — check time range')
        for i in pairs:
            entry = _entry_1pct(self.settings[i])
            candidate = _entry(self.settings[i + 1])
            self.assertTrue(
                entry.matches_candidate(candidate),
                f'Settings at t={self.times_ms[i]:.3f} ms and '
                f't={self.times_ms[i + 1]:.3f} ms should be indistinguishable '
                f'at 1 % precision but were incorrectly separated',
            )

    def test_speed_transition_pairs_distinct_at_any_precision(self):
        """At the frequency-order step-down points the speed jump (≥ 7 %)
        makes adjacent pairs distinguishable even at 1 % precision."""
        # Locate pairs where PS speed changes
        transition_indices = [
            i for i in range(len(self.settings) - 1)
            if self.settings[i]['ps1speed'] != self.settings[i + 1]['ps1speed']
        ]
        self.assertGreater(
            len(transition_indices), 0,
            'Expected at least one speed-transition pair in 0.1–3.0 ms range',
        )
        for i in transition_indices:
            entry = _entry_1pct(self.settings[i])
            candidate = _entry(self.settings[i + 1])
            self.assertFalse(
                entry.matches_candidate(candidate),
                f'Speed-transition pair at t={self.times_ms[i]:.3f} ms should '
                f'be distinct even at 1 % precision',
            )
