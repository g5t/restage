"""The chopper constants, against chopcal.

restage computes BIFROST chopper settings in Python; chopcal computes them in C++. Keeping
two implementations honest needs an oracle, and this is the first half of it: every constant
the calculations rest on is derived from the same SI primitives in the same order, so the
two agree to the last bit rather than to three decimal places. `test_energy` is the second
half, comparing the settings themselves.

These are constant expressions with no runtime arithmetic behind them, so exact equality is
the right assertion -- and it is what catches a transcription slip on the day it is written.
"""
from __future__ import annotations

import unittest

from restage import constants


class DerivedConstantTestCase(unittest.TestCase):
    """The derivation, checked against the one it mirrors."""

    def setUp(self):
        try:
            from chopcal import constants as theirs
        except ImportError as error:      # pragma: no cover - chopcal is a test extra
            self.skipTest(f'chopcal is not installed ({error})')
        self.theirs = theirs

    def test_the_si_primitives_are_the_same_numbers(self):
        for name in ('PLANCK', 'ELEMENTARY_CHARGE', 'NEUTRON_MASS'):
            self.assertEqual(getattr(constants, name), getattr(self.theirs, name), name)

    def test_the_derived_neutron_quantities_are_bit_identical(self):
        """Not close: derived from the same primitives in the same operation order."""
        for name in ('H_OVER_M', 'H2_OVER_2M'):
            self.assertEqual(getattr(constants, name), getattr(self.theirs, name), name)

    def test_the_source_and_geometry_agree(self):
        for name in ('SOURCE_DURATION', 'SOURCE_FREQUENCY', 'PULSE_HIGH_FLUX_OFFSET',
                     'INSTRUMENT_LENGTH', 'PULSE_SHAPING_DISTANCE',
                     'FRAME_OVERLAP_1_DISTANCE', 'FRAME_OVERLAP_2_DISTANCE',
                     'BANDWIDTH_DISTANCE', 'PULSE_SHAPING_ANGLE', 'DEGREES_PER_TURN'):
            self.assertEqual(getattr(constants, name), getattr(self.theirs, name), name)


class SelfConsistencyTestCase(unittest.TestCase):
    """One constant, one value -- which is the whole point of deriving them."""

    def test_energy_and_wavelength_round_trip(self):
        """The failure this replaces: h^2/2m was written 81.82 in one place and as 0.1106
        -- its inverse square root, implying 81.75037 -- in another, so this round trip lost
        0.043 % and a 3 angstrom request produced settings for 2.99872 angstrom.
        """
        from math import sqrt
        for energy in (0.75, 2.0, 3.5, 5.0, 25.0, 100.0):
            wavelength = sqrt(constants.H2_OVER_2M / energy)
            self.assertAlmostEqual(constants.H2_OVER_2M / wavelength / wavelength,
                                   energy, places=12, msg=f'{energy} meV')

    def test_the_old_literals_are_gone(self):
        """A regression guard with a name.

        Each of these was a tabulated stand-in for something now derived: 81.82 and 0.1106
        for h^2/2m -- disagreeing with each other -- 0.0002528 and 3956.0 for h/m, and
        0.00286 for the source duration. Any of them reappearing as a *number* means the
        module has gone back to tabulating what it derives. Read from the parsed source
        rather than the text, so the prose above is free to name them.
        """
        import ast
        from pathlib import Path
        import restage
        retired = {81.82, 0.1106, 0.0002528, 3956.0, 0.00286}
        source = Path(restage.__file__).parent
        for name in ('constants.py', 'bifrost_choppers.py', 'energy.py'):
            tree = ast.parse((source / name).read_text())
            found = {node.value for node in ast.walk(tree)
                     if isinstance(node, ast.Constant) and isinstance(node.value, float)}
            self.assertEqual(found & retired, set(), f'retired literal in {name}')


if __name__ == '__main__':
    unittest.main()
