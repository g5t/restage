"""Every number the chopper calculations depend on, in one place, with its provenance.

These used to be written several ways. The neutron h^2/2m appeared as ``81.82`` in
``bifrost_choppers.script`` and again in ``energy.py``, while ``wavelength_extremes`` used
``0.1106`` -- its inverse square root -- which implies 81.75037 instead. Since
``0.1106 * sqrt(81.82)`` is 1.000426 rather than 1, asking for a 3 angstrom band produced
settings for 2.99872 angstrom, and the two spellings disagreed with each other by more than
either disagreed with the truth.

Deriving everything from the SI primitives makes that class of disagreement impossible to
write. The expressions mirror ``chopcal``'s ``src/constants.h`` operation for operation, so
the two are bit-identical rather than merely close; ``test/test_constants.py`` asserts it.
Mirroring is the point: a transcribed literal can be mistyped, a derivation cannot quietly
disagree with the one it follows.
"""
from __future__ import annotations

# -- SI 2019 and CODATA 2022 -------------------------------------------------------------
# The first two are exact by definition of the SI; the neutron mass is measured.

#: Planck constant, J s. Exact: the SI defines the kilogram through it.
PLANCK = 6.62607015e-34
#: Elementary charge, C. Exact: the SI defines the ampere through it.
ELEMENTARY_CHARGE = 1.602176634e-19
#: Neutron rest mass, kg. CODATA 2022.
NEUTRON_MASS = 1.67492750056e-27

# -- derived, in the units the chopper calculations work in --------------------------------
# Each is the SI expression scaled once into angstrom/meV, so the value and the way it was
# reached are the same line of code.

#: h/m, angstrom m/s. A neutron of wavelength L travels ``H_OVER_M / L`` metres per second.
H_OVER_M = PLANCK / NEUTRON_MASS * 1e10

#: h^2/2m, meV angstrom^2. A neutron of wavelength L has energy ``H2_OVER_2M / (L * L)``,
#: and a neutron of energy E has wavelength ``sqrt(H2_OVER_2M / E)``.
H2_OVER_2M = PLANCK * PLANCK / (2 * NEUTRON_MASS) / (ELEMENTARY_CHARGE * 1e-3) * 1e20

# -- the ESS source ------------------------------------------------------------------------
# From mcstas-comps/share/ESS_butterfly-lib.h, which is what the simulated source itself
# uses. These calculations previously rounded the duration to 2.86e-3.

#: Length of the high-flux plateau, s. McStas: ESS_SOURCE_DURATION.
SOURCE_DURATION = 2.857e-3
#: Source repetition rate, Hz. McStas: ESS_SOURCE_FREQUENCY.
SOURCE_FREQUENCY = 14.0
#: Time from the proton pulse to the high-flux plateau, s. A BIFROST working figure rather
#: than a published one; it has no McStas counterpart.
PULSE_HIGH_FLUX_OFFSET = 2.0e-4

# -- BIFROST geometry ----------------------------------------------------------------------

#: Moderator to sample, m.
INSTRUMENT_LENGTH = 162.0
#: Moderator to the first pulse-shaping disk, m. Left as its four surveyed parts.
PULSE_SHAPING_DISTANCE = 4.41 + 0.032 + 2.0 - 0.1
#: Moderator to the first and second frame-overlap disks, m.
FRAME_OVERLAP_1_DISTANCE = 8.530
FRAME_OVERLAP_2_DISTANCE = 14.973
#: Moderator to the first bandwidth disk, m. Measured from the moderator, not from the
#: pulse-shaping pair -- the name this used to carry said otherwise and the code did this.
BANDWIDTH_DISTANCE = 78.0

#: Opening angle of a pulse-shaping disk, degrees.
PULSE_SHAPING_ANGLE = 170.0

#: Degrees in a turn -- a unit conversion, not a measurement.
DEGREES_PER_TURN = 360.0
