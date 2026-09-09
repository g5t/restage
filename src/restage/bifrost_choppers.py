"""BIFROST chopper settings, as speeds in hertz and delays in seconds.

A *delay* is when a disk's reference point passes its pickup, measured from the source
reference time. A *phase*, which this module used to return, is that same instant expressed
as an angle -- and McStas divides it by ``fabs(omega)``, so the same phase means two
different things depending on which way a disk turns. A delay does not care, and is what a
real chopper controller measures and holds with a PID loop. ``chopcal`` switched for that
reason at v0.5.0, and every instrument model this drives has followed.

The formulas are the same ones ``chopcal`` compiles, transcribed operation for operation --
including where the additions sit, since floating-point addition is not associative and the
cross-check in ``test/test_energy.py`` asserts equality rather than nearness. The constants
they use are derived in :mod:`restage.constants` from the same SI primitives, so the two
implementations agree to the last bit rather than to three decimal places.
"""
from __future__ import annotations

import warnings

from .constants import (
    BANDWIDTH_DISTANCE,
    DEGREES_PER_TURN,
    FRAME_OVERLAP_1_DISTANCE,
    FRAME_OVERLAP_2_DISTANCE,
    H2_OVER_2M,
    H_OVER_M,
    INSTRUMENT_LENGTH,
    PULSE_HIGH_FLUX_OFFSET,
    PULSE_SHAPING_ANGLE,
    PULSE_SHAPING_DISTANCE,
    SOURCE_DURATION,
    SOURCE_FREQUENCY,
)

#: How long one pulse-shaping slit takes to cross the beam at the source frequency. A burst
#: cannot be longer than this, which is what bounds the frequency order.
SLIT_CROSSING = PULSE_SHAPING_ANGLE / DEGREES_PER_TURN / SOURCE_FREQUENCY


def band_extremes(energy_minimum: float):
    """The band's wavelengths and velocities: ``(lambda_0, lambda_1, v_0, v_1)``.

    ``lambda_1`` is the slowest neutron asked for and ``lambda_0`` the fastest, one band
    width shorter; ``v_0`` and ``v_1`` are their speeds, so ``v_0`` is the *slower* of the
    two. The naming is chopcal's, kept so the two can be read side by side.
    """
    from math import sqrt
    lambda_1 = sqrt(H2_OVER_2M / energy_minimum)
    band = H_OVER_M / (INSTRUMENT_LENGTH - PULSE_SHAPING_DISTANCE) / SOURCE_FREQUENCY
    lambda_0 = lambda_1 - band
    return lambda_0, lambda_1, H_OVER_M / lambda_1, H_OVER_M / lambda_0


def wavelength_extremes(energy_minimum: float):
    """The shortest and longest wavelength the chopper train is set to pass, in angstrom."""
    lambda_0, lambda_1, _, _ = band_extremes(energy_minimum)
    return lambda_0, lambda_1


def reduce_frequency_order(frequency_order: float, opening_time: float) -> float:
    """The largest order that can actually produce this burst.

    The pulse-shaping pair co-rotate with one lagging the other, so the burst is one slit
    crossing less the lag -- and a crossing gets shorter the faster they turn. Asking for a
    burst longer than a crossing at the requested order is not a small error to round away,
    it is unachievable, so the order comes down until it is achievable and the caller is
    told.
    """
    if frequency_order * opening_time > SLIT_CROSSING:
        from math import floor
        reduced = floor(SLIT_CROSSING / opening_time)
        warnings.warn(
            f'A {opening_time} s burst is longer than a pulse-shaping slit crossing at '
            f'order {frequency_order}; the order is reduced to {reduced}',
            # Past `pulse_shaping_chopper_speeds_delays` and `calculate`, so the warning is
            # attributed to whoever asked for the burst rather than to this module.
            stacklevel=4,
        )
        return reduced
    return frequency_order


def pulse_shaping_chopper_speeds_delays(
        frequency_order: float, opening_time: float, energy_minimum: float
):
    """Speed and delay for each disk of the pulse-shaping pair.

    A disk is set by when its own slit centre is on the beam, so the two settings sit half a
    burst either side of the band's arrival, each moved out by half a crossing. Lagging the
    second by a crossing less the requested opening makes the burst the requested opening at
    any speed.
    """
    frequency_order = reduce_frequency_order(frequency_order, opening_time)
    if not frequency_order or not SOURCE_FREQUENCY:
        # A parked pair has no reference crossing to be delayed from.
        return 0, 0, 0, 0
    _, _, v_0, v_1 = band_extremes(energy_minimum)
    speed = frequency_order * SOURCE_FREQUENCY
    offset = ((PULSE_SHAPING_DISTANCE / v_1 + PULSE_SHAPING_DISTANCE / v_0) / 2.0
              + SOURCE_DURATION / 2.0 + PULSE_HIGH_FLUX_OFFSET)
    crossing = PULSE_SHAPING_ANGLE / DEGREES_PER_TURN / speed
    delay_0 = offset + opening_time / 2.0 - crossing / 2.0
    delay_1 = delay_0 - opening_time + crossing
    return speed, delay_0, speed, delay_1


def _delay(distance: float, energy: float) -> float:
    """When the middle of the band reaches a disk this far down the guide, in seconds."""
    _, _, v_0, v_1 = band_extremes(energy)
    return ((distance / v_1 + distance / v_0) / 2.0
            + PULSE_HIGH_FLUX_OFFSET + SOURCE_DURATION / 2.0)


def frame_overlap_chopper_speeds_delays(energy_minimum: float):
    """Frame-overlap disks turn at the source frequency and nothing else."""
    return SOURCE_FREQUENCY, _delay(FRAME_OVERLAP_1_DISTANCE, energy_minimum), \
        SOURCE_FREQUENCY, _delay(FRAME_OVERLAP_2_DISTANCE, energy_minimum)


def bandwidth_chopper_speeds_delays(energy_minimum: float):
    """The bandwidth pair, counter-rotating and sharing one delay.

    Both are on the beam at the middle of the band, whichever way they turn to get there.
    Stated as a phase this needed the reader to know that McStas divides by the magnitude of
    the speed, so that one positive angle meant one positive time for either sign; as a delay
    it is simply the same number twice.
    """
    _, _, v_0, v_1 = band_extremes(energy_minimum)
    reference = PULSE_HIGH_FLUX_OFFSET + SOURCE_DURATION / 2.0
    delay = ((reference + BANDWIDTH_DISTANCE / v_1)
             + (reference + BANDWIDTH_DISTANCE / v_0)) / 2.0
    return SOURCE_FREQUENCY, delay, -SOURCE_FREQUENCY, delay


def calculate(order: float, time: float, energy: float, names: tuple[str, ...]):
    """``{name}speed`` in hertz and ``{name}delay`` in seconds, for six named disks.

    ``names`` is in beam order: the pulse-shaping pair, the frame-overlap pair, then the
    bandwidth pair.
    """
    a, b, c, d, e, f = names
    s, p = 'speed', 'delay'
    r = dict()
    r[f'{a}{s}'], r[f'{a}{p}'], r[f'{b}{s}'], r[f'{b}{p}'] = pulse_shaping_chopper_speeds_delays(order, time, energy)
    r[f'{c}{s}'], r[f'{c}{p}'], r[f'{d}{s}'], r[f'{d}{p}'] = frame_overlap_chopper_speeds_delays(energy)
    r[f'{e}{s}'], r[f'{e}{p}'], r[f'{f}{s}'], r[f'{f}{p}'] = bandwidth_chopper_speeds_delays(energy)
    return r


def main(order: float, time: float, energy: float, names: tuple[str, ...] | None = None):
    if names is None or len(names) != 6:
        # names = ('ps1', 'ps2', 'fo1', 'fo2', 'bw1', 'bw2')
        names = ('pulse_shaping_chopper_1', 'pulse_shaping_chopper_2',
                 'frame_overlap_chopper_1', 'frame_overlap_chopper_2',
                 'bandwidth_chopper_1', 'bandwidth_chopper_2')
    rep = calculate(order, time, energy, names)
    print(' '.join([f'{k}={v}' for k, v in rep.items()]))


def is_valid(minimum, maximum, default=None):
    def checker(value):
        try:
            value = float(value)
            if value < minimum:
                value = minimum if default is None else default
            if value > maximum:
                value = maximum
        except:
            from argparse import ArgumentTypeError
            raise ArgumentTypeError(f"{value} is not a valid number between {minimum} and {maximum}")
        return value
    return checker


def is_order(value):
    checker = is_valid(1, 14)
    return round(checker(value))


def script():
    from argparse import ArgumentParser
    parser = ArgumentParser('bifrost_choppers')
    # No nargs=1: it would make a supplied value a one-element list while the default
    # stays a scalar, so `script()` indexed a float and died whenever the flag was omitted.
    parser.add_argument('-o', '--order', type=is_order, default=14.0,
                        help='Pulse Shaping frequency in units of the source frequency')
    parser.add_argument('-t', '--time', type=is_valid(0, 1/SOURCE_FREQUENCY, 0.004), default=0.004,
                        help='Pulse Shaping opening time in seconds')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-e', '--energy', nargs='?', type=is_valid(0.01, 100), default=None,
                       help='Minimum energy of the incident neutron bandwidth in meV')
    group.add_argument('-w', '--wavelength', nargs='?', type=is_valid(0.5, 30), default=None,
                       help='Maximum wavelength of the incident neutron bandwidth in angstrom')
    args = parser.parse_args()

    def energy_zero(energy: float, wavelength: float | None = None):
        if wavelength is not None and wavelength > 0:
            energy = H2_OVER_2M / wavelength / wavelength
        return energy

    main(args.order, args.time, energy_zero(args.energy, args.wavelength))


if __name__ == '__main__':
    script()
