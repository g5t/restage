"""Utilities for calculating chopper values, duplicated from old McStas instrument"""
from __future__ import annotations


SOURCE_FREQUENCY = 14.  # Hz
# PuleHighFluxOffset
SOURCE_HIGH_FLUX_DELAY = 0.0002  # s; Time from T0 to 'high pulse'
# ModPulseLengthHighF
SOURCE_HIGH_FLUX_WIDTH = 0.00286  # s; Duration of the 'high pulse'

INSTRUMENT_LENGTH = 162.0  # m
# chopPulseDist
MOD_PS_DISTANCE = 4.41 + 0.032 + 2.0 - 0.1  # m; Moderator to Pulse Shaping chopper distance
# chopFrameOverlap1Pos
MOD_FO1_DISTANCE = 8.530  # m, Pulse Shaping chopper to Frame Overlap 1 chopper distance
# chopFrameOverlap2Pos
MOD_FO2_DISTANCE = 14.973  # m
# chopBWPos
PS_BW_DISTANCE = 78  # m, Pulse Shaping chopper to Bandwidth chopper distance (actually moderator to BW?)


def wavelength_extremes(energy_minimum: float):
    from math import sqrt
    wavelength_band = 1 / (INSTRUMENT_LENGTH - MOD_PS_DISTANCE) / SOURCE_FREQUENCY / 0.0002528
    wavelength_maximum = 1 / (0.1106 * sqrt(energy_minimum))
    wavelength_minimum = wavelength_maximum - wavelength_band
    return wavelength_minimum, wavelength_maximum


def velocity_extremes(energy_minimum: float):
    minimum, maximum = [3956.0 / w for w in wavelength_extremes(energy_minimum)]
    return minimum, maximum


def pulse_shaping_chopper_speeds_phases(frequency_order: float, opening_time: float, energy_minimum: float):
    from math import floor
    opening_angle = 170.  # degrees
    reduction = 360.0 * opening_time * frequency_order * SOURCE_FREQUENCY
    if reduction > opening_angle:
        frequency_order = floor(opening_angle / 360. / SOURCE_FREQUENCY / opening_time)
        print(f"Requested frequency and opening time is unobtainable. Frequency order reduced to {frequency_order}")
    if frequency_order and SOURCE_FREQUENCY:
        v_1, v_2 = velocity_extremes(energy_minimum)
        average_delay = MOD_PS_DISTANCE * (1 / v_1 + 1 / v_2) / 2 + SOURCE_HIGH_FLUX_WIDTH / 2 + SOURCE_HIGH_FLUX_DELAY
        frequency = SOURCE_FREQUENCY * frequency_order
        phase0 = (average_delay + opening_time / 2) * frequency * 360. - opening_angle / 2
        phase1 = phase0 - 360. * opening_time * frequency + opening_angle
    else:
        return 0, 0, 0, 0
    return frequency, phase0, frequency, phase1


def pulse_shaping_opening_time(frequency, phase0, phase1):
    opening_angle = 170.
    opening_time = (phase0 - phase1 + opening_angle) / (360. * frequency)
    return opening_time


def pulse_shaping_average_delay(frequency, phase0, phase1):
    opening_angle = 170.
    opening_time = pulse_shaping_opening_time(frequency, phase0, phase1)
    average_delay = (phase0 + opening_angle / 2) / (360. * frequency) - opening_time / 2
    return average_delay


def _phase(distance: float, energy: float):
    v_1, v_2 = velocity_extremes(energy)
    delay = distance * (1 / v_1 + 1 / v_2) / 2 + SOURCE_HIGH_FLUX_WIDTH / 2 + SOURCE_HIGH_FLUX_DELAY
    return delay * SOURCE_FREQUENCY * 360.


def frame_overlap_chopper_speeds_phases(energy_minimum: float):
    # Frame Overlap choppers operate only at the source frequency
    return SOURCE_FREQUENCY, _phase(MOD_FO1_DISTANCE, energy_minimum), \
        SOURCE_FREQUENCY, _phase(MOD_FO2_DISTANCE, energy_minimum)


def bandwidth_chopper_speeds_phases(energy_minimum: float):
    phase = _phase(PS_BW_DISTANCE, energy_minimum)
    # McStas implements disk chopper phase _strictly_ as a delay time -- and calculates the delay as phase/fabs(speed)
    # Therefore the phase should *always* be positive, even though the second disk rotates in the opposite direction
    return SOURCE_FREQUENCY, phase, -SOURCE_FREQUENCY, phase


def calculate(order: float, time: float, energy: float, names: list[str] | None = None):
    if names is None or len(names) != 6:
        names = ['ps1', 'ps2', 'fo1', 'fo2', 'bw1', 'bw2']
    a, b, c, d, e, f = names
    s, p = 'speed', 'phase'
    r = dict()
    r[f'{a}{s}'], r[f'{a}{p}'], r[f'{b}{s}'], r[f'{b}{p}'] = pulse_shaping_chopper_speeds_phases(order, time, energy)
    r[f'{c}{s}'], r[f'{c}{p}'], r[f'{d}{s}'], r[f'{d}{p}'] = frame_overlap_chopper_speeds_phases(energy)
    r[f'{e}{s}'], r[f'{e}{p}'], r[f'{f}{s}'], r[f'{f}{p}'] = bandwidth_chopper_speeds_phases(energy)
    return r


def main(order: float, time: float, energy: float, names: list[str] | None = None):
    if names is None or len(names) != 6:
        names = ['ps1', 'ps2', 'fo1', 'fo2', 'bw1', 'bw2']
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
    parser.add_argument('-o', '--order', nargs=1, type=is_order, default=14.0,
                        help='Pulse Shaping frequency in units of the source frequency')
    parser.add_argument('-t', '--time', nargs=1, type=is_valid(0, 1/SOURCE_FREQUENCY, 0.004), default=0.004,
                        help='Pulse Shaping opening time in seconds')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-e', '--energy', nargs='?', type=is_valid(0.01, 100), default=None,
                       help='Minimum energy of the incident neutron bandwidth in meV')
    group.add_argument('-w', '--wavelength', nargs='?', type=is_valid(0.5, 30), default=None,
                       help='Maximum wavelength of the incident neutron bandwidth in angstrom')
    args = parser.parse_args()

    def energy_zero(energy: float, wavelength: float | None = None):
        if wavelength is not None and wavelength > 0:
            energy = 81.82 / wavelength / wavelength
        return energy

    main(args.order[0], args.time[0], energy_zero(args.energy, args.wavelength))


if __name__ == '__main__':
    script()
