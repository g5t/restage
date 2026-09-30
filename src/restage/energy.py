from dataclasses import dataclass

from .constants import H2_OVER_2M


def _wavelength_angstrom_to_energy_mev(wavelength):
    """A wavelength in angstrom as an energy in meV.

    Through the derived h^2/2m rather than a literal: this used to be ``81.82`` while the
    chopper module used ``0.1106``, whose implied value is 81.75037, so the two disagreed
    about the same neutron by 0.04 %.
    """
    return H2_OVER_2M / wavelength / wavelength


@dataclass(frozen=True)
class ChopperKnobs:
    """How an instrument names a disk's run-time knobs, and the unit of its delay.

    The calculations here work in hertz and seconds. What an instrument declares is its
    own business, so the translation is done here, once, when the names are made.
    """
    speed: str
    delay: str
    #: How many of the instrument's delay units make one second.
    delay_per_second: float
    description: str

    def speed_name(self, disk: str) -> str:
        return f'{disk}{self.speed}'

    def delay_name(self, disk: str) -> str:
        return f'{disk}{self.delay}'

    def names(self, disks) -> tuple[str, ...]:
        return tuple(n for d in disks for n in (self.speed_name(d), self.delay_name(d)))

    def settings(self, disk: str, speed_hz: float, delay_s: float) -> dict[str, float]:
        """One disk's speed and delay, calculated in Hz and s, as the instrument wants them."""
        return {self.speed_name(disk): speed_hz,
                self.delay_name(disk): delay_s * self.delay_per_second}


#: Each knob is named for the ESS log it is published as and declared in that log's unit:
#: ``{disk}_rotation_speed`` in Hz and ``{disk}_delay`` in ns, the chopper's TotDly. This is
#: what niess emits from 0.8.
ESS_KNOBS = ChopperKnobs('_rotation_speed', '_delay', 1e9,
                         "'{name}_rotation_speed' (Hz) and '{name}_delay' (ns)")

#: ``{disk}speed`` in Hz and ``{disk}delay`` in s, as chopcal v0.5 and niess 0.6 and 0.7
#: emit them.
LEGACY_KNOBS = ChopperKnobs('speed', 'delay', 1.0,
                            "'{name}speed' (Hz) and '{name}delay' (s)")

#: Newest first. `chopper_knobs` picks the one an instrument declares.
CHOPPER_KNOBS = (ESS_KNOBS, LEGACY_KNOBS)


def chopper_knobs(disks, declared=None) -> ChopperKnobs:
    """Which convention an instrument uses for these disks' knobs.

    The one it declares the most knobs of: an instrument may leave a disk's knob out, or
    share one knob between two disks, without that making the convention ambiguous. With
    nothing to go on -- no declared names, or none of any convention -- the legacy
    spelling, which is what restage has always set. A tie goes to the legacy spelling too.
    """
    if declared is None:
        return LEGACY_KNOBS
    declared = set(declared)
    counts = {knobs: sum(name in declared for name in knobs.names(disks))
              for knobs in CHOPPER_KNOBS}
    best = max(counts.values())
    if best == 0 or counts[LEGACY_KNOBS] == best:
        return LEGACY_KNOBS
    return max(counts, key=counts.get)


#: Spellings of one quantity that an instrument may use instead of the one restage set.
#: A phase is when a disk's reference point passes the beam, in degrees; instruments
#: written before chopcal v0.5.0 and niess 0.6.0 declare ``{name}phase``. McStas accepts
#: either on ``DiskChopper`` and lets ``phase`` win, so an instrument carrying the old name
#: is not broken -- it is just named for a convention restage no longer speaks.
_RENAMES = (
    ('delay', 'phase', 'chopper delay (seconds)',
     "pass them to DiskChopper as delay=, not phase=. A phase is an angle and depends on "
     "which way the disk turns; a delay is a time and does not."),
    ('phase', 'delay', 'chopper phase (degrees)',
     "pass them to DiskChopper as phase=, not delay=. A phase is an angle and depends on "
     "which way the disk turns; a delay is a time and does not."),
    ('speed', '_rotation_speed', "chopper speed as '{name}speed'",
     "or build the instrument with the niess release it was written for; niess 0.8 names "
     "a disk's knobs for the ESS logs they are published as."),
    ('_rotation_speed', 'speed', "chopper speed as '{name}_rotation_speed'",
     "or rebuild the instrument with niess 0.8 or later, which names a disk's knobs for the "
     "ESS logs they are published as."),
    ('_delay', 'delay', "chopper delay as '{name}_delay' (nanoseconds)",
     "or rebuild the instrument with niess 0.8 or later. Note the unit: '{name}_delay' is "
     "in nanoseconds and '{name}delay' in seconds."),
    ('delay', '_delay', "chopper delay as '{name}delay' (seconds)",
     "or build the instrument with the niess release it was written for. Note the unit: "
     "'{name}delay' is in seconds and '{name}_delay' in nanoseconds."),
)


def chopper_convention_hint(available, wanted) -> str | None:
    """Say so when a parameter is missing only because of a chopper knob rename.

    ``Parameter ..._chopper_1phase is not a valid parameter name`` is a true statement and a
    misleading one: nothing is wrong with the instrument, the two sides simply disagree
    about what to call the same quantity. Spotting that the instrument has the *other*
    spelling is what turns an afternoon into a one-line rename, so it is worth the few lines
    it costs to check.
    """
    available = set(available)
    for ours, theirs, what, advice in _RENAMES:
        swapped = sorted(n for n in wanted
                         if n.endswith(ours) and n not in available
                         and n.removesuffix(ours) + theirs in available)
        if not swapped:
            continue
        return (
            f"restage sets {what}; this instrument declares "
            f"'{swapped[0].removesuffix(ours)}{theirs}' instead of "
            f"'{swapped[0]}'{'' if len(swapped) == 1 else f' (and {len(swapped) - 1} more)'}. "
            f"Rename the instrument's chopper parameters from '{{name}}{theirs}' to "
            f"'{{name}}{ours}', {advice}"
        )
    return None


def get_and_remove(d: dict, k: str, default=None):
    if k in d:
        v = d[k]
        del d[k]
        return v
    return default


def one_generic_energy_to_chopper_parameters(
        calculate_choppers, chopper_names: tuple[str, ...],
        time: float, order: int, parameters: dict,
        chopper_parameter_present: bool, knobs: ChopperKnobs = LEGACY_KNOBS,
):
    from loguru import logger
    if any(x in parameters for x in ('ei', 'wavelength', 'lambda', 'energy', 'e')):
        if chopper_parameter_present:
            logger.warning('Specified chopper parameter(s) overridden by Ei or wavelength.')
        ei = get_and_remove(parameters, 'ei', get_and_remove(parameters, 'energy', get_and_remove(parameters, 'e')))
        if ei is None:
            wavelength = get_and_remove(parameters, 'wavelength', get_and_remove(parameters, 'lambda'))
            ei = _wavelength_angstrom_to_energy_mev(wavelength)
        # Calculated in Hz and s, under the legacy names, then handed over as the
        # instrument declares them.
        choppers = calculate_choppers(order, time, ei, names=chopper_names)
        for disk in chopper_names:
            parameters.update(knobs.settings(disk, choppers[LEGACY_KNOBS.speed_name(disk)],
                                             choppers[LEGACY_KNOBS.delay_name(disk)]))
    return parameters


def _unset_knobs_to_zero(parameters: dict, knobs: ChopperKnobs, disks) -> bool:
    """Give every knob a value, and say whether the caller had set any of them."""
    present = False
    for name in knobs.names(disks):
        if name not in parameters:
            parameters[name] = 0
        else:
            present = True
    return present


BIFROST_CHOPPERS = tuple(f'{a}_chopper_{b}' for a in ('pulse_shaping', 'frame_overlap', 'bandwidth')
                         for b in (1, 2))


def bifrost_translate_energy_to_chopper_parameters(parameters: dict,
                                                   knobs: ChopperKnobs = LEGACY_KNOBS):
    from .bifrost_choppers import calculate, SLIT_CROSSING
    chopper_parameter_present = _unset_knobs_to_zero(parameters, knobs, BIFROST_CHOPPERS)
    order = get_and_remove(parameters, 'order', 14)
    # One slit crossing at order 15 -- just shorter than the shortest burst order 14 can
    # make, so the default never provokes a reduction.
    default_time = SLIT_CROSSING / 15
    time = get_and_remove(parameters, 'time', get_and_remove(parameters, 't', default_time))
    return one_generic_energy_to_chopper_parameters(calculate, BIFROST_CHOPPERS, time, order,
                                                    parameters, chopper_parameter_present,
                                                    knobs)


CSPEC_CHOPPERS = ('bw1', 'bw2', 'bw3', 's', 'p', 'm1', 'm2')


def cspec_translate_energy_to_chopper_parameters(parameters: dict,
                                                 knobs: ChopperKnobs = LEGACY_KNOBS):
    from .cspec_choppers import calculate
    chopper_parameter_present = _unset_knobs_to_zero(parameters, knobs, CSPEC_CHOPPERS)
    time = get_and_remove(parameters, 'time', 0.004)
    order = get_and_remove(parameters, 'order', 16)
    return one_generic_energy_to_chopper_parameters(calculate, CSPEC_CHOPPERS, time, order,
                                                    parameters, chopper_parameter_present,
                                                    knobs)


def no_op_translate_energy_to_chopper_parameters(parameters: dict):
    return parameters


def energy_to_chopper_translator(instrument: str, declared=None):
    """The function turning energy parameters into chopper settings for an instrument.

    Chosen by the instrument's name. ``declared`` is the parameter names the instrument
    declares, from which the knob convention is read -- see `chopper_knobs`. Without it
    the legacy names are set, as they always were.
    """
    from functools import partial
    for key, translate, disks in (
            ('bifrost', bifrost_translate_energy_to_chopper_parameters, BIFROST_CHOPPERS),
            ('cspec', cspec_translate_energy_to_chopper_parameters, CSPEC_CHOPPERS)):
        if key in instrument.lower():
            return partial(translate, knobs=chopper_knobs(disks, declared))
    return no_op_translate_energy_to_chopper_parameters


def declared_parameter_names(*instruments) -> list[str]:
    """Every parameter name the given instruments declare, for `energy_to_chopper_translator`."""
    return [p.name for instr in instruments for p in instr.parameters]


def get_energy_parameter_names(instr: str):
    if 'bifrost' in instr.lower():
        return ['e', 'ei', 'energy', 'wavelength', 'lambda', 'time', 't', 'order']
    elif 'cspec' in instr.lower():
        return ['e', 'ei', 'energy', 'wavelength', 'lambda', 'reps']
    else:
        return []
