from .constants import H2_OVER_2M


def _wavelength_angstrom_to_energy_mev(wavelength):
    """A wavelength in angstrom as an energy in meV.

    Through the derived h^2/2m rather than a literal: this used to be ``81.82`` while the
    chopper module used ``0.1106``, whose implied value is 81.75037, so the two disagreed
    about the same neutron by 0.04 %.
    """
    return H2_OVER_2M / wavelength / wavelength


#: The two spellings of when a disk's reference point passes the beam. restage sets
#: ``{name}delay``, in seconds; instruments written before chopcal v0.5.0 and niess 0.6.0
#: declare ``{name}phase``, in degrees. McStas accepts either on ``DiskChopper`` and lets
#: ``phase`` win, so an instrument carrying the old name is not broken -- it is just named
#: for a convention restage no longer speaks.
_CONVENTIONS = (('delay', 'phase'), ('phase', 'delay'))


def chopper_convention_hint(available, wanted) -> str | None:
    """Say so when a parameter is missing only because of the delay/phase rename.

    ``Parameter ..._chopper_1phase is not a valid parameter name`` is a true statement and a
    misleading one: nothing is wrong with the instrument, the two sides simply disagree
    about what to call the same quantity. Spotting that the instrument has the *other*
    spelling is what turns an afternoon into a one-line rename, so it is worth the few lines
    it costs to check.
    """
    available = set(available)
    for ours, theirs in _CONVENTIONS:
        swapped = sorted(n for n in wanted
                         if n.endswith(ours) and n not in available
                         and n.removesuffix(ours) + theirs in available)
        if not swapped:
            continue
        units = 'seconds' if ours == 'delay' else 'degrees'
        return (
            f"restage sets chopper {ours} ({units}); this instrument declares "
            f"'{swapped[0].removesuffix(ours)}{theirs}' instead of "
            f"'{swapped[0]}'{'' if len(swapped) == 1 else f' (and {len(swapped) - 1} more)'}. "
            f"Rename the instrument's chopper parameters from '{{name}}{theirs}' to "
            f"'{{name}}{ours}' and pass them to DiskChopper as {ours}=, not {theirs}=. "
            f"A phase is an angle and depends on which way the disk turns; a delay is a "
            f"time and does not."
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
        chopper_parameter_present: bool
):
    from loguru import logger
    if any(x in parameters for x in ('ei', 'wavelength', 'lambda', 'energy', 'e')):
        if chopper_parameter_present:
            logger.warning('Specified chopper parameter(s) overridden by Ei or wavelength.')
        ei = get_and_remove(parameters, 'ei', get_and_remove(parameters, 'energy', get_and_remove(parameters, 'e')))
        if ei is None:
            wavelength = get_and_remove(parameters, 'wavelength', get_and_remove(parameters, 'lambda'))
            ei = _wavelength_angstrom_to_energy_mev(wavelength)
        choppers = calculate_choppers(order, time, ei, names=chopper_names)
        parameters.update(choppers)
    return parameters


def bifrost_translate_energy_to_chopper_parameters(parameters: dict):
    from itertools import product
    from .bifrost_choppers import calculate, SLIT_CROSSING
    choppers = tuple(f'{a}_chopper_{b}' for a, b in product(['pulse_shaping', 'frame_overlap', 'bandwidth'], [1, 2]))
    # names = [a+b for a, b in product(('ps', 'fo', 'bw'), ('1', '2'))]
    chopper_parameter_present = False
    for name in product(choppers, ('speed', 'delay')):
        n = ''.join(name)
        if n not in parameters:
            parameters[n] = 0
        else:
            chopper_parameter_present = True
    order = get_and_remove(parameters, 'order', 14)
    # One slit crossing at order 15 -- just shorter than the shortest burst order 14 can
    # make, so the default never provokes a reduction.
    default_time = SLIT_CROSSING / 15
    time = get_and_remove(parameters, 'time', get_and_remove(parameters, 't', default_time))
    return one_generic_energy_to_chopper_parameters(calculate, choppers, time, order, parameters, chopper_parameter_present)


def cspec_translate_energy_to_chopper_parameters(parameters: dict):
    from itertools import product
    from .cspec_choppers import calculate
    choppers = ('bw1', 'bw2', 'bw3', 's', 'p', 'm1', 'm2')
    chopper_parameter_present = False
    for name in product(choppers, ('speed', 'delay')):
        n = ''.join(name)
        if n not in parameters:
            parameters[n] = 0
        else:
            chopper_parameter_present = True
    time = get_and_remove(parameters, 'time', 0.004)
    order = get_and_remove(parameters, 'order', 16)
    return one_generic_energy_to_chopper_parameters(calculate, choppers, time, order, parameters, chopper_parameter_present)


def no_op_translate_energy_to_chopper_parameters(parameters: dict):
    return parameters


def energy_to_chopper_translator(instrument: str):
    if 'bifrost' in instrument.lower():
        return bifrost_translate_energy_to_chopper_parameters
    if 'cspec' in instrument.lower():
        return cspec_translate_energy_to_chopper_parameters
    return no_op_translate_energy_to_chopper_parameters


def get_energy_parameter_names(instr: str):
    if 'bifrost' in instr.lower():
        return ['e', 'ei', 'energy', 'wavelength', 'lambda', 'time', 't', 'order']
    elif 'cspec' in instr.lower():
        return ['e', 'ei', 'energy', 'wavelength', 'lambda', 'reps']
    else:
        return []
