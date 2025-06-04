def _wavelength_angstrom_to_energy_mev(wavelength):
    return 81.82 / wavelength / wavelength


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
    from .bifrost_choppers import calculate
    choppers = tuple(f'{a}_chopper_{b}' for a, b in product(['pulse_shaping', 'frame_overlap', 'bandwidth'], [1, 2]))
    # names = [a+b for a, b in product(('ps', 'fo', 'bw'), ('1', '2'))]
    chopper_parameter_present = False
    for name in product(choppers, ('speed', 'phase')):
        name = ''.join(name)
        if name not in parameters:
            parameters[name] = 0
        else:
            chopper_parameter_present = True
    order = get_and_remove(parameters, 'order', 14)
    time = get_and_remove(parameters, 'time', get_and_remove(parameters, 't', 170/180/(2 * 15 * 14)))
    return one_generic_energy_to_chopper_parameters(calculate, choppers, time, order, parameters, chopper_parameter_present)


def cspec_translate_energy_to_chopper_parameters(parameters: dict):
    from itertools import product
    from .cspec_choppers import calculate
    choppers = ('bw1', 'bw2', 'bw3', 's', 'p', 'm1', 'm2')
    chopper_parameter_present = False
    for name in product(choppers, ('speed', 'phase')):
        name = ''.join(name)
        if name not in parameters:
            parameters[name] = 0
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
