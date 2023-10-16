from .range import MRange


def _wavelength_angstrom_to_energy_mev(wavelength):
    return 81.82 / wavelength / wavelength


def get_and_remove(d: dict, k: str, default=None):
    if k in d:
        v = d[k]
        del d[k]
        return v
    return default


def generic_energy_to_chopper_parameters(calculate_choppers, time, order,
                                         parameters: dict[str, MRange], grid: bool = False):
    from .range import parameters_to_scan
    if any(x in parameters for x in ('ei', 'wavelength', 'lambda', 'energy', 'e')):
        ei = get_and_remove(parameters, 'ei', get_and_remove(parameters, 'energy', get_and_remove(parameters, 'e')))
        if ei is None:
            wavelength = get_and_remove(parameters, 'wavelength', get_and_remove(parameters, 'lambda'))
            if wavelength is None:
                raise ValueError('No energy or wavelength specified')
            ei = [_wavelength_angstrom_to_energy_mev(x) for x in wavelength]
        _, _, input_parameters = parameters_to_scan({'order': order, 'time': time, 'ei': ei}, grid=grid)
        choppers = [calculate_choppers(order, time, ei) for order, time, ei in input_parameters]
        # convert from the list of dicts to a dict of lists, and insert into the parameters:
        parameters.update({k: [x[k] for x in choppers] for k in choppers[0].keys()})
    return parameters


def bifrost_energy_to_chopper_parameters(parameters: dict[str, MRange], grid: bool = False):
    from itertools import product
    from .bifrost_choppers import calculate as calculate_choppers
    for name in product([a+b for a, b in product(('ps', 'fo', 'bw'), ('1', '2'))], ('speed', 'phase')):
        name = ''.join(name)
        if name not in parameters:
            parameters[name] = MRange(0, 0, 1)
    time = get_and_remove(parameters, 'time', 0.004)
    order = get_and_remove(parameters, 'order', 1)
    return generic_energy_to_chopper_parameters(calculate_choppers, time, order, parameters, grid=grid)


def cspec_energy_to_chopper_parameters(parameters: dict[str, MRange], grid: bool = False):
    from itertools import product
    from .cspec_choppers import calculate as calculate_choppers
    for name in product(('bw1', 'bw2', 'bw3', 's', 'p', 'm1', 'm2'), ('speed', 'phase')):
        name = ''.join(name)
        if name not in parameters:
            parameters[name] = MRange(0, 0, 1)
    time = get_and_remove(parameters, 'time', 0.004)
    order = get_and_remove(parameters, 'order', 16)
    return generic_energy_to_chopper_parameters(calculate_choppers, time, order, parameters, grid=grid)


def no_op_energy_to_chopper_parameters(parameters: dict[str, MRange], grid: bool = False):
    return parameters


def get_energy_to_chopper_translator(instrument: str):
    if 'bifrost' in instrument.lower():
        return bifrost_energy_to_chopper_parameters
    elif 'cspec' in instrument.lower():
        return cspec_energy_to_chopper_parameters
    else:
        return no_op_energy_to_chopper_parameters
