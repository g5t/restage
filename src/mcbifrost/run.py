from typing import Union
from .range import MRange


def parse_run():
    from argparse import ArgumentParser
    parser = ArgumentParser('mcbifrost_run')
    parser.add_argument('instrument', nargs=1, type=str, default=None)
    parser.add_argument('parameters', nargs='*', type=str, default=None)

    args = parser.parse_args()

    parameters = {}
    while len(args.parameters):
        if '=' in args.parameters[0]:
            k, v = args.parameters[0].split('=')
            parameters[k] = MRange.from_str(v)
        elif len(args.parameters) > 1 and '=' not in args.parameters[1]:
            parameters[args.parameters[0]] = MRange.from_str(args.parameters[1])
            del args.parameters[1]
        else:
            raise ValueError(f'Invalid parameter {args.parameters[0]}')
        del args.parameters[0]
    print(args)
    print(parameters)
    return args, parameters


def parameters_to_scan(parameters: dict[str, Union[list, MRange]], grid: bool = False):
    """Convert a dictionary of ranged parameters to a list of parameter names and an iterable of parameter value tuples.

    The ranged parameters can be either MRange objects or lists of values. If a list of values is provided, it will be
    iterated over directly.

    :parameter parameters: A dictionary of ranged parameters.
    :parameter grid: Controls how the parameters are iterated; True implies a grid scan, False implies a linear scan.
    """
    names = [x.lower() for x in parameters.keys()]
    values = [x if hasattr(x, '__iter__') else [x] for x in parameters.values()]
    if grid:
        from itertools import product
        # singular MRange objects *should* stop the grid along their axis:
        return names, product(*values)
    else:
        from .range import Singular
        # replace singular MRange entries with Singular iterators, to avoid stopping the zip early:
        n_max = max([len(v) for v in values])
        for i, v in enumerate(values):
            if len(v) > 1 and len(v) != n_max:
                others = [names[i] for i, n in enumerate(values) if len(n) == n_max]
                par = 'parameters' if len(others) > 1 else 'parameter'
                have = 'have' if len(others) > 1 else 'has'
                others = ', '.join(others)
                raise ValueError(f'Parameter {names[i]} has {len(v)} values, but {par} {others} {have} {n_max}')
        return names, zip(*[v if len(v) > 1 else Singular(v[0], n_max) for v in values])


def _wavelength_angstrom_to_energy_mev(wavelength):
    return 81.82 / wavelength / wavelength


def get_and_remove(d: dict, k: str, default=None):
    if k in d:
        v = d[k]
        del d[k]
        return v
    return default


def energy_to_chopper_parameters(parameters: dict[str, MRange], grid: bool = False):
    from itertools import product
    from .choppers import calculate as calculate_choppers
    for name in product([a+b for a, b in product(('ps', 'fo', 'bw'), ('1', '2'))], ('speed', 'phase')):
        name = ''.join(name)
        if name not in parameters:
            parameters[name] = MRange(0, 0, 1)

    if any(x in parameters for x in ('ei', 'wavelength', 'lambda', 'energy', 'e')):
        ei = get_and_remove(parameters, 'ei', get_and_remove(parameters, 'energy', get_and_remove(parameters, 'e')))
        if ei is None:
            wavelength = get_and_remove(parameters, 'wavelength', get_and_remove(parameters, 'lambda'))
            if wavelength is None:
                raise ValueError('No energy or wavelength specified')
            ei = [_wavelength_angstrom_to_energy_mev(x) for x in wavelength]
        time = get_and_remove(parameters, 'time', 0.004)
        order = get_and_remove(parameters, 'order', 1)
        _, input_parameters = parameters_to_scan({'order': order, 'time': time, 'ei': ei}, grid=grid)
        choppers = [calculate_choppers(order, time, ei) for order, time, ei in input_parameters]
        # convert from the list of dicts to a dict of lists, and insert into the parameters:
        parameters.update({k: [x[k] for x in choppers] for k in choppers[0].keys()})
    return parameters


def run_point(args, parameters):
    print(f'{args} {parameters}')
    pass


def run():
    """Entrypoint for the mcbifrost_run command."""
    args, parameters = parse_run()
    parameters = energy_to_chopper_parameters(parameters)
    names, scan = parameters_to_scan(parameters)
    print('Now running mcbifrost_run')
    for i, p in enumerate(scan):
        point_parameters = {k: v for k, v in zip(names, p)}
        print(f'Point {i}:')
        run_point(args, point_parameters)
