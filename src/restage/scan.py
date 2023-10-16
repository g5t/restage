
from .range import MRange


def make_scan_parser():
    from argparse import ArgumentParser
    parser = ArgumentParser('mcbifrost_scan')
    parser.add_argument('primary', nargs=1, type=str, default=None,
                        help='Primary spectrometer `.instr` file name')
    parser.add_argument('secondary', nargs=1, type=str, default=None,
                        help='Secondary spectrometer `.instr` file name')
    parser.add_argument('parameters', nargs='*', type=str, default=None)
    parser.add_argument('-R', action='append', default=[], help='Runtime parameters')
    parser.add_argument('-g', '--grid', action='store_true', default=False, help='Grid scan')
    return parser


def parse_scan_parameters(unparsed: list[str]) -> dict[str, MRange]:
    """Parse a list of input parameters into a dictionary of MRange objects.

    :parameter unparsed: A list of ranged parameters.
    """
    from .range import parse_list
    return parse_list(MRange, unparsed)


def parse_scan():
    args = make_scan_parser().parse_args()
    parameters = parse_scan_parameters(args.parameters)
    return args, parameters


def run_point(args, parameters):
    print(f'{args} {parameters}')
    pass


def entrypoint():
    """Entrypoint for the mcbifrost_run command."""
    from .energy import bifrost_energy_to_chopper_parameters
    from .range import parameters_to_scan
    args, parameters = parse_scan()
    parameters = bifrost_energy_to_chopper_parameters(parameters)
    n_points, names, scan = parameters_to_scan(parameters)
    print('Now running mcbifrost_run')
    for i, p in enumerate(scan):
        point_parameters = {k: v for k, v in zip(names, p)}
        print(f'Point {i} of {n_points}:')
        run_point(args, point_parameters)
