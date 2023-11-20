from __future__ import annotations


def make_scan_parser():
    from argparse import ArgumentParser
    parser = ArgumentParser('restage_scan')
    parser.add_argument('primary', nargs=1, type=str, default=None,
                        help='Primary spectrometer `.instr` file name')
    parser.add_argument('secondary', nargs=1, type=str, default=None,
                        help='Secondary spectrometer `.instr` file name')
    parser.add_argument('parameters', nargs='*', type=str, default=None)
    parser.add_argument('-R', action='append', default=[], help='Runtime parameters')
    parser.add_argument('-g', '--grid', action='store_true', default=False, help='Grid scan')
    return parser


def parse_scan():
    from .range import parse_scan_parameters
    args = make_scan_parser().parse_args()
    parameters = parse_scan_parameters(args.parameters)
    return args, parameters


def run_point(args, parameters):
    print(f'{args} {parameters}')
    pass


def entrypoint():
    """Entrypoint for the restage_scan command."""
    from .energy import bifrost_translate_energy_to_chopper_parameters
    from .range import parameters_to_scan
    args, parameters = parse_scan()
    n_points, names, scan = parameters_to_scan(parameters)
    for i, p in enumerate(scan):
        point_parameters = bifrost_translate_energy_to_chopper_parameters({k: v for k, v in zip(names, p)})
        run_point(args, point_parameters)
