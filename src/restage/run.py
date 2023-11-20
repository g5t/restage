from __future__ import annotations

from .range import Singular


def make_run_parser():
    from argparse import ArgumentParser
    parser = ArgumentParser('mcbifrost_run')
    parser.add_argument('primary', nargs=1, type=str, default=None,
                        help='Primary spectrometer `.instr` file name')
    parser.add_argument('secondary', nargs=1, type=str, default=None,
                        help='Secondary spectrometer `.instr` file name')
    parser.add_argument('parameters', nargs='*', type=str, default=None)
    return parser


def parse_run_parameters(unparsed: list[str]) -> dict[str, Singular]:
    """Parse a list of input parameters into a dictionary of Singular objects.

    :parameter unparsed: A list of parameters.
    """
    from .range import parse_list
    return parse_list(Singular, unparsed)


def parse_run():
    args = make_run_parser().parse_args()
    parameters = parse_run_parameters(args.parameters)
    return args, parameters


def entrypoint():
    args, parameters = parse_run()
    run(args, parameters)


def run(args, parameters, overrides: dict | None = None):
    values = {k: v for k, v in parameters.items()}
    values.update(overrides or {})

    # 1. find the part of the instrument file that contains the primary instrument
    # 2. check if the primary instrument parameter set is already in the database
    # 3. if not, run the primary instrument and insert it in the database
    # 4. run the secondary instrument
