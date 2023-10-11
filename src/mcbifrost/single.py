from typing import Union
from .range import Singular, MRange
from .tables import SimulationEntry, InstrEntry

def make_single_parser():
    from argparse import ArgumentParser
    parser = ArgumentParser('mcbifrost_single')
    parser.add_argument('instrument', nargs=1, type=str, default=None,
                        help='Instrument `.instr` file name')
    parser.add_argument('parameters', nargs='*', type=str, default=None)
    parser.add_argument('split-at', nargs=1, stype=str, default='mcpl_split',
                        help='The component name where the instr should be split -- must exist in the instrument file')
    parser.add_argument('-R', action='append', default=[], help='Runtime parameters')
    parser.add_argument('-g', '--grid', action='store_true', default=False, help='Grid scan')
    return parser


def parse_single_parameters(unparsed: list[str]) -> dict[str, MRange]:
    """Parse a list of input parameters into a dictionary of Singular objects.

    :parameter unparsed: A list of parameters.
    """
    from .range import parse_list
    return parse_list(MRange, unparsed)


def parse_single():
    args = make_single_parser().parse_args()
    parameters = parse_single_parameters(args.parameters)
    return args, parameters


def entrypoint():
    args, parameters = parse_single()
    single_from_file(args, parameters)


def single_from_file(args, parameters):
    from mccode.loader import load_mcstas_instr
    instr = load_mcstas_instr(args.instrument[0])
    split_at = args.split_at[0]
    runtime_arguments = args.R
    grid = args.grid
    single(instr, parameters, split_at=split_at, runtime_arguments=runtime_arguments, grid=grid)


def single(instr, parameters, split_at=None, runtime_arguments=None, grid=False):
    from zenlog import log
    if split_at is None:
        split_at = 'mcpl_split'
    if runtime_arguments is None:
        runtime_arguments = []

    if not instr.has_component_named(split_at):
        log.error(f'The specified split-at component, {split_at}, does not exist in the instrument file')
    # splitting defines an instrument parameter in both returned instrument, 'mcpl_filename'.
    pre, post = instr.mcpl_split(split_at, remove_unused_parameters=True)
    # ... reduce the parameters to those that are relevant to the two instruments.
    pre_parameters = {k: v for k, v in parameters.items() if k in pre.has_parameter(k)}
    post_parameters = {k: v for k, v in parameters.items() if k in post.has_parameter(k)}

    pre_entry = single_pre(pre, pre_parameters, runtime_arguments, grid)
    single_combined(pre_entry, post, pre_parameters, post_parameters, runtime_arguments, grid)


def single_pre(instr, parameters, runtime_arguments, grid):
    from .cache import cache_instr, cache_has_simulation, cache_simulation
    from .energy import energy_to_chopper_parameters
    from .range import parameters_to_scan
    # check if this instr is already represented in the module's cache database
    # if not, it is compiled and added to the cache with (hopefully sensible) defaults specified
    entry = cache_instr(instr)
    names, scan = parameters_to_scan(energy_to_chopper_parameters(parameters, grid=grid), grid=grid)
    for values in scan:
        nv = {n: v for n, v in zip(names, values)}
        if not cache_has_simulation(entry, nv):
            sit = SimulationEntry(nv)
            sit.output_path = do_primary_simulation(sit, entry, nv, runtime_arguments)
            cache_simulation(entry, sit)
    return entry


def single_combined(pre_entry, post, pre_parameters, post_parameters, runtime_arguments, grid):
    from .cache import cache_instr, cache_get_simulation
    from .energy import energy_to_chopper_parameters
    from .range import parameters_to_scan

    entry = cache_instr(post)
    parameters = {**pre_parameters, **post_parameters}
    names, scan = parameters_to_scan(energy_to_chopper_parameters(parameters, grid=grid), grid=grid)
    for values in scan:
        pars = {n: v for n, v in zip(names, values)}
        primary_pars = {k: v for k, v in pars.items() if k in pre_parameters}
        secondary_pars = {k: v for k, v in pars.items() if k in post_parameters}
        primary_simulation_entry = cache_get_simulation(pre_entry, primary_pars)
        do_secondary_simulation(primary_simulation_entry, entry, secondary_pars, runtime_arguments)


def do_primary_simulation(sit: SimulationEntry,
                          instr_file_entry: InstrEntry,
                          parameters: dict,
                          runtime_arguments: list[str]):
    from zenlog import log
    from pathlib import Path
    from mccode.compiler.c import run_compiled_instrument, CBinaryTarget
    from .cache import module_data_path
    work_dir = module_data_path('sim').joinpath(sit.id)

    if work_dir.exists():
        log.warn('Simulation directory already exists, expect problems with McStas')

    binary_at = Path(instr_file_entry.binary_path)
    target = CBinaryTarget(mpi=False, acc=False, count=1, nexus=False)
    # FIXME there's probably more work required to get runtime_arguments into shape
    args = f'--dir {work_dir} ' + ' '.join(runtime_arguments) + ' '.join([f'{k}={v}' for k, v in parameters.items()])
    args += f' mcpl_filename={work_dir.joinpath(sit.id)}.mcpl'
    run_compiled_instrument(binary_at, target, args, capture=False)
    return str(work_dir)


def do_secondary_simulation(p_sit: SimulationEntry, entry: InstrEntry, pars: dict, args: list[str]):
    from pathlib import Path
    from mccode.compiler.c import run_compiled_instrument, CBinaryTarget

    mcpl_path = Path(p_sit.output_path).joinpath(p_sit.id)
    if mcpl_path.with_suffix('.mcpl').exists():
        mcpl_path = mcpl_path.with_suffix('.mcpl')
    elif mcpl_path.with_suffix('.mcpl.gz').exists():
        mcpl_path = mcpl_path.with_suffix('.mcpl.gz')
    else:
        raise RuntimeError(f"Could not find MCPL file {mcpl_path} with either .mcpl or .mcpl.gz suffixes")

    args = ' '.join(args) + ' '.join([f'{k}={v}' for k, v in pars.items()])
    args += f' mcpl_filename={mcpl_path}'

    executable = Path(entry.binary_path)
    target = CBinaryTarget(mpi=False, acc=False, count=1, nexus=False)
    run_compiled_instrument(executable, target, args, capture=False)

