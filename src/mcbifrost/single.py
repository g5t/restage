from typing import Union
from .range import Singular, MRange
from .tables import SimulationEntry, InstrEntry


def make_single_parser():
    from argparse import ArgumentParser
    parser = ArgumentParser('mcbifrost_single')
    aa = parser.add_argument
    aa('instrument', nargs=1, type=str, default=None, help='Instrument `.instr` file name')
    aa('parameters', nargs='*', type=str, default=None)
    aa('--split-at', nargs=1, type=str, default='mcpl_split',
       help='Component at which to split -- must exist in instr')
    aa('-m', '--mesh', action='store_true', default=False, help='N-dimensional mesh scan')
    # the following are McCode runtime arguments which might be used by the instrument
    aa('-s', '--seed', nargs=1, type=int, default=None, help='Random number generator seed')
    aa('-n', '--ncount', nargs=1, type=int, default=None, help='Number of neutrons to simulate')
    aa('-d', '--dir', nargs=1, type=str, default=None, help='Output directory')
    aa('-t', '--trace', action='store_true', default=False, help='Enable tracing')
    aa('-g', '--gravitation', action='store_true', default=False, help='Enable gravitation for all trajectories')
    aa('--bufsiz', nargs=1, type=int, default=None, help='Monitor_nD list/buffer-size')
    aa('--format', nargs=1, type=str, default=None, help='Output data files using FORMAT')
    # Other McCode runtime arguments exist, but are likely not used during a scan:
    # --no-output-files             Do not write any data files
    # -i, --info                    Detailed instrument information
    # --list-parameters             Print the instrument parameters to standard output
    # --meta-list                   Print names of components which defined metadata
    # --meta-defined COMP[:NAME]    Print component defined metadata, or (0,1) if NAME provided
    # --meta-type COMP:NAME         Print metadata format type specified in definition
    # --meta-data COMP:NAME         Print metadata data text specified in definition
    # --source                      Show the instrument source code which was compiled
    return parser


def get_best_of(src, names: tuple):
    for name in names:
        if name in src:
            return name
    raise RuntimeError(f"None of {names} found in {src}")


def insert_best_of(src, snk, names: tuple):
    if any(x in src for x in names):
        snk[names[0]] = get_best_of(src, names)
    return snk


def regular_mccode_runtime_dict(args: dict) -> dict:
    t = insert_best_of(args, {}, ('seed', 's'))
    t = insert_best_of(args, t, ('ncount', 'n'))
    t = insert_best_of(args, t, ('dir', 'out_dir', 'd'))
    t = insert_best_of(args, t, ('trace', 't'))
    t = insert_best_of(args, t, ('gravitation', 'g'))
    t = insert_best_of(args, t, ('bufsiz',))
    t = insert_best_of(args, t, ('format',))
    return t


def mccode_runtime_dict_to_args_list(t: dict) -> list[str]:
    """Convert a dictionary of McCode runtime arguments to a string.

    :parameter args: A dictionary of McCode runtime arguments.
    :return: A list of arguments suitable for use in a command line call to a McCode compiled instrument.
    """
    # convert to a standardized string:
    out = []
    if 'seed' in t and t['seed'] is not None:
        out.append(f'--seed={t["seed"]}')
    if 'ncount' in t and t['ncount'] is not None:
        out.append(f'--ncount={t["ncount"]}')
    if 'dir' in t and t['dir'] is not None:
        out.append(f'--dir={t["dir"]}')
    if 'trace' in t and t['trace']:
        out.append('--trace')
    if 'gravitation' in t and t['gravitation']:
        out.append('--gravitation')
    if 'bufsiz' in t and t['bufsiz'] is not None:
        out.append(f'--bufsiz={t["bufsiz"]}')
    if 'format' in t and t['format'] is not None:
        out.append(f'--format={t["format"]}')
    return out


def parse_single_parameters(unparsed: list[str]) -> dict[str, Union[MRange, Singular]]:
    """Parse a list of input parameters into a dictionary of MRange or Singular objects.

    :parameter unparsed: A list of parameters.
    :return: A dictionary of MRange or Singular objects. The Singular objects have their maximum length set to the
             maximum iterations of all the ranges to avoid infinite iterations.
    """
    from .range import parse_command_line_parameters
    ranges = parse_command_line_parameters(unparsed)
    max_length = max([len(v) for v in ranges.values() if isinstance(v, MRange)])
    for k, v in ranges.items():
        if isinstance(v, Singular) and v.maximum is None:
            ranges[k] = Singular(v.value, max_length)
    return ranges


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
    single(instr, parameters, split_at=args.split_at[0], grid=args.mesh,
           seed=args.seed[0] if args.seed is not None else None,
           ncount=args.ncount[0] if args.ncount is not None else None,
           out_dir=args.dir[0] if args.dir is not None else None,
           trace=args.trace,
           gravitation=args.gravitation,
           bufsiz=args.bufsiz[0] if args.bufsiz is not None else None,
           format=args.format[0] if args.format is not None else None)


def single(instr, parameters, split_at=None, grid=False, **runtime_arguments):
    from zenlog import log
    if split_at is None:
        split_at = 'mcpl_split'

    if not instr.has_component_named(split_at):
        log.error(f'The specified split-at component, {split_at}, does not exist in the instrument file')
    # splitting defines an instrument parameter in both returned instrument, 'mcpl_filename'.
    pre, post = instr.mcpl_split(split_at, remove_unused_parameters=True)
    # ... reduce the parameters to those that are relevant to the two instruments.
    pre_parameters = {k: v for k, v in parameters.items() if k in pre.has_parameter(k)}
    post_parameters = {k: v for k, v in parameters.items() if k in post.has_parameter(k)}

    pre_entry = single_pre(pre, pre_parameters, grid, **runtime_arguments,)
    single_combined(pre_entry, post, pre_parameters, post_parameters, grid, **runtime_arguments)


def single_pre(instr, parameters, grid, **runtime_arguments):
    from .cache import cache_instr, cache_has_simulation, cache_simulation
    from .energy import energy_to_chopper_parameters
    from .range import parameters_to_scan
    # check if this instr is already represented in the module's cache database
    # if not, it is compiled and added to the cache with (hopefully sensible) defaults specified
    entry = cache_instr(instr)
    names, scan = parameters_to_scan(energy_to_chopper_parameters(parameters, grid=grid), grid=grid)
    args = regular_mccode_runtime_dict(runtime_arguments)
    for values in scan:
        nv = {n: v for n, v in zip(names, values)}
        sit = SimulationEntry(nv, seed=args.get('seed'), ncount=args.get('ncount'),
                              gravitation=args.get('gravitation', False))
        if not cache_has_simulation(entry, sit):
            sit.output_path = do_primary_simulation(sit, entry, nv, args)
            cache_simulation(entry, sit)
    return entry


def single_combined(pre_entry, post, pre_parameters, post_parameters, grid, **runtime_arguments):
    from .cache import cache_instr, cache_get_simulation
    from .energy import energy_to_chopper_parameters
    from .range import parameters_to_scan
    entry = cache_instr(post)
    args = regular_mccode_runtime_dict(runtime_arguments)
    # recombine the parameters to ensure the 'correct' scan is performed
    # TODO the order of a mesh scan may not be preserved here - is this a problem?
    parameters = {**pre_parameters, **post_parameters}
    names, scan = parameters_to_scan(energy_to_chopper_parameters(parameters, grid=grid), grid=grid)
    for values in scan:
        pars = {n: v for n, v in zip(names, values)}
        # parameters for the secondary instrument:
        secondary_pars = {k: v for k, v in pars.items() if k in post_parameters}
        # use the parameters for the primary instrument to construct a (partial) simulation entry for matching
        primary_sent = SimulationEntry({k: v for k, v in pars.items() if k in pre_parameters},
                                       seed=args.get('seed'), ncount=args.get('ncount'),
                                       gravitation=args.get('gravitation', False))
        # and use it to retrieve the already-simulated primary instrument details:
        primary_simulation_entry = cache_get_simulation(pre_entry, primary_sent)
        do_secondary_simulation(primary_simulation_entry, entry, secondary_pars, runtime_arguments)


def do_primary_simulation(sit: SimulationEntry,
                          instr_file_entry: InstrEntry,
                          parameters: dict,
                          args: dict):
    from zenlog import log
    from pathlib import Path
    from mccode.compiler.c import run_compiled_instrument, CBinaryTarget
    from .cache import module_data_path
    # create a directory for this simulation based on the uuid generated for the simulation entry
    work_dir = module_data_path('sim').joinpath(sit.id)

    if work_dir.exists():
        log.warn('Simulation directory already exists, expect problems with McStas')

    binary_at = Path(instr_file_entry.binary_path)
    target = CBinaryTarget(mpi=False, acc=False, count=1, nexus=False)

    # ensure the primary spectrometer uses our output directory
    args_dict = {k: v for k, v in args.items() if k != 'dir'}
    args_dict['dir'] = work_dir
    # convert the dictionary to a list of arguments, then combine with the parameters
    args = mccode_runtime_dict_to_args_list(args_dict)
    args = ' '.join(args) + ' ' + ' '.join([f'{k}={v}' for k, v in parameters.items()])
    # and append our mcpl_filename parameter
    args += f' mcpl_filename={work_dir.joinpath(sit.id)}.mcpl'
    run_compiled_instrument(binary_at, target, args, capture=False)
    return str(work_dir)


def do_secondary_simulation(p_sit: SimulationEntry, entry: InstrEntry, pars: dict, args: dict[str]):
    from pathlib import Path
    from mccode.compiler.c import run_compiled_instrument, CBinaryTarget

    mcpl_path = Path(p_sit.output_path).joinpath(p_sit.id)
    if mcpl_path.with_suffix('.mcpl').exists():
        mcpl_path = mcpl_path.with_suffix('.mcpl')
    elif mcpl_path.with_suffix('.mcpl.gz').exists():
        mcpl_path = mcpl_path.with_suffix('.mcpl.gz')
    else:
        raise RuntimeError(f"Could not find MCPL file {mcpl_path} with either .mcpl or .mcpl.gz suffixes")

    args = ' '.join(mccode_runtime_dict_to_args_list(args)) + ' ' + ' '.join([f'{k}={v}' for k, v in pars.items()])
    args += f' mcpl_filename={mcpl_path}'

    executable = Path(entry.binary_path)
    target = CBinaryTarget(mpi=False, acc=False, count=1, nexus=False)
    run_compiled_instrument(executable, target, args, capture=False)

