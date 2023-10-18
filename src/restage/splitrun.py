from typing import Union
from .range import Singular, MRange
from .tables import SimulationEntry, InstrEntry


def make_splitrun_parser():
    from argparse import ArgumentParser
    parser = ArgumentParser('splitrun')
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


def get_best_of(src: dict, names: tuple):
    for name in names:
        if name in src:
            return src[name]
    raise RuntimeError(f"None of {names} found in {src}")


def insert_best_of(src: dict, snk: dict, names: tuple):
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


def mccode_runtime_dict_to_args_list(args: dict) -> list[str]:
    """Convert a dictionary of McCode runtime arguments to a string.

    :parameter args: A dictionary of McCode runtime arguments.
    :return: A list of arguments suitable for use in a command line call to a McCode compiled instrument.
    """
    # convert to a standardized string:
    out = []
    if 'seed' in args and args['seed'] is not None:
        out.append(f'--seed={args["seed"]}')
    if 'ncount' in args and args['ncount'] is not None:
        out.append(f'--ncount={args["ncount"]}')
    if 'dir' in args and args['dir'] is not None:
        out.append(f'--dir={args["dir"]}')
    if 'trace' in args and args['trace']:
        out.append('--trace')
    if 'gravitation' in args and args['gravitation']:
        out.append('--gravitation')
    if 'bufsiz' in args and args['bufsiz'] is not None:
        out.append(f'--bufsiz={args["bufsiz"]}')
    if 'format' in args and args['format'] is not None:
        out.append(f'--format={args["format"]}')
    return out


def parse_splitrun_parameters(unparsed: list[str]) -> dict[str, Union[MRange, Singular]]:
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


def parse_splitrun():
    args = make_splitrun_parser().parse_args()
    parameters = parse_splitrun_parameters(args.parameters)
    return args, parameters


def entrypoint():
    args, parameters = parse_splitrun()
    splitrun_from_file(args, parameters)


def splitrun_from_file(args, parameters):
    from mccode.loader import load_mcstas_instr
    instr = load_mcstas_instr(args.instrument[0])
    splitrun(instr, parameters, split_at=args.split_at[0], grid=args.mesh,
             seed=args.seed[0] if args.seed is not None else None,
             ncount=args.ncount[0] if args.ncount is not None else None,
             out_dir=args.dir[0] if args.dir is not None else None,
             trace=args.trace,
             gravitation=args.gravitation,
             bufsiz=args.bufsiz[0] if args.bufsiz is not None else None,
             format=args.format[0] if args.format is not None else None)


def splitrun(instr, parameters, split_at=None, grid=False, **runtime_arguments):
    from zenlog import log
    if split_at is None:
        split_at = 'mcpl_split'

    if not instr.has_component_named(split_at):
        log.error(f'The specified split-at component, {split_at}, does not exist in the instrument file')
    # splitting defines an instrument parameter in both returned instrument, 'mcpl_filename'.
    pre, post = instr.mcpl_split(split_at, remove_unused_parameters=True)
    # ... reduce the parameters to those that are relevant to the two instruments.
    pre_parameters = {k: v for k, v in parameters.items() if pre.has_parameter(k)}
    post_parameters = {k: v for k, v in parameters.items() if post.has_parameter(k)}

    pre_entry = splitrun_pre(pre, pre_parameters, grid, **runtime_arguments,)
    splitrun_combined(pre_entry, pre, post, pre_parameters, post_parameters, grid, **runtime_arguments)


def splitrun_pre(instr, parameters, grid, **runtime_arguments):
    from .cache import cache_instr, cache_has_simulation, cache_simulation
    from .energy import get_energy_to_chopper_translator
    from .range import parameters_to_scan
    from .instr import collect_parameter_dict
    # check if this instr is already represented in the module's cache database
    # if not, it is compiled and added to the cache with (hopefully sensible) defaults specified
    entry = cache_instr(instr)
    energy_to_chopper_parameters = get_energy_to_chopper_translator(instr.name)
    n_pts, names, scan = parameters_to_scan(energy_to_chopper_parameters(parameters, grid=grid), grid=grid)
    args = regular_mccode_runtime_dict(runtime_arguments)
    sit_kw = {'seed': args.get('seed'), 'ncount': args.get('ncount'), 'gravitation': args.get('gravitation', False)}
    if n_pts == 0:
        # no scan required, ensure the 'all-defaults' simulation entry is cached
        sit = SimulationEntry(collect_parameter_dict(instr, {}), **sit_kw)
        if not cache_has_simulation(entry, sit):
            sit.output_path = do_primary_simulation(sit, entry, parameters, args)
            cache_simulation(entry, sit)
    for values in scan:
        nv = {n: v for n, v in zip(names, values)}
        sit = SimulationEntry(collect_parameter_dict(instr, nv), **sit_kw)
        if not cache_has_simulation(entry, sit):
            sit.output_path = do_primary_simulation(sit, entry, nv, args)
            cache_simulation(entry, sit)
    return entry


def splitrun_combined(pre_entry, pre, post, pre_parameters, post_parameters, grid, summary=True, **runtime_arguments):
    from .cache import cache_instr, cache_get_simulation
    from .energy import get_energy_to_chopper_translator
    from .range import parameters_to_scan
    from .instr import collect_parameter_dict
    from .tables import best_simulation_entry_match
    from .emulate import mccode_sim_io, mccode_dat_io, mccode_dat_line
    instr_entry = cache_instr(post)
    args = regular_mccode_runtime_dict(runtime_arguments)
    sit_kw = {'seed': args.get('seed'), 'ncount': args.get('ncount'), 'gravitation': args.get('gravitation', False)}
    # recombine the parameters to ensure the 'correct' scan is performed
    # TODO the order of a mesh scan may not be preserved here - is this a problem?
    parameters = {**pre_parameters, **post_parameters}
    energy_to_chopper_parameters = get_energy_to_chopper_translator(post.name)
    n_pts, names, scan = parameters_to_scan(energy_to_chopper_parameters(parameters, grid=grid), grid=grid)
    n_zeros = len(str(n_pts))  # we could use math.log10(n_pts) + 1, but why not use a hacky solution?

    detectors, dat_lines = [], []
    for number, values in enumerate(scan):
        pars = {n: v for n, v in zip(names, values)}
        # parameters for the secondary instrument:
        secondary_pars = {k: v for k, v in pars.items() if k in post_parameters}
        # use the parameters for the primary instrument to construct a (partial) simulation entry for matching
        table_parameters = collect_parameter_dict(pre, {k: v for k, v in pars.items() if k in pre_parameters})
        primary_sent = SimulationEntry(table_parameters, **sit_kw)
        # and use it to retrieve the already-simulated primary instrument details:
        sim_entry = best_simulation_entry_match(cache_get_simulation(pre_entry, primary_sent), primary_sent)
        # now we can use the best primary simulation entry to perform the secondary simulation
        # but because McCode refuses to use a specified output directory if it is not empty,
        # we need to update the runtime_arguments first!
        # TODO Use the following line instead of the one after it when McCode is fixed to use zero-padded folder names
        # # runtime_arguments['dir'] = args["dir"].joinpath(str(number).zfill(n_zeros))
        runtime_arguments['dir'] = args['dir'].joinpath(str(number))
        do_secondary_simulation(sim_entry, instr_entry, secondary_pars, runtime_arguments)
        if summary:
            detectors, line = mccode_dat_line(runtime_arguments['dir'], secondary_pars)
            dat_lines.append(line)

    if summary:
        with args['dir'].joinpath('mccode.sim').open('w') as f:
            mccode_sim_io(post, parameters, args, detectors, file=f)
        with args['dir'].joinpath('mccode.dat').open('w') as f:
            mccode_dat_io(post, parameters, args, detectors, dat_lines, file=f)


def _args_pars_mcpl(args: dict, params: dict, mcpl_filename) -> str:
    # Combine the arguments, parameters, and mcpl filename into a single command-arguments string:
    first = ' '.join(mccode_runtime_dict_to_args_list(args))
    second = ' '.join([f'{k}={v}' for k, v in params.items()])
    third = f'mcpl_filename={mcpl_filename}'
    return ' '.join((first, second, third))


def do_primary_simulation(sit: SimulationEntry,
                          instr_file_entry: InstrEntry,
                          parameters: dict,
                          args: dict, repeat_particle_count: int = 10_000):
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
    # and append our mcpl_filename parameter
    mcpl_filename = f'{work_dir.joinpath(sit.id)}.mcpl'
    if not repeat_particle_count or args.get('ncount') is None:
        # convert the dictionary to a list of arguments, then combine with the parameters
        args_dict['dir'] = work_dir
        run_compiled_instrument(binary_at, target, _args_pars_mcpl(args_dict, parameters, mcpl_filename), capture=False)
    else:
        from .emulate import combine_mccode_dats_in_directories, combine_mccode_sims_in_directories
        from .mcpl import mcpl_particle_count, mcpl_merge_files
        remaining, count, latest_result = args['ncount'], args['ncount'], -1
        files, outputs = [], []
        # Normally we _don't_ create `work_dir` to avoid McStas complaining about the directory existing
        # but in this case we will use subdirectories for the actuall output files, so we need to create it
        work_dir.mkdir(parents=True)
        # ensure we have a standardized dictionary
        args_dict = regular_mccode_runtime_dict(args_dict)
        # check for the presence of a defined seed; which _can_not_ be used for repeated simulations:
        import random
        if 'seed' in args_dict:
            random.seed(args_dict['seed'])

        while remaining > 0:
            if latest_result == 0:
                log.warn(f'No particles emitted in previous run, stopping')
                break
            elif latest_result > 0:
                # update the remaining particle count and adjust our guess for how many particles to simulate
                remaining -= latest_result
                count = (remaining * args_dict['ncount']) // latest_result
            if 'seed' in args_dict:
                args_dict['seed'] = random.random()
            args_dict['ncount'] = max(count, repeat_particle_count)
            outputs.append(work_dir.joinpath(f'{len(files)}'))
            files.append(work_dir.joinpath(f'part_{len(files)}.mcpl'))
            args_dict['dir'] = outputs[-1]
            run_compiled_instrument(binary_at, target, _args_pars_mcpl(args_dict, parameters, files[-1]), capture=False)
            latest_result = mcpl_particle_count(files[-1])
        # now we need to concatenate the mcpl files
        mcpl_merge_files(files, mcpl_filename)
        # and merge any output (.dat) files
        combine_mccode_dats_in_directories(outputs, work_dir)
        # ... plus the mccode.sim file
        combine_mccode_sims_in_directories(outputs, work_dir)
    return str(work_dir)


def do_secondary_simulation(p_sit: SimulationEntry, entry: InstrEntry, pars: dict, args: dict[str]):
    from pathlib import Path
    from mccode.compiler.c import run_compiled_instrument, CBinaryTarget
    from .mcpl import mcpl_real_filename

    mcpl_path = mcpl_real_filename(Path(p_sit.output_path).joinpath(p_sit.id))
    executable = Path(entry.binary_path)
    target = CBinaryTarget(mpi=False, acc=False, count=1, nexus=False)
    run_compiled_instrument(executable, target, _args_pars_mcpl(args, pars, mcpl_path), capture=False)

