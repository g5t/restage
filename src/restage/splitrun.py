from __future__ import annotations

from typing import Union
from pathlib import Path
from .range import Singular, MRange
from .tables import SimulationEntry, InstrEntry


def make_splitrun_parser():
    from argparse import ArgumentParser
    parser = ArgumentParser('splitrun')
    aa = parser.add_argument
    parser = ArgumentParser('splitrun')
    aa = parser.add_argument
    aa('instrument', nargs=1, type=str, default=None,
       help='Instrument `.instr` file name or serialised HDF5 Instr object')
    aa('parameters', nargs='*', type=str, default=None)
    aa('-n', '--ncount', nargs=1, type=int, default=None, help='Number of neutrons to simulate')
    aa('-m', '--mesh', action='store_true', default=False, help='N-dimensional mesh scan')
    aa('-d', '--dir', nargs=1, type=str, default=None, help='Output directory')
    aa('-s', '--seed', nargs=1, type=int, default=None, help='Random number generator seed')
    aa('-t', '--trace', action='store_true', default=False, help='Enable tracing')
    aa('-g', '--gravitation', action='store_true', default=False,
       help='Enable gravitation for all trajectories')
    aa('--bufsiz', nargs=1, type=int, default=None, help='Monitor_nD list/buffer-size')
    aa('--format', nargs=1, type=str, default=None, help='Output data files using FORMAT')
    aa('--nmin', nargs=1, type=int, default=None,
       help='Minimum number of particles to simulate during first instrument simulations')
    aa('--nmax', nargs=1, type=int, default=None,
       help='Maximum number of particles to simulate during first instrument simulations')
    aa('--dryrun', action='store_true', default=False,
       help='Do not run any simulations, just print the commands')
    # splitrun controlling parameters
    aa('--split-at', nargs=1, type=str, default=['mcpl_split'],
       help='Component at which to split -- DEFAULT: mcpl_split')
    aa('-P', action='append', default=[], help='Cache parameter matching precision')

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


def parse_splitrun_precision(unparsed: list[str]) -> dict[str, float]:
    precision = {}
    for p in unparsed:
        if '=' not in p:
            raise ValueError(f'Invalid precision specification: {p}')
        k, v = p.split('=', 1)
        precision[k] = float(v)
    return precision


def sort_args(args: list[str]) -> list[str]:
    """Take the list of arguments and sort them into the correct order for splitrun"""
    # TODO this is a bit of a hack, but it works for now
    first, last = [], []
    k = 0
    while k < len(args):
        if args[k].startswith('-'):
            first.append(args[k])
            k += 1
            if '=' not in first[-1] and k < len(args) and not args[k].startswith('-') and '=' not in args[k]:
                first.append(args[k])
                k += 1
        else:
            last.append(args[k])
            k += 1
    return first + last


def parse_splitrun():
    from .range import parse_scan_parameters
    import sys
    sys.argv[1:] = sort_args(sys.argv[1:])

    args = make_splitrun_parser().parse_args()
    parameters = parse_scan_parameters(args.parameters)
    precision = parse_splitrun_precision(args.P)
    return args, parameters, precision


def entrypoint():
    args, parameters, precision = parse_splitrun()
    splitrun_from_file(args, parameters, precision)


def splitrun_from_file(args, parameters, precision):
    from .instr import load_instr
    instr = load_instr(args.instrument[0])
    splitrun(instr, parameters, precision, split_at=args.split_at[0], grid=args.mesh,
             seed=args.seed[0] if args.seed is not None else None,
             ncount=args.ncount[0] if args.ncount is not None else None,
             out_dir=args.dir[0] if args.dir is not None else None,
             trace=args.trace,
             gravitation=args.gravitation,
             bufsiz=args.bufsiz[0] if args.bufsiz is not None else None,
             format=args.format[0] if args.format is not None else None,
             minimum_particle_count=args.nmin[0] if args.nmin is not None else None,
             maximum_particle_count=args.nmax[0] if args.nmax is not None else None,
             dry_run=args.dryrun
             )


def splitrun(instr, parameters, precision: dict[str, float], split_at=None, grid=False,
             minimum_particle_count=None,
             maximum_particle_count=None,
             dry_run=False,
             callback=None, callback_arguments: dict[str, str] | None = None,
             **runtime_arguments):
    from zenlog import log
    from .energy import get_energy_parameter_names
    if split_at is None:
        split_at = 'mcpl_split'

    if not instr.has_component_named(split_at):
        log.error(f'The specified split-at component, {split_at}, does not exist in the instrument file')
    # splitting defines an instrument parameter in both returned instrument, 'mcpl_filename'.
    pre, post = instr.mcpl_split(split_at, remove_unused_parameters=True)
    # ... reduce the parameters to those that are relevant to the two instruments.
    pre_parameters = {k: v for k, v in parameters.items() if pre.has_parameter(k)}
    post_parameters = {k: v for k, v in parameters.items() if post.has_parameter(k)}

    energy_parameter_names = get_energy_parameter_names(instr.name)
    if any(x in parameters for x in energy_parameter_names):
        # these are special parameters which are used to calculate the chopper parameters
        # in the primary instrument
        pre_parameters.update({k: v for k, v in parameters.items() if k in energy_parameter_names})

    pre_entry = splitrun_pre(pre, pre_parameters, grid, precision, **runtime_arguments,
                             minimum_particle_count=minimum_particle_count,
                             maximum_particle_count=maximum_particle_count,
                             dry_run=dry_run)

    splitrun_combined(pre_entry, pre, post, pre_parameters, post_parameters, grid, precision,
                      dry_run=dry_run, callback=callback, callback_arguments=callback_arguments, **runtime_arguments)


def splitrun_pre(instr, parameters, grid, precision: dict[str, float],
                 minimum_particle_count=None, maximum_particle_count=None, dry_run=False,
                 **runtime_arguments):

    from functools import partial
    from .cache import cache_instr
    from .energy import energy_to_chopper_translator
    from .range import parameters_to_scan

    # check if this instr is already represented in the module's cache database
    # if not, it is compiled and added to the cache with (hopefully sensible) defaults specified
    entry = cache_instr(instr)
    # get the function with converts energy parameters to chopper parameters:
    translate = energy_to_chopper_translator(instr.name)
    # determine the scan in the user-defined parameters!
    n_pts, names, scan = parameters_to_scan(parameters, grid=grid)
    args = regular_mccode_runtime_dict(runtime_arguments)
    sit_kw = {'seed': args.get('seed'), 'ncount': args.get('ncount'), 'gravitation': args.get('gravitation', False)}

    step = partial(_pre_step, instr, entry, names, precision, translate, sit_kw, minimum_particle_count,
                   maximum_particle_count, dry_run)

    # this does not work due to the sqlite database being locked by the parallel processes
    # from joblib import Parallel, delayed
    # Parallel(n_jobs=-3)(delayed(step)(values) for values in scan)

    for values in scan:
        step(values)
    if n_pts == 0:
        # If the parameters are empty, we still need to run the simulation once:
        step([])
    return entry


def _pre_step(instr, entry, names, precision, translate, kw, min_pc, max_pc, dry_run, values):
    """The per-step function for the primary instrument simulation. Broken out for parallelization"""
    from .instr import collect_parameter_dict
    from .cache import cache_has_simulation, cache_simulation, cache_get_simulation
    nv = translate({n: v for n, v in zip(names, values)})
    sim = SimulationEntry(collect_parameter_dict(instr, nv), precision=precision, **kw)
    if not cache_has_simulation(entry, sim):
        sim.output_path = do_primary_simulation(sim, entry, nv, kw,
                                                minimum_particle_count=min_pc,
                                                maximum_particle_count=max_pc,
                                                dry_run=dry_run)
        cache_simulation(entry, sim)
    return cache_get_simulation(entry, sim)


def splitrun_combined(pre_entry, pre, post, pre_parameters, post_parameters, grid, precision: dict[str, float],
                      summary=True, dry_run=False, callback=None, callback_arguments: dict[str, str] | None = None,
                      **runtime_arguments):
    from pathlib import Path
    from .cache import cache_instr, cache_get_simulation
    from .energy import energy_to_chopper_translator
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
    n_pts, names, scan = parameters_to_scan(parameters, grid=grid)
    n_zeros = len(str(n_pts))  # we could use math.log10(n_pts) + 1, but why not use a hacky solution?

    # Ensure _an_ output folder is created for the run, even if the user did not specify one.
    # TODO Fix this hack
    if args.get('dir') is None:
        from os.path import commonprefix
        from datetime import datetime
        instr_name = commonprefix((pre.name, post.name))
        args['dir'] = Path().resolve().joinpath(f'{instr_name}{datetime.now():%Y%m%d_%H%M%S}')

    if not Path(args['dir']).exists():
        Path(args['dir']).mkdir(parents=True)

    detectors, dat_lines = [], []
    # get the function that performs the translation (or no-op if the instrument name is unknown)
    translate = energy_to_chopper_translator(post.name)
    for number, values in enumerate(scan):
        # convert, e.g., energy parameters to chopper parameters:
        pars = translate({n: v for n, v in zip(names, values)})
        # parameters for the primary instrument:
        primary_pars = {k: v for k, v in pars.items() if k in pre_parameters}
        # parameters for the secondary instrument:
        secondary_pars = {k: v for k, v in pars.items() if k in post_parameters}
        # use the parameters for the primary instrument to construct a (partial) simulation entry for matching
        primary_table_parameters = collect_parameter_dict(pre, primary_pars, strict=True)
        primary_sent = SimulationEntry(primary_table_parameters, precision=precision, **sit_kw)
        # and use it to retrieve the already-simulated primary instrument details:
        sim_entry = best_simulation_entry_match(cache_get_simulation(pre_entry, primary_sent), primary_sent)
        # now we can use the best primary simulation entry to perform the secondary simulation
        # but because McCode refuses to use a specified output directory if it is not empty,
        # we need to update the runtime_arguments first!
        # TODO Use the following line instead of the one after it when McCode is fixed to use zero-padded folder names
        # # runtime_arguments['dir'] = args["dir"].joinpath(str(number).zfill(n_zeros))
        runtime_arguments['dir'] = args['dir'].joinpath(str(number))
        do_secondary_simulation(sim_entry, instr_entry, secondary_pars, runtime_arguments, dry_run=dry_run)
        if summary and not dry_run:
            # the data file has *all* **scanned** parameters recorded for each step:
            detectors, line = mccode_dat_line(runtime_arguments['dir'], {k: v for k,v in zip(names, values)})
            dat_lines.append(line)
        if callback is not None:
            arguments = {}
            arg_names = names + ['number', 'n_pts', 'pars', 'dir', 'arguments']
            arg_values = values + [number, n_pts, pars, runtime_arguments['dir'], runtime_arguments]
            for x, v in zip(arg_names, arg_values):
                if callback_arguments is not None and x in callback_arguments:
                    arguments[callback_arguments[x]] = v
            callback(**arguments)

    if summary and not dry_run:
        with args['dir'].joinpath('mccode.sim').open('w') as f:
            mccode_sim_io(post, parameters, args, detectors, file=f, grid=grid)
        with args['dir'].joinpath('mccode.dat').open('w') as f:
            mccode_dat_io(post, parameters, args, detectors, dat_lines, file=f, grid=grid)


def _args_pars_mcpl(args: dict, params: dict, mcpl_filename) -> str:
    # Combine the arguments, parameters, and mcpl filename into a single command-arguments string:
    first = ' '.join(mccode_runtime_dict_to_args_list(args))
    second = ' '.join([f'{k}={v}' for k, v in params.items()])
    third = f'mcpl_filename={mcpl_filename}'
    return ' '.join((first, second, third))


def _clamp(minimum, maximum, value):
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value


def do_primary_simulation(sit: SimulationEntry,
                          instr_file_entry: InstrEntry,
                          parameters: dict,
                          args: dict,
                          minimum_particle_count: int | None = None,
                          maximum_particle_count: int | None = None,
                          dry_run: bool = False
                          ):
    from zenlog import log
    from pathlib import Path
    from functools import partial
    from mccode_antlr.compiler.c import run_compiled_instrument, CBinaryTarget
    from .cache import directory_under_module_data_path
    # create a directory for this simulation based on the uuid generated for the simulation entry
    work_dir = directory_under_module_data_path('sim', prefix=f'p_{instr_file_entry.id}_')

    binary_at = Path(instr_file_entry.binary_path)
    target = CBinaryTarget(mpi=False, acc=False, count=1, nexus=False)

    # ensure the primary spectrometer uses our output directory
    args_dict = {k: v for k, v in args.items() if k != 'dir'}
    # and append our mcpl_filename parameter
    # TODO update the SimulationTable entry to use this filename too
    #   If you do, make sure the cache query ignores filenames?
    if 'mcpl_filename' in sit.parameter_values and sit.parameter_values['mcpl_filename'].is_str and \
            sit.parameter_values['mcpl_filename'].value is not None and \
            len(sit.parameter_values['mcpl_filename'].value):
        mcpl_filename = sit.parameter_values['mcpl_filename'].value.strip('"')
    else:
        from .tables import Value
        log.info('Expected mcpl_filename parameter in primary simulation, using default')
        mcpl_filename = f'{sit.id}.mcpl'
        sit.parameter_values['mcpl_filename'] = Value.str(mcpl_filename)
    mcpl_filepath = work_dir.joinpath(mcpl_filename)
    runner = partial(run_compiled_instrument, binary_at, target, capture=False, dry_run=dry_run)
    if dry_run or args.get('ncount') is None:
        if work_dir.exists():
            if any(work_dir.iterdir()):
                log.warn('Simulation directory already exists and is not empty, expect problems with runtime')
            else:
                # No warning since we made the directory above :/
                work_dir.rmdir()
        # convert the dictionary to a list of arguments, then combine with the parameters
        args_dict['dir'] = work_dir
        runner(_args_pars_mcpl(args_dict, parameters, mcpl_filepath))
    else:
        repeat_simulation_until(args['ncount'], runner, args_dict, parameters, work_dir, mcpl_filepath, minimum_particle_count, maximum_particle_count)
    return str(work_dir)


def repeat_simulation_until(count, runner, args: dict, parameters, work_dir: Path, mcpl_filepath: Path,
                            minimum_particle_count: int | None = None,
                            maximum_particle_count: int | None = None):
    import random
    from functools import partial
    from zenlog import log
    from .emulate import combine_mccode_dats_in_directories, combine_mccode_sims_in_directories
    from .mcpl import mcpl_particle_count, mcpl_merge_files
    goal, latest_result, one_trillion = count, -1, 1_000_000_000_000
    # avoid looping for too long by limiting the minimum number of particles to simulate
    minimum_particle_count = _clamp(1, one_trillion, minimum_particle_count or count)
    # avoid any one loop iteration from taking too long by limiting the maximum number of particles to simulate
    clamp = partial(_clamp, minimum_particle_count,
                    _clamp(minimum_particle_count, one_trillion, maximum_particle_count or count))

    # Normally we _don't_ create `work_dir` to avoid complaints about the directory existing but in this case
    # we will use subdirectories for the actual output files, so we need to create it
    if not work_dir.exists():
        work_dir.mkdir(parents=True)
    # ensure we have a standardized dictionary
    args = regular_mccode_runtime_dict(args)
    # check for the presence of a defined seed; which _can_not_ be used for repeated simulations:
    if 'seed' in args and args['seed'] is not None:
        random.seed(args['seed'])

    files, outputs, counts = [], [], []
    while goal - sum(counts) > 0:
        if len(counts) and counts[-1] <= 0:
            log.warn(f'No particles emitted in previous run, stopping')
            break

        if 'seed' in args:
            args['seed'] = random.randint(1, 2 ** 32 - 1)

        outputs.append(work_dir.joinpath(f'{len(files)}'))
        files.append(work_dir.joinpath(f'part_{len(files)}.mcpl'))
        args['dir'] = outputs[-1]
        # adjust our guess for how many particles to simulate : how many we need divided by the last transmission
        args['ncount'] = clamp(((goal - sum(counts)) * args['ncount']) // counts[-1] if len(counts) else goal)
        runner(_args_pars_mcpl(args, parameters, files[-1]))
        counts.append(mcpl_particle_count(files[-1]))

    # now we need to concatenate the mcpl files, and combine output (.dat and .sim) files
    mcpl_merge_files(files, str(mcpl_filepath))
    combine_mccode_dats_in_directories(outputs, work_dir)
    combine_mccode_sims_in_directories(outputs, work_dir)


def do_secondary_simulation(p_sit: SimulationEntry, entry: InstrEntry, pars: dict, args: dict,
                            dry_run: bool = False):
    from zenlog import log
    from pathlib import Path
    from shutil import copy
    from mccode_antlr.compiler.c import run_compiled_instrument, CBinaryTarget
    from .mcpl import mcpl_real_filename
    from mccode_antlr.loader import write_combined_mccode_sims

    if 'mcpl_filename' in p_sit.parameter_values and p_sit.parameter_values['mcpl_filename'].is_str and \
            p_sit.parameter_values['mcpl_filename'].value is not None and \
            len(p_sit.parameter_values['mcpl_filename'].value):
        mcpl_filename = p_sit.parameter_values['mcpl_filename'].value.strip('"')
    else:
        log.info('Expected mcpl_filename parameter in secondary simulation, using default')
        mcpl_filename = f'{p_sit.id}.mcpl'

    mcpl_path = mcpl_real_filename(Path(p_sit.output_path).joinpath(mcpl_filename))
    executable = Path(entry.binary_path)
    target = CBinaryTarget(mpi=False, acc=False, count=1, nexus=False)
    run_compiled_instrument(executable, target, _args_pars_mcpl(args, pars, mcpl_path), capture=False, dry_run=dry_run)

    if not dry_run:
        # Copy the primary simulation's .dat file to the secondary simulation's directory and combine .sim files?
        work_dir = Path(args['dir'])
        for dat in Path(p_sit.output_path).glob('*.dat'):
            copy(dat, work_dir.joinpath(dat.name))
        p_sim = Path(p_sit.output_path).joinpath('mccode.sim')
        s_sim = work_dir.joinpath('mccode.sim')
        if p_sim.exists() and s_sim.exists():
            write_combined_mccode_sims([p_sim, s_sim], s_sim)
