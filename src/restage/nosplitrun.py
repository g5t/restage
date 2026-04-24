from __future__ import annotations

from pathlib import Path

from .tables import SimulationEntry, InstrEntry


def make_nosplitrun_parser():
    from .splitrun import make_splitrun_parser
    parser = make_splitrun_parser()
    parser.prog = 'nosplitrun'
    parser.description = (
        'Run an instrument simulation without MCPL splitting, for comparison with splitrun. '
        'The --split-at and --mcpl-* arguments are accepted but ignored.'
    )
    return parser


def entrypoint():
    from .splitrun import parse_splitrun
    args, parameters, precision = parse_splitrun(make_nosplitrun_parser())
    nosplitrun_from_file(args, parameters, precision)


def nosplitrun_from_file(args, parameters, precision):
    from .instr import load_instr
    instr = load_instr(args.instrument)
    nosplitrun_args(instr, parameters, precision, args)


def nosplitrun_args(instr, parameters, precision, args, **kwargs):
    from .splitrun import regular_mccode_runtime_dict
    nosplitrun(
        instr, parameters, precision,
        grid=args.mesh,
        seed=args.seed,
        ncount=args.ncount,
        out_dir=args.dir,
        trace=args.trace,
        gravitation=args.gravitation,
        bufsiz=args.bufsiz,
        format=args.format,
        dry_run=args.dryrun,
        parallel=args.parallel,
        gpu=args.gpu,
        process_count=args.process_count,
        progress=args.progress,
        **kwargs,
    )


def nosplitrun(instr, parameters, precision: dict[str, float],
               grid: bool = False,
               dry_run: bool = False,
               parallel: bool = False,
               gpu: bool = False,
               process_count: int = 0,
               progress: bool = False,
               summary: bool = True,
               callback=None,
               callback_arguments: dict[str, str] | None = None,
               **runtime_arguments):
    """Run the full (unsplit) instrument for each scan point.

    This mirrors the behaviour of :func:`splitrun` but without any MCPL
    intermediate file.  Use it to compare simulation results with and without
    the split optimisation.

    :param instr: Compiled instrument object.
    :param parameters: Scan parameters (from :func:`parse_scan_parameters`).
    :param precision: Per-parameter cache matching tolerance.
    :param grid: If True, perform a mesh/grid scan.
    :param dry_run: Print commands without executing them.
    :param parallel: Use MPI parallelism.
    :param gpu: Use GPU/OpenACC parallelism.
    :param process_count: Number of MPI processes (0 = system default).
    :param progress: Show a tqdm progress bar; simulation output is redirected
        to ``sim.log`` in each run's working directory.
    :param summary: Write ``mccode.sim`` / ``mccode.dat`` summary files.
    :param callback: Optional callable invoked after each scan point.
    :param callback_arguments: Mapping from internal arg names to callback kwarg names.
    :param runtime_arguments: Passed through to the McCode runtime (ncount, seed, dir, …).
    """
    from tqdm.auto import tqdm
    from mccode_antlr.compiler.c import run_compiled_instrument, CBinaryTarget
    from mccode_antlr.run.range import parameters_to_scan
    from .cache import cache_instr
    from .energy import energy_to_chopper_translator, get_energy_parameter_names
    from .emulate import mccode_sim_io, mccode_dat_io, mccode_dat_line
    from .instr import collect_parameter_dict
    from .splitrun import regular_mccode_runtime_dict, _run_and_log, _args_pars_direct

    # Compile / retrieve from cache
    entry: InstrEntry = cache_instr(instr, mpi=parallel, acc=gpu)

    args = regular_mccode_runtime_dict(runtime_arguments)
    sit_kw = {'seed': args.get('seed'), 'ncount': args.get('ncount'), 'gravitation': args.get('gravitation', False)}

    # Energy → chopper translation (no-op for unknown instruments)
    translate = energy_to_chopper_translator(instr.name)
    energy_parameter_names = get_energy_parameter_names(instr.name)

    n_pts, names, scan = parameters_to_scan(parameters, grid=grid)

    # Build output root directory
    if args.get('dir') is None:
        from datetime import datetime
        args['dir'] = Path().resolve() / f'{instr.name}{datetime.now():%Y%m%d_%H%M%S}'
    root_dir = Path(args['dir'])
    if not root_dir.exists():
        root_dir.mkdir(parents=True)

    target = CBinaryTarget(mpi=entry.mpi, acc=entry.acc, count=process_count, nexus=False)
    binary_at = Path(entry.binary_path)

    detectors, dat_lines = [], []
    scan_iter = tqdm(enumerate(scan), desc='nosplitrun', total=n_pts, unit='point', disable=not progress)
    for number, values in scan_iter:
        pars = translate({n: v for n, v in zip(names, values)})
        # include energy parameters if present
        if any(x in parameters for x in energy_parameter_names):
            pars.update({k: v for k, v in parameters.items() if k in energy_parameter_names})
        instr_pars = collect_parameter_dict(instr, pars)

        work_dir = root_dir / str(number)
        run_args = {k: v for k, v in args.items() if k != 'dir'}
        run_args['dir'] = work_dir

        cmd = _args_pars_direct(run_args, instr_pars)
        runner = lambda c: run_compiled_instrument(binary_at, target, c,
                                                   capture=progress, dry_run=dry_run)
        _run_and_log(runner, cmd, work_dir, progress)

        if summary and not dry_run:
            detectors, line = mccode_dat_line(work_dir, {k: v for k, v in zip(names, values)})
            dat_lines.append(line)

        if callback is not None:
            cb_args = {}
            arg_names = names + ['number', 'n_pts', 'pars', 'dir', 'arguments']
            arg_values = list(values) + [number, n_pts, pars, work_dir, runtime_arguments]
            for x, v in zip(arg_names, arg_values):
                if callback_arguments is not None and x in callback_arguments:
                    cb_args[callback_arguments[x]] = v
            callback(**cb_args)

    if n_pts == 0:
        # single no-parameter run
        work_dir = root_dir / '0'
        run_args = {k: v for k, v in args.items() if k != 'dir'}
        run_args['dir'] = work_dir
        cmd = _args_pars_direct(run_args, collect_parameter_dict(instr, {}))
        runner = lambda c: run_compiled_instrument(binary_at, target, c,
                                                   capture=progress, dry_run=dry_run)
        _run_and_log(runner, cmd, work_dir, progress)
        if summary and not dry_run:
            detectors, line = mccode_dat_line(work_dir, {})
            dat_lines.append(line)

    if summary and not dry_run:
        with root_dir.joinpath('mccode.sim').open('w') as f:
            mccode_sim_io(instr, parameters, args, detectors, file=f, grid=grid)
        with root_dir.joinpath('mccode.dat').open('w') as f:
            mccode_dat_io(instr, parameters, args, detectors, dat_lines, file=f, grid=grid)
