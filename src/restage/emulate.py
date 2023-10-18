from pathlib import Path


def mccode_sim_io(instr, parameters, args: dict, detectors: list[str], file=None):
    from datetime import datetime
    from restage import __version__
    from restage.range import parameters_to_scan
    if file is None:
        from io import StringIO
        file = StringIO()
    print('begin instrument:', file=file)
    print(f'  Creator: restage {__version__}', file=file)
    print(f"  Source: '{instr.source or 'Not provided'}'", file=file)
    print('  Trace_enabled: no', file=file)
    print('  Default_main: yes', file=file)
    print('  Embeded_runtime: yes', file=file)
    print('end instrument', file=file)
    print(file=file)
    print('begin simulation', file=file)
    print(f'Date: {datetime.now():%a %b %d %H %M %Y}', file=file)
    print(f'Ncount: {args.get("ncount", -1)}', file=file)
    n_pts, names, scan = parameters_to_scan(parameters)
    print(f'Numpoints: {n_pts}', file=file)
    print(f'Param: {", ".join(str(p) for p in parameters)}', file=file)
    print(f'end simulation', file=file)
    print(file=file)
    print('begin data', file=file)
    print(f'type: multiarray_1d({n_pts})', file=file)
    print(f'title: Scan of {", ".join(names)}', file=file)
    print(f'xvars: {", ".join(names)}', file=file)
    print(f'yvars: {" ".join(f"({x}_I,{x}_ERR)" for x in detectors)}', file=file)
    print(f"xlabel: '{', '.join(names)}'", file=file)
    print(f"ylabel: 'Intensity'", file=file)
    x = [x for x in parameters.values() if len(x)][0]
    print(f'xlimits: {min(x)} {max(x)}', file=file)  # McCode only uses the first-scanned parameter here
    print('filename: mccode.dat', file=file)
    print(f'variables: {" ".join(names)} {" ".join(f"{x}_I {x}_ERR" for x in detectors)}', file=file)
    print('end data', file=file)

    return file


def mccode_dat_io(instr, parameters, args: dict, detectors: list[str], lines: list[str], file=None):
    from datetime import datetime
    from restage.range import parameters_to_scan
    if file is None:
        from io import StringIO
        file = StringIO()
    n_pts, names, scan = parameters_to_scan(parameters)
    print(f"# Instrument-source: '{instr.source or 'Not Provided'}'", file=file)
    print(f'# Date: {datetime.now():%a %b %d %H %M %Y}', file=file)
    print(f'# Ncount: {args.get("ncount", -1)}', file=file)
    print(f'# Numpoints: {n_pts}', file=file)
    print(f'# Param: {", ".join(str(p) for p in parameters)}', file=file)
    print(f'# type: multiarray_1d({n_pts})', file=file)
    print(f'# title: Scan of {", ".join(names)}', file=file)
    print(f"# xlabel: '{', '.join(names)}'", file=file)
    print(f"# ylabel: 'Intensity'", file=file)
    print(f'# xvars: {", ".join(names)}', file=file)
    print(f'# yvars: {" ".join(f"({x}_I,{x}_ERR)" for x in detectors)}', file=file)
    x = [x for x in parameters.values() if len(x)][0]
    print(f'# xlimits: {min(x)} {max(x)}', file=file)  # McCode only uses the first-scanned parameter here
    print(f'# filename: mccode.dat', file=file)
    print(f'# variables: {" ".join(names)} {" ".join(f"{x}_I {x}_ERR" for x in detectors)}', file=file)
    for line in lines:
        print(line, file=file)
    return file


def mccode_dat_line(directory, parameters):
    from pathlib import Path
    from collections import namedtuple
    Detector = namedtuple('Detector', ['name', 'intensity', 'error', 'count'])
    filepath = Path(directory).joinpath('mccode.sim')
    if not filepath.exists():
        raise RuntimeError(f'No mccode.sim file found in {directory}')
    with filepath.open('r') as file:
        lines = file.read()

    blocks = [x.split('end data')[0].strip() for x in lines.split('begin data') if 'end data' in x]
    blocks = [{k.strip(): v.strip() for k, v in [y.split(':', 1) for y in x.split('\n')]} for x in blocks]
    detectors = [Detector(x['component'], *x['values'].split()) for x in blocks]

    line = f'{" ".join(str(v) for v in parameters.values())} {" ".join(f"{x.intensity} {x.error}" for x in detectors)}'
    names = [x.name for x in detectors]
    return names, line


def combine_mccode_dats_in_directories(directories: list[Path], output: Path):
    from mccode.loader import write_combined_mccode_dats
    dat_names = [x.name for x in directories[0].glob('*.dat')]
    for directory in directories[1:]:
        dir_dat_names = [x.name for x in directory.glob('*.dat')]
        if not all(x in dat_names for x in dir_dat_names):
            raise RuntimeError(f'Extra mccode.dat file(s) are present in {directory}')
        if not all(x in dir_dat_names for x in dat_names):
            raise RuntimeError(f'Not all mccode.dat files are present in {directory}')

    for name in dat_names:
        dat_files = [x.joinpath(name) for x in directories]
        write_combined_mccode_dats(dat_files, output.joinpath(name))


def combine_mccode_sims_in_directories(directories: list[Path], output: Path):
    from mccode.loader import write_combined_mccode_sims
    sim_names = [x.name for x in directories[0].glob('*.sim')]
    for directory in directories[1:]:
        dir_sim_names = [x.name for x in directory.glob('*.sim')]
        if not all(x in sim_names for x in dir_sim_names):
            raise RuntimeError(f'Extra mccode.sim file(s) are present in {directory}')
        if not all(x in dir_sim_names for x in sim_names):
            raise RuntimeError(f'Not all mccode.sim files are present in {directory}')

    for name in sim_names:
        sim_files = [x.joinpath(name) for x in directories]
        write_combined_mccode_sims(sim_files, output.joinpath(name))