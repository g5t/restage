
def mccode_sim_io(instr, parameters, args: dict):
    from io import StringIO
    from datetime import datetime
    from restage import __version__
    from restage.range import parameters_to_scan
    content = StringIO()
    print('begin instrument:', file=content)
    print(f'  Creator: restage {__version__}', file=content)
    print(f'  Source: {instr.source}', file=content)
    print('  Trace_enabled: no', file=content)
    print('  Default_main: yes', file=content)
    print('  Embeded_runtime: yes', file=content)
    print('end instrument', file=content)
    print(file=content)
    print('begin simulation', file=content)
    print(f'Date: {datetime.now():%a %b %d %H %M %Y}', file=content)
    print(f'Ncount: {args.get("ncount", -1)}', file=content)
    n_pts, names, scan = parameters_to_scan(parameters)
    print(f'Numpoints: {n_pts}', file=content)
    print(f'Param: {", ".join(str(p) for p in parameters)}', file=content)
    print(f'end simulation', file=content)
    print(file=content)
    print('begin data', file=content)
    print(f'type: multiarray_1d({n_pts})', file=content)
    print(f'title: Scan of {", ".join(names)}', file=content)
    print(f'xvars: {", ".join(names)}', file=content)
    print(f'yvars: ', file=content)
    print(f"xlabel: '{', '.join(names)}'", file=content)
    print(f"ylabel: 'Intensity'", file=content)
    #print(f'xlimits : {", ".join(f"{p.min}:{p.max}" for p in parameters)}', file=content)
    print('filename: mccode.dat', file=content)
    print(f'variables: {" ".join(names)}', file=content)
    print('end data', file=content)

    return content


def mccode_dat_io(instr, parameters, args: dict):
    from io import StringIO
    from datetime import datetime
    from restage.range import parameters_to_scan
    content = StringIO()
    n_pts, names, scan = parameters_to_scan(parameters)
    print(f"# Instrument-source: '{instr.source}'", file=content)
    print(f'# Date: {datetime.now():%a %b %d %H %M %Y}', file=content)
    print(f'# Ncount: {args.get("ncount", -1)}', file=content)
    print(f'# Numpoints: {n_pts}', file=content)
    print(f'# Param: {", ".join(str(p) for p in parameters)}', file=content)
    print(f'# type: multiarray_1d({n_pts})', file=content)
    print(f'# title: Scan of {", ".join(names)}', file=content)
    print(f"# xlabel: '{', '.join(names)}'", file=content)
    print(f"# ylabel: 'Intensity'", file=content)
    print(f'# xvars: {", ".join(names)}', file=content)
    print(f'# yvars: ', file=content)
#    print(f'# xlimits : {", ".join(f"{p.min}:{p.max}" for p in parameters)}', file=content)
    print(f'# filename: mccode.dat', file=content)
    print(f'# variables: {" ".join(names)}', file=content)

    return content


def extend_mccode_dat_io(content, directory, parameters):
    # attempt to read the mccode.sim file to extract detector intensities
    # and append them to the mccode.dat file
    from pathlib import Path
    from collections import namedtuple
    Detector = namedtuple('Detector', ['name', 'intensity', 'error', 'count'])
    filepath = Path(directory).joinpath('mccode.sim')
    if not filepath.exists():
        return content
    with filepath.open('r') as file:
        lines = file.read()

    blocks = [x.split('end data')[0].strip() for x in lines.split('begin data') if 'end data' in x]
    blocks = [{k.strip(): v.strip() for k, v in [y.split(':', 1) for y in x.split('\n')]} for x in blocks]
    detectors = [Detector(x['component'], *x['values'].split()) for x in blocks]
    print(f'{" ".join(str(v) for v in parameters.values())} {" ".join(f"{x.intensity} {x.error}" for x in detectors)}',
          file=content)
    return content

