"""Collector files from mcstas-readout-master, across restage's two stages.

A Collector component writes weighted-ray records into an HDF5 file in the simulation's
output directory, for replay to Event Formation Units after the run. restage runs an
instrument in two stages, and both leave collector files in places nothing else looks:

- The primary stage is repeated until enough rays reach the split point, each pass in its
  own numbered directory. Its collector files have to be combined like its ``.dat`` files,
  or the cached primary holds only fragments. `merge_collector_passes` does that, with the
  library's own ``append``, which sums records and normalizations so the per-ray weight
  stays the estimate from all passes together.

- One primary serves every scan point that shares its parameters, so each point's
  secondary collector file has a partner in the primary's cache directory. A replayable
  scan needs both in every point -- the beam monitors before the split saw that point's
  beam as much as the detectors after it did. `assemble_collector_scan` builds one
  multi-point file per collector file name.

Copying the primary's records into every point would store the same records once per
point: a 180-point rotation scan served by one primary would hold 180 copies of what is
already the largest part of the data. Instead each distinct primary's records are stored
once, in a ``stored_readouts`` dataset inside the collector group, and the group's
``readouts`` is an HDF5 virtual dataset that maps each point's slice onto its primary's
block. Readers see an ordinary contiguous ``readouts`` with ordinary cues; nothing in the
file format changes.

Everything here needs h5py and, for the combinations the library defines, the
``mcstas_readout`` package. Both are optional: without them restage runs as before and
says what it left uncombined.
"""
from __future__ import annotations

from pathlib import Path

#: Root attribute value libreadout writes into every collector file.
COLLECTOR_PROGRAM = 'libreadout'
#: Where a collector group keeps the records its virtual ``readouts`` is assembled from.
STORED_READOUTS = 'stored_readouts'
#: Parameters the split itself adds to both halves. They say where restage kept the
#: intermediate rays, which is nothing a replayed point should publish -- and the path is
#: into this machine's cache.
SPLIT_PARAMETERS = ('mcpl_filename',)


def _h5py():
    try:
        import h5py
    except ImportError:
        return None
    return h5py


def _readout():
    try:
        import mcstas_readout
    except ImportError:
        return None
    return mcstas_readout


def _is_collector_file(path: Path) -> bool:
    h5py = _h5py()
    try:
        with h5py.File(path, 'r') as file:
            program = file.attrs.get('program')
    except OSError:
        return False
    if isinstance(program, bytes):
        program = program.decode()
    return program == COLLECTOR_PROGRAM


def collector_files(directory: Path) -> dict[str, Path]:
    """The collector files directly in `directory`, by file name.

    Empty when h5py is missing, since there is then no way to tell a collector file from
    any other HDF5 file; `_warn_if_unhandled` says so when it matters.
    """
    directory = Path(directory)
    if _h5py() is None or not directory.is_dir():
        return {}
    return {p.name: p for p in sorted(directory.glob('*.h5')) if _is_collector_file(p)}


def _warn_if_unhandled(directories, what: str) -> bool:
    """Warn, and return True, if collector files may be present but cannot be handled."""
    from zenlog import log
    candidates = [p for d in directories for p in Path(d).glob('*.h5')]
    if not candidates:
        return False
    missing = [name for name, module in (('h5py', _h5py()), ('mcstas_readout', _readout()))
               if module is None]
    if not missing:
        return False
    log.warning(f"HDF5 files found but {' and '.join(missing)} not installed: collector "
                f"files were not {what}. Install restage[collectors] to combine them.")
    return True


def _combine_or_copy(combine, output: Path, inputs: list[Path]) -> None:
    """The library's combinations need two or more inputs; one is simply copied."""
    from shutil import copyfile
    if len(inputs) == 1:
        copyfile(inputs[0], output)
    else:
        combine(output, inputs)


def merge_collector_passes(pass_dirs: list[Path], work_dir: Path) -> list[Path]:
    """Append each collector file across the primary's repeated passes into `work_dir`.

    Every pass simulated the same point with its own ray count, so the library's
    ``append`` is the right combination: it sums records and normalizations. A file name
    missing from some passes is an error, as a missing ``.dat`` file is.
    """
    pass_dirs = [Path(d) for d in pass_dirs]
    if not pass_dirs or _warn_if_unhandled(pass_dirs, 'merged across primary passes'):
        return []
    per_pass = [collector_files(d) for d in pass_dirs]
    names = sorted(set().union(*per_pass))
    merged = []
    for name in names:
        if not all(name in files for files in per_pass):
            raise RuntimeError(f'Collector file {name} is missing from some primary passes')
        output = Path(work_dir).joinpath(name)
        _combine_or_copy(_readout().append_collector_files, output,
                         [files[name] for files in per_pass])
        merged.append(output)
    return merged


def assemble_collector_scan(points: list[tuple[Path, Path]], out_dir: Path) -> list[Path]:
    """One multi-point collector file per file name, from a scan's two stages.

    `points` holds, in scan order, each point's secondary output directory and the output
    directory of the primary simulation it used. The secondary files are concatenated by
    the library; the primaries' groups are then added once per distinct primary, behind a
    virtual ``readouts`` -- see the module docstring.
    """
    points = [(Path(s), Path(p)) for s, p in points]
    if not points:
        return []
    directories = {d for pair in points for d in pair}
    if _warn_if_unhandled(directories, 'assembled into a scan file'):
        return []
    secondary = [collector_files(s) for s, _ in points]
    primary = {p: collector_files(p) for p in {p for _, p in points}}
    names = sorted(set().union(*secondary, *primary.values()))
    assembled = []
    for name in names:
        output = Path(out_dir).joinpath(name)
        if output.exists():
            raise RuntimeError(f'Refusing to overwrite {output}')
        in_points = [files.get(name) for files in secondary]
        if all(in_points):
            _combine_or_copy(_readout().concatenate_collector_files, output, in_points)
        elif any(in_points):
            raise RuntimeError(f'Collector file {name} is missing from some scan points')
        sources = [primary[p].get(name) for _, p in points]
        if any(sources):
            if not all(sources):
                raise RuntimeError(f'Collector file {name} is missing from some primaries')
            _add_primary_groups(output, sources)
        _drop_split_parameters(output)
        assembled.append(output)
    return assembled


def _drop_split_parameters(output: Path) -> None:
    """Remove the split's own parameters from an assembled file; see SPLIT_PARAMETERS."""
    h5py = _h5py()
    with h5py.File(output, 'a') as file:
        if 'parameters' not in file:
            return
        for name in SPLIT_PARAMETERS:
            if name in file['parameters']:
                del file['parameters'][name]


def _groups(file) -> dict:
    """The collector groups of an open file, by name."""
    def kind(group):
        value = group.attrs.get('type')
        return value.decode() if isinstance(value, bytes) else value
    h5py = _h5py()
    return {name: obj for name, obj in file.items()
            if isinstance(obj, h5py.Group) and kind(obj) == 'Readouts'}


def _add_primary_groups(output: Path, sources: list[Path]) -> None:
    """Add each primary file's groups to `output`, one scan point per entry of `sources`.

    `sources` repeats a primary's file for every point it served. Each distinct file's
    records are stored once, in first-appearance order; each point's slice of the virtual
    ``readouts`` maps onto its primary's block.
    """
    import numpy as np
    h5py = _h5py()
    distinct = list(dict.fromkeys(sources))
    which = [distinct.index(s) for s in sources]
    n_points = len(sources)

    with h5py.File(output, 'a') as out:
        opened = [h5py.File(s, 'r') for s in distinct]
        try:
            if 'parameters' in out and 'parameters' in opened[0]:
                existing = next(iter(out['parameters'].values()), None)
                if existing is not None and existing.shape[0] != n_points:
                    raise RuntimeError(f'{output} holds {existing.shape[0]} points, '
                                       f'expected {n_points}')
            for key, value in opened[0].attrs.items():
                if key not in out.attrs:
                    out.attrs[key] = value
            names = list(_groups(opened[0]))
            for file in opened[1:]:
                if list(_groups(file)) != names:
                    raise RuntimeError('Primary collector files hold different groups')
            for name in names:
                if name in out:
                    raise RuntimeError(f'Collector group {name} is in both stages')
                _add_group(out, name, [f[name] for f in opened], which, np, h5py)
            _add_parameters(out, opened, which, n_points)
        finally:
            for file in opened:
                file.close()


def _add_group(out, name, groups, which, np, h5py):
    """One primary collector group, stored once per distinct primary, viewed per point."""
    blocks = []
    for group in groups:
        if group['cues'].shape[0] != 1:
            raise RuntimeError(f'Primary collector group {group.name} holds '
                               f'{group["cues"].shape[0]} points, expected one')
        blocks.append(group['readouts'][...])
    first = groups[0]['readouts']
    dtype = first.dtype
    starts = np.cumsum([0] + [len(b) for b in blocks])
    lengths = [len(blocks[k]) for k in which]
    total = int(sum(lengths))

    group = out.create_group(name)
    for key, value in groups[0].attrs.items():
        group.attrs[key] = value
    stored = group.create_dataset(STORED_READOUTS, data=np.concatenate(blocks), dtype=dtype)
    # which cache directory each stored block came from, in storage order
    stored.attrs['sources'] = [str(Path(g.file.filename).parent) for g in groups]
    stored.attrs['starts'] = starts[:-1]

    if total:
        layout = h5py.VirtualLayout(shape=(total,), dtype=dtype)
        source = h5py.VirtualSource(stored)
        offset = 0
        for k, length in zip(which, lengths):
            if length:
                layout[offset:offset + length] = source[int(starts[k]):int(starts[k]) + length]
            offset += length
        readouts = group.create_virtual_dataset('readouts', layout)
    else:
        readouts = group.create_dataset('readouts', shape=(0,), dtype=dtype)
    for key, value in first.attrs.items():
        readouts.attrs[key] = value

    cues = np.cumsum(lengths)
    if cues.size and cues[-1] > np.iinfo(np.uint32).max:
        raise RuntimeError(f'{name} would hold more records than uint32 cues can index')
    for dataset in ('cues', 'weights', 'normalizations'):
        values = np.array([groups[k][dataset][0] for k in which])
        if dataset == 'cues':
            values = cues
        group.create_dataset(dataset, data=values.astype(groups[0][dataset].dtype))


def _add_parameters(out, opened, which, n_points):
    """The primary-only parameters, one value per point from the primary that served it.

    The secondary files record the secondary instrument's parameters; replay publishes a
    point's parameters, and the discs' settings belong to the primary.
    """
    if 'parameters' not in opened[0]:
        return
    parameters = out.require_group('parameters')
    for key, value in opened[0]['parameters'].attrs.items():
        if key not in parameters.attrs:
            parameters.attrs[key] = value
    for name, source in opened[0]['parameters'].items():
        if name in parameters:
            continue
        values = [opened[k]['parameters'][name][0] for k in which]
        dataset = parameters.create_dataset(name, shape=(n_points,), dtype=source.dtype)
        dataset[...] = values
        for key, value in source.attrs.items():
            dataset.attrs[key] = value
