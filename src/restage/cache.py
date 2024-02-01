from __future__ import annotations

from mccode_antlr.instr import Instr
from .tables import InstrEntry, SimulationTableEntry, SimulationEntry
from mccode_antlr.compiler.c import CBinaryTarget


def setup_database(named: str):
    from platformdirs import user_cache_path
    from .database import Database
    db_file = user_cache_path('restage', 'ess', ensure_exists=True).joinpath(f'{named}.db')
    db = Database(db_file)
    return db


# Create the global database object in the module namespace.
DATABASE = setup_database('database')


def module_data_path(sub: str):
    from platformdirs import user_data_path
    path = user_data_path('restage', 'ess').joinpath(sub)
    if not path.exists():
        path.mkdir(parents=True)
    return path


def directory_under_module_data_path(sub: str, prefix=None, suffix=None, name=None):
    """Create a new directory under the module's given data path, and return its path"""
    # Use mkdtemp to have a short-unique name if no name is given
    from tempfile import mkdtemp
    from pathlib import Path
    under = module_data_path(sub)
    if name is not None:
        p = under.joinpath(name)
        if not p.exists():
            p.mkdir(parents=True)
    return Path(mkdtemp(dir=under, prefix=prefix or '', suffix=suffix or ''))


def _compile_instr(entry: InstrEntry, instr: Instr, config: dict | None = None,
                   mpi: bool = False, acc: bool = False,
                   target=None, generator=None):
    from tempfile import mkdtemp
    from mccode_antlr import __version__
    from mccode_antlr.compiler.c import compile_instrument, CBinaryTarget
    if config is None:
        config = dict(default_main=True, enable_trace=False, portable=False, include_runtime=True,
                      embed_instrument_file=False, verbose=False)
    if target is None:
        target = CBinaryTarget(mpi=mpi or False, acc=acc or False, count=1, nexus=False)
    if generator is None:
        from mccode_antlr.translators.target import MCSTAS_GENERATOR
        generator = MCSTAS_GENERATOR

    output = directory_under_module_data_path('bin')
    binary_path = compile_instrument(instr, target, output, generator=generator, config=config)
    entry.mccode_version = __version__
    entry.binary_path = str(binary_path)
    return entry


def cache_instr(instr: Instr, mpi: bool = False, acc: bool = False, mccode_version=None, binary_path=None, **kwargs) -> InstrEntry:
    instr_contents = str(instr)
    # the query returns a list[InstrTableEntry]
    query = DATABASE.query_instr_file(search={'file_contents': instr_contents, 'mpi': mpi, 'acc': acc})
    if len(query) > 1:
        raise RuntimeError(f"Multiple entries for {instr_contents} in {DATABASE.instr_file_table}")
    elif len(query) == 1:
        return query[0]

    instr_file_entry = InstrEntry(file_contents=instr_contents, mpi=mpi, acc=acc, binary_path=binary_path or '',
                                  mccode_version=mccode_version or 'NONE')
    if binary_path is None:
        instr_file_entry = _compile_instr(instr_file_entry, instr, mpi=mpi, acc=acc, **kwargs)

    DATABASE.insert_instr_file(instr_file_entry)
    return instr_file_entry


def verify_table_parameters(table, parameters: dict):
    names = list(parameters.keys())
    if any(x not in names for x in table.parameters):
        raise RuntimeError(f"Missing parameter names {names} from {table.parameters}")
    if any(x not in table.parameters for x in names):
        raise RuntimeError(f"Extra parameter names {names} not in {table.parameters}")
    return table


def cache_simulation_table(entry: InstrEntry, row: SimulationEntry) -> SimulationTableEntry:
    query = DATABASE.retrieve_simulation_table(entry.id)
    if len(query) > 1:
        raise RuntimeError(f"Multiple entries for {entry.id} in {DATABASE.simulations_table}")
    elif len(query):
        table = verify_table_parameters(query[0], row.parameter_values)
    else:
        table = SimulationTableEntry(list(row.parameter_values.keys()), f'pst_{entry.id}', entry.id)
        DATABASE.insert_simulation_table(table)
    return table


def cache_has_simulation(entry: InstrEntry, row: SimulationEntry) -> bool:
    table = cache_simulation_table(entry, row)
    query = DATABASE.retrieve_simulation(table.id, row)
    return len(query) > 0


def cache_get_simulation(entry: InstrEntry, row: SimulationEntry) -> list[SimulationEntry]:
    table = cache_simulation_table(entry, row)
    query = DATABASE.retrieve_simulation(table.id, row)
    if len(query) == 0:
        raise RuntimeError(f"Expected 1 or more entry for {table.id} in {DATABASE.simulations_table}, got none")
    return query


def cache_simulation(entry: InstrEntry, simulation: SimulationEntry):
    table = cache_simulation_table(entry, simulation)
    DATABASE.insert_simulation(table, simulation)


def _cleanup_instr_table(allow_different=True):
    """Look through the cache tables and remove any entries which are no longer valid"""
    from pathlib import Path
    from mccode_antlr import __version__
    entries = DATABASE.all_instr_files()
    for entry in entries:
        if not entry.binary_path or not Path(entry.binary_path).exists():
            DATABASE.delete_instr_file(entry.id)
        elif allow_different and entry.mccode_version != __version__:
            DATABASE.delete_instr_file(entry.id)
            # plus remove the binary
            Path(entry.binary_path).unlink()
            # and its directory if it is empty (it's _probably_ empty, but we should make sure)
            if not any(Path(entry.binary_path).parent.iterdir()):
                Path(entry.binary_path).parent.rmdir()


def _cleanup_simulations_table(keep_empty=False, allow_different=False, cleanup_directories=False):
    """Look through the cached table listing simulation tables and remove any entries which are no longer valid"""
    from pathlib import Path
    for entry in DATABASE.retrieve_all_simulation_tables():
        if not DATABASE.table_exists(entry.table_name):
            DATABASE.delete_simulation_table(entry.id)
            continue

        # clean up the entries of the table
        _cleanup_simulations(entry.id, keep_empty=keep_empty, cleanup_directories=cleanup_directories)
        # and remove the table if it is empty
        if not (keep_empty or len(DATABASE.retrieve_all_simulations(entry.id))):
            DATABASE.delete_simulation_table(entry.id)
            continue

        # check that the column names all match
        if not (allow_different or DATABASE.table_has_columns(entry.table_name, entry.parameters)):
            # Remove the simulation output folders for each tabulated simulation:
            if cleanup_directories:
                for sim in DATABASE.retrieve_all_simulations(entry.id):
                    sim_path = Path(sim.output_path)
                    for item in sim_path.iterdir():
                        item.unlink()
                    sim_path.rmdir()
            DATABASE.delete_simulation_table(entry.id)


def _cleanup_nexus_table():
    # TODO implement this`
    pass


def _cleanup_simulations(primary_id: str, keep_empty=False, cleanup_directories=False):
    """Look through a cached simulations table's entries and remove any which are no longer valid"""
    from pathlib import Path
    entries = DATABASE.retrieve_all_simulations(primary_id)
    for entry in entries:
        # Does the table reference a missing simulation output directory?
        if not Path(entry.output_path).exists():
            DATABASE.delete_simulation(primary_id, entry.id)
        # or an empty one?
        elif not keep_empty and not any(Path(entry.output_path).iterdir()):
            if cleanup_directories:
                Path(entry.output_path).rmdir()
            DATABASE.delete_simulation(primary_id, entry.id)
        # TODO add a lifetime to check against?


def cache_cleanup(keep_empty=False, allow_different=False, cleanup_directories=False):
    _cleanup_instr_table(allow_different=allow_different)
    _cleanup_nexus_table()
    _cleanup_simulations_table(keep_empty=keep_empty, allow_different=allow_different,
                               cleanup_directories=cleanup_directories)


# FIXME auto cleanup is removing cached table entries incorrectly at the moment
# # automatically clean up the cache when the module is loaded
# cache_cleanup()
