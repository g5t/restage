from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from mccode_antlr.instr import Instr
from .tables import InstrEntry, SimulationTableEntry, SimulationEntry
from .database import Database

@dataclass
class FileSystem:
    root: Path
    db_fixed: tuple[Database,...]
    db_write: Database

    @classmethod
    def from_config(cls, named: str):
        from .config import config
        db_fixed = []
        db_write = None
        root = None
        if not named.endswith('.db'):
            named += '.db'
        if config['cache'].exists():
            path = config['cache'].as_path()
            if not path.exists():
                path.mkdir(parents=True)
            db_write = Database(path / named)
            root = path

        def exists_not_root(roc):
            roc = Path(roc)
            if not roc.exists() or not (roc / named).exists():
                return False
            return root.resolve() != roc.resolve() if root is not None else True

        if config['fixed'].exists() and config['fixed'].get() is not None:
            more = [Path(c) for c in config['fixed'].as_str_seq() if exists_not_root(c)]
            for m in more:
                db_fixed.append(Database(m / named, readonly=True))

        if db_write is not None and db_write.readonly:
            raise ValueError("Specified writable database location is readonly")
        if db_write is None:
            from platformdirs import user_cache_path
            db_write = Database(user_cache_path('restage', 'ess', ensure_exists=True) / named)
        if root is None:
            from platformdirs import user_data_path
            root = user_data_path('restage', 'ess')
        return cls(root, tuple(db_fixed), db_write)

    def query(self, method, *args, **kwargs):
        q = [x for r in self.db_fixed for x in getattr(r, method)(*args, **kwargs)]
        q.extend(getattr(self.db_write, method)(*args, **kwargs))
        return q

    def insert(self, method, *args, **kwargs):
        getattr(self.db_write, method)(*args, **kwargs)

    def query_instr_file(self, *args, **kwargs):
        query = [x for r in self.db_fixed for x in r.query_instr_file(*args, **kwargs)]
        query.extend(self.db_write.query_instr_file(*args, **kwargs))
        return query

    def insert_instr_file(self, *args, **kwargs):
        self.db_write.insert_instr_file(*args, **kwargs)

    def query_simulation_table(self, *args, **kwargs):
        return self.query('query_simulation_table', *args, **kwargs)

    def retrieve_simulation_table(self, *args, **kwargs):
        return self.query('retrieve_simulation_table', *args, **kwargs)

    def insert_simulation_table(self, *args, **kwargs):
        self.insert('insert_simulation_table', *args, **kwargs)

    def insert_simulation(self, *args, **kwargs):
        # By definition, 'self.db_write' is writable and Database.insert_simulation
        # _always_ ensures the presence of the specified table in its database.
        # Therefore this method 'just works'.
        self.insert('insert_simulation', *args, **kwargs)

    def retrieve_simulation(self, table_id: str, row: SimulationEntry):
        matches = []
        for db in self.db_fixed:
            if len(db.retrieve_simulation_table(table_id, False)) == 1:
                matches.extend(db.retrieve_simulation(table_id, row))
        if len(self.db_write.retrieve_simulation_table(table_id, False)) == 1:
            matches.extend(self.db_write.retrieve_simulation(table_id, row))
        return matches



FILESYSTEM = FileSystem.from_config('database')


def module_data_path(sub: str):
    path = FILESYSTEM.root / sub
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
                   target=None, flavor=None):
    from mccode_antlr import __version__, Flavor
    from mccode_antlr.compiler.c import compile_instrument, CBinaryTarget
    if config is None:
        config = dict(default_main=True, enable_trace=False, portable=False, include_runtime=True,
                      embed_instrument_file=False, verbose=False)
    if target is None:
        target = CBinaryTarget(mpi=mpi or False, acc=acc or False, count=1, nexus=False)
    if flavor is None:
        flavor = Flavor.MCSTAS

    output = directory_under_module_data_path('bin')
    source_file = output.joinpath(instr.name).with_suffix('.c')
    binary_path = compile_instrument(instr, target, output, flavor=flavor, config=config, source_file=source_file)
    entry.mccode_version = __version__
    entry.binary_path = str(binary_path)
    return entry


def cache_instr(instr: Instr, mpi: bool = False, acc: bool = False, mccode_version=None, binary_path=None, **kwargs) -> InstrEntry:
    instr_contents = str(instr)
    # the query returns a list[InstrTableEntry]
    query = FILESYSTEM.query_instr_file(search={'file_contents': instr_contents, 'mpi': mpi, 'acc': acc})
    if len(query) > 1:
        raise RuntimeError(f"Multiple entries for {instr_contents} in {FILESYSTEM}")
    elif len(query) == 1:
        return query[0]

    instr_file_entry = InstrEntry(file_contents=instr_contents, mpi=mpi, acc=acc, binary_path=binary_path or '',
                                  mccode_version=mccode_version or 'NONE')
    if binary_path is None:
        instr_file_entry = _compile_instr(instr_file_entry, instr, mpi=mpi, acc=acc, **kwargs)

    FILESYSTEM.insert_instr_file(instr_file_entry)
    return instr_file_entry


def cache_get_instr(instr: Instr, mpi: bool = False, acc: bool = False) -> InstrEntry | None:
    query = FILESYSTEM.query_instr_file(search={'file_contents': str(instr), 'mpi': mpi, 'acc': acc})
    if len(query) > 1:
        raise RuntimeError(f"Multiple entries for {instr} in {FILESYSTEM}")
    elif len(query) == 1:
        return query[0]
    return None


def verify_table_parameters(table, parameters: dict):
    names = list(parameters.keys())
    if any(x not in names for x in table.parameters):
        raise RuntimeError(f"Missing parameter names {names} from {table.parameters}")
    if any(x not in table.parameters for x in names):
        raise RuntimeError(f"Extra parameter names {names} not in {table.parameters}")
    return table


def cache_simulation_table(entry: InstrEntry, row: SimulationEntry) -> SimulationTableEntry:
    query = FILESYSTEM.retrieve_simulation_table(entry.id)
    if len(query):
        for q in query:
            verify_table_parameters(q, row.parameter_values)
        table = query[0]
    else:
        table = SimulationTableEntry(list(row.parameter_values.keys()), f'pst_{entry.id}', entry.id)
        FILESYSTEM.insert_simulation_table(table)
    return table


def cache_has_simulation(entry: InstrEntry, row: SimulationEntry) -> bool:
    table = cache_simulation_table(entry, row)
    query = FILESYSTEM.retrieve_simulation(table.id, row)
    return len(query) > 0


def cache_get_simulation(entry: InstrEntry, row: SimulationEntry) -> list[SimulationEntry]:
    table = cache_simulation_table(entry, row)
    query = FILESYSTEM.retrieve_simulation(table.id, row)
    if len(query) == 0:
        raise RuntimeError(f"Expected 1 or more entry for {table.id} in {FILESYSTEM}, got none")
    return query


def cache_simulation(entry: InstrEntry, simulation: SimulationEntry):
    table = cache_simulation_table(entry, simulation)
    FILESYSTEM.insert_simulation(table, simulation)
