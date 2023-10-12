from mccode.instr import Instr
from .tables import InstrEntry, SimulationTableEntry, SimulationEntry


def setup_database(named: str):
    from platformdirs import user_cache_path
    from .database import Database
    db_file = user_cache_path('mcbifrost', 'ess', ensure_exists=True).joinpath(f'{named}.db')
    db = Database(db_file)
    return db


# Create the global database object in the module namespace.
DATABASE = setup_database('database')


def module_data_path(sub: str):
    from platformdirs import user_data_path
    path = user_data_path('mcbifrost', 'ess').joinpath(sub)
    if not path.exists():
        path.mkdir(parents=True)
    return path


def _compile_instr(entry: InstrEntry, instr: Instr, config: dict = None, target=None, generator=None):
    from mccode import __version__
    from mccode.compiler.c import compile_instrument, CBinaryTarget
    if config is None:
        config = dict(default_main=True, enable_trace=False, portable=False, include_runtime=True,
                      embed_instrument_file=False, verbose=False)
    if target is None:
        target = CBinaryTarget(mpi=False, acc=False, count=1, nexus=False)
    if generator is None:
        from mccode.translators.target import MCSTAS_GENERATOR
        generator = MCSTAS_GENERATOR

    output = module_data_path('bin').joinpath(entry.id)
    if not output.exists():
        output.mkdir(parents=True)

    binary_path = compile_instrument(instr, target, output, generator=generator, config=config)
    entry.mccode_version = __version__
    entry.binary_path = str(binary_path)
    return entry


def cache_instr(instr: Instr, mccode_version=None, binary_path=None, **kwargs) -> InstrEntry:
    instr_contents = str(instr)
    query = DATABASE.query_instr_file(search={'file_contents': instr_contents})  # returns a list[InstrTableEntry]
    if len(query) > 1:
        raise RuntimeError(f"Multiple entries for {instr_contents} in {DATABASE.instr_file_table}")
    elif len(query) == 1:
        return query[0]

    instr_file_entry = InstrEntry(file_contents=instr_contents, binary_path=binary_path or '',
                                  mccode_version=mccode_version or 'NONE')
    if binary_path is None:
        instr_file_entry = _compile_instr(instr_file_entry, instr, **kwargs)

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


def cache_get_simulation(entry: InstrEntry, row: SimulationEntry) -> SimulationEntry:
    table = cache_simulation_table(entry, row)
    query = DATABASE.retrieve_simulation(table.id, row)
    if len(query) != 1:
        raise RuntimeError(f"Expected 1 entry for {table.id} in {DATABASE.simulations_table}, got {len(query)}")
    return query[0]


def cache_simulation(entry: InstrEntry, simulation: SimulationEntry):
    table = cache_simulation_table(entry, simulation)
    DATABASE.insert_simulation(table, simulation)


# def cache_secondary_simulation_table(entry: InstrTableEntry, primary_id: str, parameters: dict) -> SecondaryInstrSimulationTable:
#     query = DATABASE.retrieve_secondary_simulation_table(primary_id, entry.id)
#     if len(query) > 1:
#         raise RuntimeError(f"Multiple entries for {primary_id} and {entry.id} in {DATABASE.secondary_simulations_table}")
#     elif len(query):
#         table = verify_table_parameters(query[0], parameters)
#     else:
#         table = SecondaryInstrSimulationTable(list(parameters.keys()), f'sst_{entry.id}', entry.id, primary_id)
#         DATABASE.insert_secondary_simulation_table(table)
#     return table
#
#
# def cache_has_secondary_simulation(entry: InstrTableEntry, primary_id: str, parameters: dict) -> bool:
#     table = cache_secondary_simulation_table(entry, primary_id, parameters)
#     query = DATABASE.retrieve_simulation(table.name, table.parameters, SimulationTableParameters(parameters))
#     return len(query) > 0
#
#
# def cache_secondary_simulation(entry: InstrTableEntry, primary_id: str, simulation: SimulationTableParameters):
#     table = cache_secondary_simulation_table(entry, primary_id, simulation.parameter_values)
#     DATABASE.insert_secondary_simulation(table, simulation)
