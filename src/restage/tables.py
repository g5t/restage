from dataclasses import dataclass, field
from mccode.common import Value


def uuid():
    from uuid import uuid4
    return str(uuid4()).replace('-', '')


COMMON_COLUMNS = ['seed', 'ncount', 'output_path', 'gravitation']


@dataclass
class SimulationEntry:
    """A class to represent the primary parameters of a simulation which constitute an entry in a specific table"""
    parameter_values: dict[str, Value]
    seed: int = None
    ncount: int = None
    output_path: str = field(default_factory=str)
    gravitation: bool = False
    precision: dict[str, float] = field(default_factory=dict)
    id: str = field(default_factory=uuid)

    @classmethod
    def from_query_result(cls, names: list[str], values):
        if 'id' not in names:
            raise RuntimeError(f"Missing 'id' column in {names}")
        if 'output_path' not in names:
            raise RuntimeError(f"Missing 'output_path' column in {names}")
        if len(names) != len(values):
            raise RuntimeError(f"Column names {names} do not match query result {values}")

        q_id = values[names.index('id')]
        q_seed = values[names.index('seed')]  # already converted to None if <null> stored in table
        q_ncount = values[names.index('ncount')]  # same here
        q_path = values[names.index('output_path')]
        q_gravitation = values[names.index('gravitation')] > 0
        extracted = ('id', 'seed', 'ncount', 'output_path', 'gravitation')
        pv = {k: v for k, v in zip(names, values) if k not in extracted}
        return cls(pv, seed=q_seed, ncount=q_ncount, output_path=q_path, gravitation=q_gravitation, id=q_id)

    def __post_init__(self):
        from zenlog import log
        for k, v in self.parameter_values.items():
            if not isinstance(v, Value):
                self.parameter_values[k] = Value.best(v)

        for k, v in self.parameter_values.items():
            if v.is_float and k not in self.precision:
                # Find the best matching precision, e.g., k='ps1speed' would select 'speed' from ('speed', 'phase', ...)
                best = [p for p in self.precision.keys() if p in k]
                if len(best) > 1:
                    log.info(f"SimulationEntry.__post_init__:: Multiple precision matches for {k}: {best}")
                if len(best):
                    self.precision[k] = self.precision[best[0]]
                elif self.parameter_values[k].has_value:
                    self.precision[k] = self.parameter_values[k].value / 10000
                else:
                    log.info(f'SimulationEntry.__post_init__:: No precision match for value-less {k}, using 0.1;'
                             ' consider specifying precision dict during initialization')
                    self.precision[k] = 0.1

    def __hash__(self):
        return hash((tuple(self.parameter_values.values()), self.seed, self.ncount, self.gravitation))

    def between_query(self):
        """Construct a query to select the primary parameters from a SQL database
        The selected values should be within the precision of the parameters values
        """
        each = []
        for k, v in self.parameter_values.items():
            if v.is_float:
                each.append(f"{k} BETWEEN {v.value - self.precision[k]} AND {v.value + self.precision[k]}")
            elif v.is_int:
                each.append(f'{k}={v.value}')
            else:
                each.append(f"{k}='{v.value}'")
        if self.seed is not None:
            each.append(f"seed={self.seed}")
        if self.ncount is not None:
            each.append(f"ncount={self.ncount}")
        if self.gravitation:
            each.append(f"gravitation={self.gravitation}")
        return ' AND '.join(each)

    def columns(self):
        """Construct a list of column names for the primary parameters"""
        columns = ['id']
        columns.extend(COMMON_COLUMNS)  # add the columns that appear in both simulation_tables and simulation tables
        columns.extend([f"{k}" for k in self.parameter_values.keys()])
        return columns

    def values(self):
        """Construct a list of column values for the primary parameters"""
        values = [f"'{self.id}'"]
        values.append('null' if self.seed is None else f"{self.seed}")
        values.append('null' if self.ncount is None else f"{self.ncount}")
        values.append('null' if self.output_path is None else f"'{self.output_path}'")
        values.append(f'{self.gravitation}')
        for v in self.parameter_values.values():
            if v.is_float or v.is_int:
                values.append(f"{v.value}")
            else:
                values.append(f"'{v.value}'")
        return values

    def insert_sql_table(self, table_name: str):
        """Construct a SQL insert statement for the primary parameters"""
        # # The following could be used to provide _raw_ values to the execution, instead of str formatting them
        # cols = ', '.join(self.columns())
        # vals = ', '.join('?' for _ in self.values())
        # return f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"
        return f"INSERT INTO {table_name} ({', '.join(self.columns())}) VALUES ({', '.join(self.values())})"

    def create_containing_sql_table(self, table_name: str):
        """Construct a SQL table definition for the primary parameters"""
        # these column names _must_ match the names in TableParameters make_sql_insert
        return f"CREATE TABLE {table_name} ({', '.join(self.columns())})"

    def parameter_distance(self, other):
        if not isinstance(other, SimulationEntry):
            raise RuntimeError(f"Cannot compare {self} to {other}")
        if self.parameter_values.keys() != other.parameter_values.keys():
            raise RuntimeError(f"Cannot compare {self} to {other}")
        total = 0
        for k in self.parameter_values.keys():
            if self.parameter_values[k].is_float or self.parameter_values[k].is_int:
                total += abs(self.parameter_values[k].value - other.parameter_values[k].value)
            elif self.parameter_values[k] != other.parameter_values[k]:
                total += 10 * len(self.parameter_values)
        return total


def best_simulation_entry_match_index(candidates: list[SimulationEntry], pivot: SimulationEntry):
    # There are many reasons a query could have returned multiple matches.
    #   there could be multiple points repeated within the uncertainty we used to select the primary simulation
    #   the same simulation could be repeated with different seed values, and we haven't specified a seed here
    #   the same simulation could be repeated with different particle counts, as no ncount was specified here
    #   the same simulation could be repeated with and without gravity, and that flag is not specified here?
    # select the best one:
    #   1. check for difference between simulated and requested parameters and sort by that
    #   2. if any are the same, pick the one with the most particles (maybe we can sample from it if fewer are needed?)
    #   ... come up with some heuristic for picking the best seed?
    if len(candidates) < 2:
        return 0
    # sort the candidate indexes by parameter-distance from the pivot
    distances = [c.parameter_distance(pivot) for c in candidates]
    indexes = sorted(range(len(candidates)), key=lambda index: distances[index])
    # if there are multiple candidates with the same distance, pick the one with the most particles
    last_distance = distances[indexes[0]]
    best = 0
    for i in indexes[1:]:
        if distances[i] != last_distance:
            break
        if (candidates[i].ncount or 0) > (candidates[best].ncount or 0):
            best = i
    return indexes[best]


def best_simulation_entry_match(candidates: list[SimulationEntry], pivot: SimulationEntry):
    return candidates[best_simulation_entry_match_index(candidates, pivot)]


@dataclass
class SimulationTableEntry:
    """A class to represent the table-entry elements of a table of primary parameters"""
    parameters: list[str]
    name: str = field(default_factory=str)
    id: str = field(default_factory=uuid)

    @classmethod
    def from_query_result(cls, values):
        from json import loads
        pid,  name, parameters = values
        return cls(loads(parameters), name, pid)

    def __post_init__(self):
        if len(self.name) == 0:
            self.name = f'primary_instr_table_{self.id}'
        if ' ' in self.name:
            from zenlog import log
            log.warn(f'"{self.name}" is not a valid SQL table name, spaces replaced by underscores')
            self.name = self.name.replace(' ', '_')

    @property
    def table_name(self):
        return self.name

    @staticmethod
    def columns():
        return ['id', 'name', 'parameters']

    def values(self):
        from json import dumps
        return [f"'{x}'" for x in [self.id, self.name, dumps(self.parameters)]]

    @classmethod
    def create_sql_table(cls, table_name: str):
        """Construct a SQL creation statement for the (primary|secondary)-instrument-tables table"""
        return f"CREATE TABLE {table_name} ({', '.join(cls.columns())})"

    def insert_sql_table(self, table_name: str):
        """Construct a SQL insert statement for the (primary|secondary)-instrument-tables table"""
        return f"INSERT INTO {table_name} ({', '.join(self.columns())}) VALUES ({', '.join(self.values())})"

    def create_simulation_sql_table(self):
        """Construct a SQL table definition for the primary parameters"""
        # these column names _must_ match the names in SimulationTableParameters
        columns = ['id'] + COMMON_COLUMNS
        columns.extend([k for k in self.parameters])
        return f"CREATE TABLE {self.table_name} ({', '.join(columns)})"

    def insert_simulation_sql_table(self, parameters: SimulationEntry):
        """Construct a SQL insert statement for the primary parameters"""
        if any([k not in self.parameters for k in parameters.parameter_values]):
            raise RuntimeError(f"Extra values {parameters.parameter_values.keys()} not in {self.parameters}")
        return parameters.insert_sql_table(self.table_name)

    def query_simulation_tables(self, simulation_tables_name, use_id=False, use_name=True, use_parameters=True):
        parts = []
        if use_id:
            parts.append(f"id='{self.id}'")
        elif use_name:
            parts.append(f"name='{self.table_name}'")
        elif use_parameters:
            from json import dumps
            parts.append(f"parameters={dumps(self.parameters)}")
        else:
            raise RuntimeError("At least one of use_id, use_name, or use_parameters must be True")
        return f"SELECT * FROM {simulation_tables_name} WHERE {' AND '.join(parts)}"

    def query(self, parameters: SimulationEntry):
        return f'SELECT * FROM {self.table_name} WHERE {parameters.between_query()}'

    def output_path(self, parameters: SimulationEntry):
        return f'SELECT output_path FROM {self.table_name} WHERE {parameters.between_query()}'



#
# @dataclass
# class SecondaryInstrSimulationTable(SimulationTableEntry):
#     """An extension to the SimulationTable class to represent the entry elements of a table of secondary parameters"""
#     primary_id: str = field(default_factory=str)
#
#     @classmethod
#     def from_query_result(cls, values):
#         sid, pid, name, parameters = values
#         parameters = parameters.translate(str.maketrans('', '', '\'\" []')).split(',')
#         return cls(parameters, name, sid, pid)
#
#     def __post_init__(self):
#         if self.primary_id is None:
#             raise RuntimeError("Secondary instrument tables require a primary_id")
#         if len(self.name) == 0:
#             self.name = f'secondary_instr_table_{self.id}'
#
#     @staticmethod
#     def columns():
#         return ['id', 'primary_id', 'name', 'parameters']
#
#     def values(self):
#         return [f"'{x}'" for x in [self.id, self.primary_id, self.name, str(self.parameters)]]
#

@dataclass
class NexusStructureEntry:
    """A class to represent the nexus structure of an instrument when stored as an entry in a table"""
    id: str
    json_contents: str
    eniius_version: str = field(default_factory=str)

    @classmethod
    def from_query_result(cls, values):
        nid, json, eniius = values
        return cls(nid, json, eniius)

    def __post_init__(self):
        if len(self.eniius_version) == 0:
            from eniius import __version__
            self.eniius_version = __version__

    @staticmethod
    def columns():
        return ['id', 'json_contents', 'eniius_version']

    def values(self):
        return [f"'{x}'" for x in (self.id, self.json_contents, self.eniius_version)]

    @classmethod
    def create_sql_table(cls, table_name: str = 'nexus_structures'):
        return f"CREATE TABLE {table_name} ({', '.join(cls.columns())})"

    def insert_sql_table(self, table_name: str = 'nexus_structures'):
        return f"INSERT INTO {table_name} ({', '.join(self.columns())}) VALUES ({', '.join(self.values())})"

    @staticmethod
    def query(id: str, table_name: str = 'nexus_structures'):
        return f"SELECT * FROM {table_name} WHERE id={id}"


@dataclass
class InstrEntry:
    """A class to represent the instrument file and its compiled binary when stored as an entry in a table"""
    file_contents: str
    binary_path: str
    mccode_version: str = field(default_factory=str)
    id: str = field(default_factory=uuid)

    @classmethod
    def from_query_result(cls, values):
        fid, file_contents, binary_path, mccode_version = values
        return cls(file_contents, binary_path, mccode_version, fid)

    def __post_init__(self):
        if len(self.mccode_version) == 0:
            from mccode import __version__
            self.mccode_version = __version__

    @staticmethod
    def columns():
        return ['id', 'file_contents', 'binary_path', 'mccode_version']

    def values(self):
        return [f"'{x}'" for x in (self.id, self.file_contents, self.binary_path, self.mccode_version)]

    @classmethod
    def create_sql_table(cls, table_name: str = 'instr_files'):
        return f"CREATE TABLE {table_name} ({', '.join(cls.columns())})"

    def insert_sql_table(self, table_name: str = 'instr_files'):
        return f"INSERT INTO {table_name} ({', '.join(self.columns())}) VALUES ({', '.join(self.values())})"

    @staticmethod
    def query(instr_id: str, table_name: str = 'instr_files'):
        return f"SELECT * FROM {table_name} WHERE instr_id={instr_id}"

    @staticmethod
    def get_binary_path(instr_id: str, table_name: str = 'instr_files'):
        return f"SELECT binary_path FROM {table_name} WHERE instr_id={instr_id}"
