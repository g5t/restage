from dataclasses import dataclass, field
from mccode.common import Value


def uuid():
    from uuid import uuid4
    return str(uuid4()).replace('-', '')


@dataclass
class SimulationEntry:
    """A class to represent the primary parameters of a simulation which constitute an entry in a specific table"""
    parameter_values: dict[str, Value]
    output_path: str = field(default_factory=str)
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
        q_path = values[names.index('output_path')]
        pv = {k: float(v) if isinstance(v, str) else v for k, v in zip(names, values) if k not in ('id', 'output_path')}
        return cls(pv, output_path=q_path, id=q_id)

    def __post_init__(self):
        for k, v in self.parameter_values.items():
            if not isinstance(v, Value):
                self.parameter_values[k] = Value.best(v)

        for k, v in self.parameter_values.items():
            if v.is_float and k not in self.precision:
                # Find the best matching precision, e.g., k='ps1speed' would select 'speed' from ('speed', 'phase', ...)
                best = [p for p in self.precision.keys() if p in k]
                if len(best) > 1:
                    from zenlog import log
                    log.info(f"Multiple precision matches for {k}: {best}")
                self.precision[k] = self.precision[best[0]] if len(best) else self.parameter_values[k].value / 10000

    def __hash__(self):
        return hash(tuple(self.parameter_values.values()))

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
        return ' AND '.join(each)

    def columns(self):
        """Construct a list of column names for the primary parameters"""
        columns = ['id', 'output_path']
        columns.extend([f"{k}" for k in self.parameter_values.keys()])
        return columns

    def values(self):
        """Construct a list of column values for the primary parameters"""
        values = [f"'{x}'" for x in (self.id, self.output_path)]
        for v in self.parameter_values.values():
            if v.is_float or v.is_int:
                values.append(f"{v.value}")
            else:
                values.append(f"'{v.value}'")
        return values

    def insert_sql_table(self, table_name: str):
        """Construct a SQL insert statement for the primary parameters"""
        return f"INSERT INTO {table_name} ({', '.join(self.columns())}) VALUES ({', '.join(self.values())})"

    def create_containing_sql_table(self, table_name: str):
        """Construct a SQL table definition for the primary parameters"""
        # these column names _must_ match the names in TableParameters make_sql_insert
        return f"CREATE TABLE {table_name} ({', '.join(self.columns())})"


@dataclass
class SimulationTableEntry:
    """A class to represent the table-entry elements of a table of primary parameters"""
    parameters: list[str]
    name: str = field(default_factory=str)
    id: str = field(default_factory=uuid)

    @classmethod
    def from_query_result(cls, values):
        pid,  name, parameters = values
        parameters = parameters.translate(str.maketrans('', '', '\'\" []')).split(',')
        return cls(parameters, name, pid)

    def __post_init__(self):
        if len(self.name) == 0:
            self.name = f'primary_instr_table_{self.id}'

    @staticmethod
    def columns():
        return ['id', 'name', 'parameters']

    def values(self):
        return [f"'{x}'" for x in [self.id, self.name, str(self.parameters)]]

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
        columns = ['id', 'output_path']
        columns.extend([k for k in self.parameters])
        return f"CREATE TABLE {self.name} ({', '.join(columns)})"

    def insert_simulation_sql_table(self, parameters: SimulationEntry):
        """Construct a SQL insert statement for the primary parameters"""
        if any([k not in self.parameters for k in parameters.parameter_values]):
            raise RuntimeError(f"Extra values {parameters.parameter_values.keys()} not in {self.parameters}")
        return parameters.insert_sql_table(self.name)

    def query(self, parameters: SimulationEntry):
        return f'SELECT * FROM {self.name} WHERE {parameters.between_query()}'

    def output_path(self, parameters: SimulationEntry):
        return f'SELECT output_path FROM {self.name} WHERE {parameters.between_query()}'



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
