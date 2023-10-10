from dataclasses import dataclass, field

def uuid():
    from uuid import uuid4
    return str(uuid4()).replace('-', '')

@dataclass
class SimulationTableParameters:
    parameter_values: dict[str, float]
    _output_path: str = field(default_factory=str)
    _precision: dict[str, float] = field(default_factory=dict)
    _id: str = field(default_factory=uuid)

    def __post_init__(self):
        for k in self.parameter_values.keys():
            if k not in self._precision:
                # Find the best matching precision, e.g., k='ps1speed' would select 'speed' from ('speed', 'phase', ...)
                best = [p for p in self._precision.keys() if p in k]
                if len(best) > 1:
                    from zenlog import log
                    log.info(f"Multiple precision matches for {k}: {best}")
                self._precision[k] = self._precision[best[0]] if len(best) else self.parameter_values[k] / 1000

    def __hash__(self):
        return hash(tuple(self.parameter_values.values()))

    def between_query(self):
        """Construct a query to select the primary parameters from a SQL database
        The selected values should be within the precision of the parameters values
        """
        each = [f"{k} BETWEEN {v - self._precision[k]} AND {v + self._precision[k]}" for k, v in self.parameter_values.items()]
        return ' AND '.join(each)

    def columns(self):
        """Construct a list of column names for the primary parameters"""
        columns = ['id', 'output_path']
        columns.extend([f"{k}" for k in self.parameter_values.keys()])
        return columns

    def values(self):
        """Construct a list of column values for the primary parameters"""
        values = [f"'{x}'" for x in (self._id, self._output_path)]
        values.extend([f"{v}" for v in self.parameter_values.values()])
        return values

    def insert_sql_table(self, table_name: str):
        """Construct a SQL insert statement for the primary parameters"""
        return f"INSERT INTO {table_name} ({', '.join(self.columns())}) VALUES ({', '.join(self.values())})"

    def create_containing_sql_table(self, table_name: str):
        """Construct a SQL table definition for the primary parameters"""
        # these column names _must_ match the names in TableParameters make_sql_insert
        return f"CREATE TABLE {table_name} ({', '.join(self.columns())})"


@dataclass
class InstrSimulationTable:
    parameters: list[str]
    name: str = field(default_factory=str)
    id: str = field(default_factory=uuid)

    @staticmethod
    def columns():
        return []

    def values(self):
        return []

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

    def insert_simulation_sql_table(self, parameters: SimulationTableParameters):
        """Construct a SQL insert statement for the primary parameters"""
        if any([k not in self.parameters for k in parameters.parameter_values]):
            raise RuntimeError(f"Extra values {parameters.parameter_values.keys()} not in {self.parameters}")
        return parameters.insert_sql_table(self.name)

    def query(self, parameters: SimulationTableParameters):
        return f'SELECT * FROM {self.name} WHERE {parameters.between_query()}'

    def output_path(self, parameters: SimulationTableParameters):
        return f'SELECT output_path FROM {self.name} WHERE {parameters.between_query()}'


@dataclass
class PrimaryInstrSimulationTable(InstrSimulationTable):
    def __post_init__(self):
        if len(self.name) == 0:
            self.name = f'primary_instr_table_{self.id}'

    @staticmethod
    def columns():
        return ['id', 'name', 'parameters']

    def values(self):
        return [f"'{x}'" for x in [self.id, self.name, str(self.parameters)]]


@dataclass
class SecondaryInstrSimulationTable(InstrSimulationTable):
    primary_id: str = field(default_factory=str)

    def __post_init__(self):
        if self.primary_id is None:
            raise RuntimeError("Secondary instrument tables require a primary_id")
        if len(self.name) == 0:
            self.name = f'secondary_instr_table_{self.id}'

    @staticmethod
    def columns():
        return ['id', 'primary_id', 'name', 'parameters']

    def values(self):
        return [f"'{x}'" for x in [self.id, self.primary_id, self.name, str(self.parameters)]]


@dataclass
class NexusStructureTableEntry:
    id: str
    json_contents: str
    eniius_version: str = field(default_factory=str)

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
class InstrTableEntry:
    file_contents: str
    binary_path: str
    mccode_version: str = field(default_factory=str)
    id: str = field(default_factory=uuid)

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
