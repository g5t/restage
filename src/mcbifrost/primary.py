from dataclasses import dataclass, field


@dataclass
class PrimaryParameters:
    values: dict[str, float]
    precision: dict[str, float] = field(default_factory=dict)

    def __post_init__(self):
        from zenlog import log
        for k in self.values.keys():
            if k not in self.precision:
                # Find the best matching precision, e.g., k='ps1speed' would select 'speed' from ('speed', 'phase', ...)
                best = [p for p in self.precision.keys() if p in k]
                if len(best) > 1:
                    log.info(f"Multiple precision matches for {k}: {best}")
                self.precision[k] = self.precision[best[0]] if len(best) else self.values[k] / 1000

    def between_query(self):
        """Construct a query to select the primary parameters from a SQL database
        The selected values should be within the precision of the parameters values
        """
        each = [f"{k} BETWEEN {v - self.precision[k]} AND {v + self.precision[k]}" for k, v in self.values.items()]
        return ' AND '.join(each)

    def make_sql_insert(self, extra_values: dict, table_name: str = 'primary'):
        """Construct a SQL insert statement for the primary parameters"""
        columns = [f"{k}" for k in self.values.keys()]
        columns.extend([f"{k}" for k in extra_values.keys()])
        values = [f"{v}" for v in self.values.values()]
        values.extend([f"{v}" for v in extra_values.values()])
        return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})"


@dataclass
class PrimaryTable:
    parameters: list[str]
    extras: list[str]
    name: str = field(default_factory=str)

    def __post_init__(self):
        if not len(self.name):
            self.name = 'primary'

    def make_sql_table(self):
        """Construct a SQL table definition for the primary parameters"""
        columns = [k for k in self.parameters]
        columns.extend([k for k in self.extras])
        return f"CREATE TABLE {self.name} ({', '.join(columns)})"

    def make_sql_insert(self, parameters: PrimaryParameters, extras: dict):
        """Construct a SQL insert statement for the primary parameters"""
        if any([k not in self.extras for k in extras]):
            raise RuntimeError(f"Extra values {extras.keys()} not in {self.extras}")
        return parameters.make_sql_insert(extras, self.name)

    def query(self, parameters: PrimaryParameters):
        return f'SELECT * FROM {self.name} WHERE {parameters.between_query()}'

    def query_extras(self, parameters: PrimaryParameters):
        return f'SELECT {", ".join(self.extras)} FROM {self.name} WHERE {parameters.between_query()}'


class PrimaryDB:
    def __init__(self, db_file: str, primary_table: PrimaryTable):
        from sqlite3 import connect
        self.db = connect(db_file)
        self.cursor = self.db.cursor()
        self.primary_table = primary_table

        # check if the database file contains the primary table already
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.primary_table.name,))
        if len(self.cursor.fetchall()) == 0:
            self.cursor.execute(self.primary_table.make_sql_table())
            self.db.commit()

    def __del__(self):
        self.db.close()

    def insert(self, parameters: PrimaryParameters, extras: dict):
        self.cursor.execute(self.primary_table.make_sql_insert(parameters, extras))
        self.db.commit()

    def query(self, parameters: PrimaryParameters):
        self.cursor.execute(self.primary_table.query(parameters))
        return self.cursor.fetchall()

    def remove(self, parameters: PrimaryParameters):
        self.cursor.execute(f"DELETE FROM {self.primary_table.name} WHERE {parameters.between_query()}")
        self.db.commit()

    def query_extras(self, parameters: PrimaryParameters):
        self.cursor.execute(self.primary_table.query_extras(parameters))
        return self.cursor.fetchall()
