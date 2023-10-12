from pathlib import Path
from .tables import SimulationEntry, SimulationTableEntry, NexusStructureEntry, InstrEntry


class Database:
    def __init__(self, db_file: Path,
                 instr_file_table: str = None,
                 nexus_structures_table: str = None,
                 simulations_table: str = None,
                 # secondary_simulations_table: str = None
                 ):
        from sqlite3 import connect
        self.db = connect(db_file)
        self.cursor = self.db.cursor()
        self.instr_file_table = instr_file_table or 'instr_file'
        self.nexus_structures_table = nexus_structures_table or 'nexus_structures'
        self.simulations_table = simulations_table or 'simulation_tables'
        # self.secondary_simulations_table = secondary_simulations_table or 'secondary_simulation_tables'
        self.verbose = False

        # check if the database file contains the tables:
        for table, tt in ((self.instr_file_table, InstrEntry),
                          (self.nexus_structures_table, NexusStructureEntry),
                          (self.simulations_table, SimulationTableEntry),
                          # (self.secondary_simulations_table, SecondaryInstrSimulationTable)
                          ):
            if not self.table_exists(table):
                self.cursor.execute(tt.create_sql_table(table_name=table))
                self.db.commit()

    def __del__(self):
        self.db.close()

    def announce(self, msg: str):
        if self.verbose:
            print(msg)

    def table_exists(self, table_name: str):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return len(self.cursor.fetchall()) > 0

    def insert_instr_file(self, instr_file: InstrEntry):
        command = instr_file.insert_sql_table(table_name=self.instr_file_table)
        self.announce(command)
        self.cursor.execute(command)
        self.db.commit()

    def retrieve_instr_file(self, instr_id: str) -> list[InstrEntry]:
        self.cursor.execute(f"SELECT * FROM {self.instr_file_table} WHERE id='{instr_id}'")
        return [InstrEntry.from_query_result(x) for x in self.cursor.fetchall()]

    def query_instr_file(self, search: dict) -> list[InstrEntry]:
        query = f"SELECT * FROM {self.instr_file_table} WHERE "
        query += ' AND '.join([f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}" for k, v in search.items()])
        self.announce(query)
        self.cursor.execute(query)
        return [InstrEntry.from_query_result(x) for x in self.cursor.fetchall()]

    def insert_nexus_structure(self, nexus_structure: NexusStructureEntry):
        command = nexus_structure.insert_sql_table(table_name=self.nexus_structures_table)
        self.announce(command)
        self.cursor.execute(command)
        self.db.commit()

    def retrieve_nexus_structure(self, id: str) -> list[NexusStructureEntry]:
        self.cursor.execute(f"SELECT * FROM {self.nexus_structures_table} WHERE id='{id}'")
        return [NexusStructureEntry.from_query_result(x) for x in self.cursor.fetchall()]

    def insert_simulation_table(self, entry: SimulationTableEntry):
        command = entry.insert_sql_table(table_name=self.simulations_table)
        self.announce(command)
        self.cursor.execute(command)
        self.db.commit()

    def retrieve_simulation_table(self, primary_id: str) -> list[SimulationTableEntry]:
        self.cursor.execute(f"SELECT * FROM {self.simulations_table} WHERE id='{primary_id}'")
        return [SimulationTableEntry.from_query_result(x) for x in self.cursor.fetchall()]

    def query_simulation_table(self, entry: SimulationTableEntry, **kwargs) -> list[SimulationTableEntry]:
        command = entry.query_simulation_tables(self.simulations_table, **kwargs)
        self.announce(command)
        self.cursor.execute(command)
        return [SimulationTableEntry.from_query_result(x) for x in self.cursor.fetchall()]

    # def insert_secondary_simulation_table(self, secondary_simulation: SecondaryInstrSimulationTable):
    #     command = secondary_simulation.insert_sql_table(table_name=self.secondary_simulations_table)
    #     self.cursor.execute(command)
    #     self.db.commit()
    #
    # def retrieve_secondary_simulation_table(self, primary_id: str, secondary_id: str) -> list[SecondaryInstrSimulationTable]:
    #     self.cursor.execute(f"SELECT * FROM {self.secondary_simulations_table} WHERE primary_id='{primary_id}' AND id='{secondary_id}'")
    #     return [SecondaryInstrSimulationTable.from_query_result(x) for x in self.cursor.fetchall()]

    def _insert_simulation(self, sim: SimulationTableEntry, pars: SimulationEntry):
        if not self.table_exists(sim.table_name):
            command = sim.create_simulation_sql_table()
            self.announce(command)
            self.cursor.execute(command)
            self.db.commit()
        command = sim.insert_simulation_sql_table(pars)
        self.announce(command)
        self.cursor.execute(command)
        self.db.commit()

    def _retrieve_simulation(self, table: str, columns: list[str], pars: SimulationEntry) -> list[SimulationEntry]:
        self.cursor.execute(f"SELECT * FROM {table} WHERE {pars.between_query()}")
        return [SimulationEntry.from_query_result(columns, x) for x in self.cursor.fetchall()]

    def retrieve_column_names(self, table_name: str):
        self.cursor.execute(f"SELECT c.name FROM pragma_table_info('{table_name}') c")
        return [x[0] for x in self.cursor.fetchall()]

    def insert_simulation(self, simulation: SimulationTableEntry, parameters: SimulationEntry):
        if len(self.retrieve_simulation_table(simulation.id)) == 0:
            self.insert_simulation_table(simulation)
        self._insert_simulation(simulation, parameters)

    def retrieve_simulation(self, primary_id: str, pars: SimulationEntry) -> list[SimulationEntry]:
        matches = self.retrieve_simulation_table(primary_id)
        if len(matches) != 1:
            raise RuntimeError(f"Expected exactly one match for id={primary_id}, got {matches}")
        table = matches[0].table_name
        columns = self.retrieve_column_names(table)
        return self._retrieve_simulation(table, columns, pars)

    # def insert_secondary_simulation(self, secondary_simulation: SecondaryInstrSimulationTable,
    #                                 simulation_parameters: SimulationTableParameters):
    #     if len(self.retrieve_secondary_simulation_table(secondary_simulation.primary_id, secondary_simulation.id)) == 0:
    #         self.insert_secondary_simulation_table(secondary_simulation)
    #     self._insert_simulation(secondary_simulation, simulation_parameters)
    #
    # def retrieve_secondary_simulation(self, primary_id: str, secondary_id: str, pars: SimulationTableParameters) -> list[SimulationTableParameters]:
    #     matches = self.retrieve_secondary_simulation_table(primary_id, secondary_id)
    #     if len(matches) != 1:
    #         raise RuntimeError(f"Expected exactly one match for primary_id={primary_id}, secondary_id={secondary_id}, got {matches}")
    #     table = matches[0].name
    #     columns = self.retrieve_column_names(table)
    #     return self._retrieve_simulation(table, columns, pars)
    #
