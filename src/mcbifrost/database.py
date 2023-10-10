from pathlib import Path
from .tables import (SimulationTableParameters, PrimaryInstrSimulationTable,
                     SecondaryInstrSimulationTable, NexusStructureTableEntry, InstrTableEntry)

class Database:
    def __init__(self, db_file: Path,
                 instr_file_table: str = None,
                 nexus_structures_table: str = None,
                 primary_simulations_table: str = None,
                 secondary_simulations_table: str = None):
        from sqlite3 import connect
        self.db = connect(db_file)
        self.cursor = self.db.cursor()
        self.instr_file_table = instr_file_table or 'instr_file'
        self.nexus_structures_table = nexus_structures_table or 'nexus_structures'
        self.primary_simulations_table = primary_simulations_table or 'primary_simulation_tables'
        self.secondary_simulations_table = secondary_simulations_table or 'secondary_simulation_tables'

        # check if the database file contains the tables:
        for table, tt in ((self.instr_file_table, InstrTableEntry),
                          (self.nexus_structures_table, NexusStructureTableEntry),
                          (self.primary_simulations_table, PrimaryInstrSimulationTable),
                          (self.secondary_simulations_table, SecondaryInstrSimulationTable)):
            if not self.table_exists(table):
                self.cursor.execute(tt.create_sql_table(table_name=table))
                self.db.commit()

    def __del__(self):
        self.db.close()

    def table_exists(self, table_name: str):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return len(self.cursor.fetchall()) > 0

    def insert_instr_file(self, instr_file: InstrTableEntry):
        command = instr_file.insert_sql_table(table_name=self.instr_file_table)
        print(command)
        self.cursor.execute(command)
        self.db.commit()

    def retrieve_instr_file(self, instr_id: str, what: str = None):
        self.cursor.execute(f"SELECT {what or '*'} FROM {self.instr_file_table} WHERE id='{instr_id}'")
        return self.cursor.fetchall()

    def insert_nexus_structure(self, nexus_structure: NexusStructureTableEntry):
        command = nexus_structure.insert_sql_table(table_name=self.nexus_structures_table)
        print(command)
        self.cursor.execute(command)
        self.db.commit()

    def retrieve_nexus_structure(self, id: str, what: str = None):
        self.cursor.execute(f"SELECT {what or '*'} FROM {self.nexus_structures_table} WHERE id='{id}'")
        return self.cursor.fetchall()

    def insert_primary_simulation_table(self, primary_simulation: PrimaryInstrSimulationTable):
        command = primary_simulation.insert_sql_table(table_name=self.primary_simulations_table)
        print(command)
        self.cursor.execute(command)
        self.db.commit()

    def retrieve_primary_simulation_table(self, primary_id: str, what: str = None):
        self.cursor.execute(f"SELECT {what or '*'} FROM {self.primary_simulations_table} WHERE id='{primary_id}'")
        return self.cursor.fetchall()

    def insert_secondary_simulation_table(self, secondary_simulation: SecondaryInstrSimulationTable):
        command = secondary_simulation.insert_sql_table(table_name=self.secondary_simulations_table)
        print(command)
        self.cursor.execute(command)
        self.db.commit()

    def retrieve_secondary_simulation_table(self, primary_id: str, secondary_id: str, what: str = None):
        self.cursor.execute(f"SELECT {what or '*'} FROM {self.secondary_simulations_table} WHERE primary_id='{primary_id}' AND id='{secondary_id}'")
        return self.cursor.fetchall()

    def insert_simulation(self, sim, pars):
        if not self.table_exists(sim.name):
            self.cursor.execute(sim.create_simulation_sql_table())
            self.db.commit()
        command = sim.insert_simulation_sql_table(pars)
        print(command)
        self.cursor.execute(command)

    def retrieve_simulation(self, table_name: str, pars: SimulationTableParameters, what: str = None):
        self.cursor.execute(f"SELECT {what or '*'} FROM {table_name} WHERE {pars.between_query()}")
        return self.cursor.fetchall()

    def insert_primary_simulation(self,
                                  primary_simulation: PrimaryInstrSimulationTable,
                                  simulation_parameters: SimulationTableParameters):
        if len(self.retrieve_primary_simulation_table(primary_simulation.id)) == 0:
            self.insert_primary_simulation_table(primary_simulation)
        self.insert_simulation(primary_simulation, simulation_parameters)

    def retrieve_primary_simulation(self, primary_id: str, pars: SimulationTableParameters, what: str = None):
        matches = self.retrieve_primary_simulation_table(primary_id, what='name')
        if len(matches) != 1:
            raise RuntimeError(f"Expected exactly one match for primary_id={primary_id}, got {matches}")
        table_name = matches[0][0]
        return self.retrieve_simulation(table_name, pars, what=what)

    def insert_secondary_simulation(self, secondary_simulation: SecondaryInstrSimulationTable,
                                    simulation_parameters: SimulationTableParameters):
        if len(self.retrieve_secondary_simulation_table(secondary_simulation.primary_id, secondary_simulation.id)) == 0:
            self.insert_secondary_simulation_table(secondary_simulation)
        self.insert_simulation(secondary_simulation, simulation_parameters)

    def retrieve_secondary_simulation(self, primary_id: str, secondary_id: str, pars: SimulationTableParameters, what: str = None):
        matches = self.retrieve_secondary_simulation_table(primary_id, secondary_id, what='name')
        if len(matches) != 1:
            raise RuntimeError(f"Expected exactly one match for primary_id={primary_id}, secondary_id={secondary_id}, got {matches}")
        table_name = matches[0][0]
        # TODO check that the expected parameters match what were provided?
        return self.retrieve_simulation(table_name, pars, what=what)

