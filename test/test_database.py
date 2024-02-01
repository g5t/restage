import unittest
from restage.database import Database


class MyTestCase(unittest.TestCase):

    def setUp(self):
        from platformdirs import user_runtime_path
        self.db_file = user_runtime_path('restage', 'ess', ensure_exists=True).joinpath('test_database.db')
        from pathlib import Path
        self.db_file = Path().joinpath('test_database.db')
        self.db = Database(self.db_file)

    def tearDown(self):
        del self.db
        if self.db_file.exists():
            self.db_file.unlink()
        del self.db_file

    def test_setup(self):
        self.assertTrue(self.db_file.exists())
        self.assertTrue(self.db_file.is_file())
        self.assertTrue(self.db_file.stat().st_size > 0)
        self.assertTrue(self.db.table_exists(self.db.instr_file_table))
        self.assertTrue(self.db.table_exists(self.db.nexus_structures_table))
        self.assertTrue(self.db.table_exists(self.db.simulations_table))

    def _check_return(self, retrieved, expected_type, expected_values):
        self.assertEqual(len(retrieved), 1)
        self.assertTrue(isinstance(retrieved[0], expected_type))
        obj = retrieved[0]
        for k, v in expected_values.items():
            self.assertTrue(hasattr(obj, k))
            self.assertEqual(getattr(obj, k), v)

    def test_instr_file(self):
        from restage import InstrEntry
        file_contents = 'fake file contents'
        binary_path = '/not/a/real/binary/path'
        mccode_version = 'version'
        mpi = False
        acc = True
        instr_file_entry = InstrEntry(file_contents=file_contents, binary_path=binary_path,
                                      mccode_version=mccode_version, mpi=mpi, acc=acc)
        self.db.insert_instr_file(instr_file_entry)
        instr_id = instr_file_entry.id
        retrieved = self.db.retrieve_instr_file(instr_id=instr_id)
        self._check_return(retrieved, InstrEntry, {'id': instr_id, 'file_contents': file_contents,
                                                   'binary_path': binary_path, 'mccode_version': mccode_version,
                                                   'mpi': mpi, 'acc': acc})

    def test_nexus_structure(self):
        from restage import NexusStructureEntry
        instr_id = 'fake instr id'
        json_contents = 'fake json contents'
        eniius_version = 'fake eniius version'
        nexus_structure_entry = NexusStructureEntry(id=instr_id, json_contents=json_contents,
                                                    eniius_version=eniius_version)
        self.db.insert_nexus_structure(nexus_structure_entry)
        retrieved = self.db.retrieve_nexus_structure(id=instr_id)
        self._check_return(retrieved, NexusStructureEntry, {'id': instr_id, 'json_contents': json_contents,
                           'eniius_version': eniius_version})

    def test_simulation_table(self):
        from restage import SimulationTableEntry
        instr_id = 'fake instr id'
        name = 'super_instr_1'
        parameters = ['par1', 'par2', 'par3', 'par4']
        entry = SimulationTableEntry(parameters=parameters, name=name, id=instr_id)
        self.db.insert_simulation_table(entry)
        retrieved = self.db.retrieve_simulation_table(instr_id)
        self._check_return(retrieved, SimulationTableEntry, {'id': instr_id, 'name': name,
                                                             'parameters': parameters})

    def test_simulation(self):
        from restage import SimulationTableEntry, SimulationEntry
        from mccode_antlr.common import Value
        name = 'super_instr_2'
        parameters = ['par1', 'par2', 'par3', 'par4']
        entry = SimulationTableEntry(parameters=parameters, name=name)
        self.db.insert_simulation_table(entry)

        # check that we can retrieve stored table information
        # (originally the idea was to depend on the previous test, but this doesn't work in practice unless if we
        #  handle the database file lifetime externally to the test cases)
        res = self.db.query_simulation_table(entry, use_id=False, use_name=True, use_parameters=True)
        self._check_return(res, SimulationTableEntry, {'name': name, 'parameters': parameters})
        self.assertEqual(res[0].id, entry.id)

        entry = res[0]

        simulation_pars = {'par1': 1.1, 'par2': 2.2, 'par3': 3.3, 'par4': 4.4}
        simulation_pars = {k: Value.best(v) for k, v in simulation_pars.items()}
        # we did the 'same' simulation three times, but with different seeds and ncounts
        first_simulation = SimulationEntry(parameter_values=simulation_pars)
        self.db.insert_simulation(entry, first_simulation)
        second_simulation = SimulationEntry(parameter_values=simulation_pars, seed=101)
        self.db.insert_simulation(entry, second_simulation)
        third_simulation = SimulationEntry(parameter_values=simulation_pars, seed=101, ncount=1_000_000_000)
        self.db.insert_simulation(entry, third_simulation)
        # It we go looking for the first simulation, we should find all three instances since we did not
        # specify seed or ncount
        retrieved = self.db.retrieve_simulation(entry.id, first_simulation)
        self.assertEqual(len(retrieved), 3)
        # If we go looking for the second simulation, we should find two instances since we specified seed
        retrieved = self.db.retrieve_simulation(entry.id, second_simulation)
        self.assertEqual(len(retrieved), 2)
        # If we go looking for the third simulation, we should find one instance since we specified seed and ncount
        retrieved = self.db.retrieve_simulation(entry.id, third_simulation)
        self.assertEqual(len(retrieved), 1)

        # So it's up to the user to specify the correct parameters to retrieve the correct simulation,
        # or to filter on the returned values for the most-appropriate simulation.


if __name__ == '__main__':
    unittest.main()
