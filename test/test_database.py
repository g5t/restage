import unittest
from mcbifrost.database import Database

class MyTestCase(unittest.TestCase):

    def setUp(self):
        from platformdirs import user_runtime_path
        #self.db_file = user_runtime_path('mcbifrost', 'ess', ensure_exists=True).joinpath('test_database.db')
        from pathlib import Path
        self.db_file = Path().joinpath('test_database.db')
        self.db = Database(self.db_file)

    def tearDown(self):
        del self.db
        # if self.db_file.exists():
        #     self.db_file.unlink()
        # del self.db_file

    def test_setup(self):
        self.assertTrue(self.db_file.exists())
        self.assertTrue(self.db_file.is_file())
        self.assertTrue(self.db_file.stat().st_size > 0)
        self.assertTrue(self.db.table_exists(self.db.instr_file_table))
        self.assertTrue(self.db.table_exists(self.db.nexus_structures_table))
        self.assertTrue(self.db.table_exists(self.db.simulations_table))

    def test_instr_file(self):
        from mcbifrost import InstrEntry
        file_contents = 'fake file contents'
        binary_path = '/not/a/real/binary/path'
        mccode_version = 'version'
        instr_file_entry = InstrEntry(file_contents=file_contents, binary_path=binary_path,
                                      mccode_version=mccode_version)
        self.db.insert_instr_file(instr_file_entry)
        instr_id = instr_file_entry.id
        retrieved = self.db.retrieve_instr_file(instr_id=instr_id)
        self.assertEqual(len(retrieved), 1)
        self.assertTrue(isinstance(retrieved[0], InstrEntry))
        self.assertEqual(retrieved[0].id, instr_id)
        self.assertEqual(retrieved[0].file_contents, file_contents)
        self.assertEqual(retrieved[0].binary_path, binary_path)
        self.assertEqual(retrieved[0].mccode_version, mccode_version)

    def test_nexus_structure(self):
        from mcbifrost import NexusStructureEntry
        instr_id = 'fake instr id'
        json_contents = 'fake json contents'
        eniius_version = 'fake eniius version'
        nexus_structure_entry = NexusStructureEntry(id=instr_id, json_contents=json_contents,
                                                    eniius_version=eniius_version)
        self.db.insert_nexus_structure(nexus_structure_entry)
        retrieved = self.db.retrieve_nexus_structure(id=instr_id)
        self.assertEqual(len(retrieved), 1)
        self.assertTrue(isinstance(retrieved[0], NexusStructureEntry))
        self.assertEqual(retrieved[0].id, instr_id)
        self.assertEqual(retrieved[0].json_contents, json_contents)
        self.assertEqual(retrieved[0].eniius_version, eniius_version)



if __name__ == '__main__':
    unittest.main()
