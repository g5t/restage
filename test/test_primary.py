import unittest
from mcbifrost import PrimaryDB, PrimaryParameters, PrimaryTable

class PrimaryTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = 'test_database.db'
        self.primary_table = PrimaryTable(parameters=['a', 'b', 'c'], extras=['d', 'e', 'f'])
        self.primary_db = PrimaryDB(self.db_file, self.primary_table)

    def tearDown(self):
        del self.primary_db
        del self.primary_table
        del self.db_file



if __name__ == '__main__':
    unittest.main()
