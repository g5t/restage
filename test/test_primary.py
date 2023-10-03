import unittest
from mcbifrost import PrimaryDB, PrimaryParameters, PrimaryTable

class PrimaryTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = 'test_database.db'
        self.table = PrimaryTable(parameters=['a', 'b', 'c'], extras=['d', 'e', 'f'])
        self.primary_db = PrimaryDB(self.db_file, self.table)

    def tearDown(self):
        del self.primary_db
        del self.table
        del self.db_file

    def test_insert(self):
        P = PrimaryParameters({'a': 1, 'b': 2, 'c': 3})
        E = {'d': 4, 'e': 5, 'f': 6}
        self.primary_db.insert(P, E)
        self.assertEqual(self.primary_db.query(P), [(1, 2, 3, 4, 5, 6)])
        self.assertEqual(self.primary_db.query_extras(P), [(4, 5, 6)])
        self.primary_db.remove(P)
        self.assertEqual(self.primary_db.query(P), [])


if __name__ == '__main__':
    unittest.main()
