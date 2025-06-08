import unittest

"""
Test the multi-database mechanism by
    1. creating a temporary database and location for binaries and simulations
    2. adding at least one instrument and one simulation to the database
    3. making the first temporary database read-only, adding a second writable one
    4. 'using' the read-only simulation
    5. adding a new simulation for the instrument in the read-only database to the
       second database 
"""

class ROCacheTestCase(unittest.TestCase):
    def setUp(self):
        from pathlib import Path
        from tempfile import mkdtemp
        import restage.cache
        import mccode_antlr
        from restage.cache import cache_instr, cache_simulation_table, cache_simulation
        from restage.database import Database
        from restage.instr import collect_parameter
        from restage import SimulationEntry
        from mccode_antlr.loader import parse_mcstas_instr

        database_name = self.id().split('.')[-1] + '.db'

        self.ro_dir = Path(mkdtemp())
        self.ro_db_file = self.ro_dir.joinpath('ro_' + database_name)
        self.ro_db = Database(self.ro_db_file)

        self.orig_ro_db = restage.cache.FILESYSTEM.db_fixed
        self.orig_rw_db = restage.cache.FILESYSTEM.db_write
        restage.cache.FILESYSTEM.db_write = self.ro_db

        contents = """DEFINE INSTRUMENT simple_test_instrument(
                            par1, double par2, int par3, par4=1, string par5="string", double par6=6.6
                            )
                        TRACE
                        COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
                        COMPONENT sample = Arm() AT (0, 0, 1) ABSOLUTE
                        COMPONENT detector = Arm() AT (0, 0, 2) ABSOLUTE
                        END
                        """
        self.instr = parse_mcstas_instr(contents)

        # Set up the 'read-only' part (this functionality checked in CacheTestCase)
        instr_entry = cache_instr(self.instr, mccode_version=mccode_antlr.__version__, binary_path=self.ro_dir / "bin" / "blah")
        cache_simulation_table(instr_entry, SimulationEntry(collect_parameter(self.instr)))
        self.par = collect_parameter(self.instr, par1=1., par2=2., par3=3, par4=4., par5='five', par6=6.)
        cache_simulation(instr_entry, SimulationEntry(self.par))

        # Close the database file, and re-open it read-only
        del self.ro_db
        self.ro_db = Database(self.ro_db_file, readonly=True)
        restage.cache.FILESYSTEM.db_fixed = (self.ro_db,)

        # Make a new writable database
        self.rw_dir = Path(mkdtemp())
        self.rw_db_file = self.rw_dir.joinpath('rw_' + database_name)
        self.rw_db = Database(self.rw_db_file)
        restage.cache.FILESYSTEM.db_write = self.rw_db

    def tearDown(self):
        import restage.cache
        restage.cache.FILESYSTEM.db_fixed = self.orig_ro_db
        restage.cache.FILESYSTEM.db_write = self.orig_rw_db

        del self.ro_db
        del self.rw_db
        for file in (self.ro_db_file, self.rw_db_file):
            file.unlink(missing_ok=True)
        for directory in (self.ro_dir, self.rw_dir):
            if directory.exists():
                directory.rmdir()
            del directory

    def test_ro_simulation_retrieval(self):
        from pathlib import Path
        from restage import SimulationEntry
        from restage.cache import cache_get_instr, cache_has_simulation, cache_get_simulation
        instr_entry = cache_get_instr(self.instr)
        self.assertEqual(Path(instr_entry.binary_path), self.ro_dir / "bin" / "blah")
        self.assertTrue(cache_has_simulation(instr_entry, SimulationEntry(self.par)))
        self.assertEqual(len(cache_get_simulation(instr_entry, SimulationEntry(self.par))), 1)

    def test_rw_simulation_insertion(self):
        from pathlib import Path
        from restage import SimulationEntry
        from restage.cache import FILESYSTEM
        from restage.cache import (
            cache_get_instr, cache_has_simulation, cache_get_simulation,
            cache_simulation, cache_simulation_table
        )
        from restage.instr import collect_parameter
        instr_entry = cache_get_instr(self.instr)
        self.assertEqual(Path(instr_entry.binary_path), self.ro_dir / "bin" / "blah")

        par = collect_parameter(self.instr, par1=2., par2=3., par3=4, par4=5., par5='six', par6=7.)
        entry = SimulationEntry(par)

        self.assertTrue(cache_has_simulation(instr_entry, SimulationEntry(self.par)))
        self.assertFalse(cache_has_simulation(instr_entry, entry))

        cache_simulation(instr_entry, entry)

        self.assertEqual(len(cache_get_simulation(instr_entry, entry)), 1)

        table = cache_simulation_table(instr_entry, entry)
        self.assertEqual(len(FILESYSTEM.db_write.retrieve_simulation(table.id, entry)), 1)
        self.assertEqual(len(FILESYSTEM.db_fixed[0].retrieve_simulation(table.id, entry)), 0)