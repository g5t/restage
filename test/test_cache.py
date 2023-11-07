import unittest


class CacheTestCase(unittest.TestCase):
    def setUp(self):
        from pathlib import Path
        from tempfile import mkdtemp
        import restage.cache
        from restage.database import Database
        from mccode_antlr.loader import parse_mcstas_instr
        self.db_dir = Path(mkdtemp())
        self.db_file = self.db_dir.joinpath('test_database.db')
        self.db = Database(self.db_file)

        self.orig_db = restage.cache.DATABASE
        restage.cache.DATABASE = self.db

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

    def tearDown(self) -> None:
        import restage.cache
        restage.cache.DATABASE = self.orig_db
        del self.orig_db

        del self.db
        if self.db_file.exists():
            self.db_file.unlink()
        del self.db_file
        if self.db_dir.exists():
            self.db_dir.rmdir()
        del self.db_dir

    def _check_attrs(self, obj, expected_values):
        for k, v in expected_values.items():
            self.assertTrue(hasattr(obj, k))
            self.assertEqual(getattr(obj, k), v)

    def test_simple_instr_file(self):
        from restage import InstrEntry
        from restage.cache import cache_instr
        import mccode_antlr
        mccode_version = mccode_antlr.__version__
        file_contents = str(self.instr)  # ideally this would be contents, but the parsed representation adds things
        binary_path = '/not/a/real/binary/path'
        retrieved = cache_instr(self.instr, mccode_version=mccode_version, binary_path=binary_path)
        self.assertTrue(isinstance(retrieved, InstrEntry))
        self._check_attrs(retrieved, {'file_contents': file_contents, 'binary_path': binary_path,
                                      'mccode_version': mccode_version})
        from_db = self.db.retrieve_instr_file(retrieved.id)
        self.assertEqual(len(from_db), 1)
        self._check_attrs(from_db[0], {'file_contents': file_contents, 'binary_path': binary_path,
                                       'mccode_version': mccode_version})

    def test_nexus_structure(self):
        pass

    def test_simulation_table(self):
        from restage import SimulationTableEntry, SimulationEntry
        from restage.instr import collect_parameter
        from restage.cache import cache_instr, cache_simulation_table
        import mccode_antlr
        mccode_version = mccode_antlr.__version__
        binary_path = '/not/a/real/binary/path'
        instr_entry = cache_instr(self.instr, mccode_version=mccode_version, binary_path=binary_path)

        sim_entry = SimulationEntry(collect_parameter(self.instr))
        sim_table = cache_simulation_table(instr_entry, sim_entry)

        self.assertTrue(isinstance(sim_table, SimulationTableEntry))
        self._check_attrs(sim_table, {'id': instr_entry.id, 'name': f'pst_{instr_entry.id}',
                                        'parameters': list(sim_entry.parameter_values.keys())})
        from_db = self.db.retrieve_simulation_table(instr_entry.id)
        self.assertEqual(len(from_db), 1)
        self._check_attrs(from_db[0], {'id': instr_entry.id, 'name': f'pst_{instr_entry.id}',
                                       'parameters': list(sim_entry.parameter_values.keys())})

    def test_simulation(self):
        from restage.tables import SimulationEntry, best_simulation_entry_match
        from restage.instr import collect_parameter
        from restage.cache import cache_instr, cache_simulation, cache_has_simulation, cache_get_simulation
        import mccode_antlr
        mccode_version = mccode_antlr.__version__
        binary_path = '/not/a/real/binary/path'
        instr_entry = cache_instr(self.instr, mccode_version=mccode_version, binary_path=binary_path)

        # double, double, int, double, string, double
        par0 = collect_parameter(self.instr, par1=1.1, par2=2.2, par3=3, par4=4.4, par5='five', par6=6.6)
        par1 = collect_parameter(self.instr, par1=1.9, par2=2.2, par3=3, par4=4.4, par5='five', par6=6.6)
        par2 = collect_parameter(self.instr, par1=1.1, par2=2.9, par3=3, par4=4.4, par5='five', par6=6.6)
        par3 = collect_parameter(self.instr, par1=1.1, par2=2.2, par3=4, par4=4.4, par5='five', par6=6.6)
        par4 = collect_parameter(self.instr, par1=1.1, par2=2.2, par3=3, par4=4.9, par5='five', par6=6.6)
        par5 = collect_parameter(self.instr, par1=1.1, par2=2.2, par3=3, par4=4.4, par5='six', par6=6.6)
        par6 = collect_parameter(self.instr, par1=1.1, par2=2.2, par3=3, par4=4.4, par5='five', par6=6.9)
        pars = (par0, par1, par2, par3, par4, par5, par6)
        for par in pars:
            cache_simulation(instr_entry, SimulationEntry(par))  # automatically inserts the table if it doesn't exist
        for par in pars:
            self.assertTrue(cache_has_simulation(instr_entry, SimulationEntry(par)))
            self.assertEqual(len(cache_get_simulation(instr_entry, SimulationEntry(par))), 1)

        # Now 'repeat' simulations with a specified seed
        for seed, par in enumerate(pars):
            cache_simulation(instr_entry, SimulationEntry(par, seed=seed))
        for seed, par in enumerate(pars):
            self.assertTrue(cache_has_simulation(instr_entry, SimulationEntry(par)))
            self.assertTrue(cache_has_simulation(instr_entry, SimulationEntry(par, seed=seed)))
            self.assertEqual(len(cache_get_simulation(instr_entry, SimulationEntry(par))), 2)
            self.assertEqual(len(cache_get_simulation(instr_entry, SimulationEntry(par, seed=seed))), 1)

        # and again where we've specified a number of particles
        for n, par in enumerate(pars):
            cache_simulation(instr_entry, SimulationEntry(par, ncount=10000+n))
        for n, par in enumerate(pars):
            self.assertTrue(cache_has_simulation(instr_entry, SimulationEntry(par)))
            matches = cache_get_simulation(instr_entry, SimulationEntry(par))
            self.assertEqual(len(matches), 3)
            best = best_simulation_entry_match(matches, SimulationEntry(par))
            self.assertEqual(best.ncount, 10000+n)


if __name__ == '__main__':
    unittest.main()
