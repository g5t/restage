from __future__ import annotations

import unittest
from mccode_antlr.compiler.check import simple_instr_compiles


def compiled_test(method, compiler: str | None = None):
    if compiler is None:
        compiler = 'cc'
    if simple_instr_compiles(compiler):
        return method

    @unittest.skip(f"Skipping due to lack of working {compiler}")
    def skipped_method(*args, **kwargs):
        return method(*args, **kwargs)

    return skipped_method


def gpu_compiled_test(method):
    return compiled_test(method, 'acc')


def mpi_compiled_test(method):
    return compiled_test(method, 'mpi/cc')


class SingleTestCase(unittest.TestCase):
    def setUp(self):
        from restage.splitrun import make_splitrun_parser
        self.parser = make_splitrun_parser()

    def test_parsing(self):
        args = self.parser.parse_args(['test.instr', 'a=1', 'b=2', '--split-at=here', '-m'])
        self.assertEqual(args.instrument, 'test.instr')
        self.assertEqual(args.parameters, ['a=1', 'b=2'])
        self.assertEqual(args.split_at, 'here')
        self.assertTrue(args.mesh)

    def test_mixed_parsing(self):
        from mccode_antlr.run.runner import sort_args
        args = self.parser.parse_args(sort_args(['test.instr', '-m', 'a=1', 'b=2', '--split-at=here']))
        self.assertEqual(args.instrument, 'test.instr')
        self.assertEqual(args.parameters, ['a=1', 'b=2'])
        self.assertEqual(args.split_at, 'here')
        self.assertTrue(args.mesh)

    def test_negative_count_throws(self):
        from restage.splitrun import si_int, si_int_limits
        with self.assertRaises(ValueError):
            si_int('-10')
        with self.assertRaises(ValueError):
            si_int_limits('10--4+10') # '10--4+10' -> ('10', '-4+10') -> (10, '-4', '10')
        with self.assertRaises(ValueError):
            # This is probably a parsing error, but would raise a negative error
            # in si_int, so is fine. '-10]-2[-1' -> ('','10]-2[-1') -> Error on int('')
            si_int_limits('-10]-2[-1')

    def test_mccode_flags(self):
        args = self.parser.parse_args(['test.instr', '-s', '123456', '-n', '1', '-d', '/a/dir', '-t', '-g'])
        self.assertEqual(args.seed, 123456)
        self.assertEqual(args.ncount, (None, 1, None))
        self.assertEqual(args.dir, '/a/dir')
        self.assertEqual(args.trace, True)
        self.assertEqual(args.gravitation, True)

        args = self.parser.parse_args(['test.instr', '-s=99999', '-n=10000', '-d=/b/dir'])
        self.assertEqual(args.seed, 99999)
        self.assertEqual(args.ncount, (None, 10000, None))
        self.assertEqual(args.dir, '/b/dir')
        self.assertEqual(args.trace, False)
        self.assertEqual(args.gravitation, False)

        args = self.parser.parse_args(['test.instr', '--seed', '888', '--ncount', '4', '--dir', '/c/dir', '--trace',
                                       '--gravitation', '--bufsiz', '1000', '--format', 'NEXUS'])
        self.assertEqual(args.seed, 888)
        self.assertEqual(args.ncount, (None, 4, None))
        self.assertEqual(args.dir, '/c/dir')
        self.assertEqual(args.trace, True)
        self.assertEqual(args.gravitation, True)
        self.assertEqual(args.bufsiz, 1000)
        self.assertEqual(args.format, 'NEXUS')

        args = self.parser.parse_args(['test.instr', '--seed=777', '--ncount=5', '--dir=/d/dir', '--bufsiz=2000',
                                       '--format=RAW'])
        self.assertEqual(args.seed, 777)
        self.assertEqual(args.ncount, (None, 5, None))
        self.assertEqual(args.dir, '/d/dir')
        self.assertEqual(args.trace, False)
        self.assertEqual(args.gravitation, False)
        self.assertEqual(args.bufsiz, 2000)
        self.assertEqual(args.format, 'RAW')

    def test_ncount_varieties(self):
        args = self.parser.parse_args(['test.instr', '--ncount=5'])
        self.assertEqual(args.ncount, (None, 5, None))
        args = self.parser.parse_args(['test.instr', '-n' ,'4k'])
        self.assertEqual(args.ncount, (None, 4000, None))
        args = self.parser.parse_args(['test.instr', '-n', '3-2+1'])
        self.assertEqual(args.ncount, (3, 2, 1))
        args = self.parser.parse_args(['test.instr', '-n', '1M]1G[1T'])
        self.assertEqual(args.ncount, (10**6, 10**9, 10**12))
        args = self.parser.parse_args(['test.instr', '-n', '1Ki}Mi{2Gi'])
        self.assertEqual(args.ncount, (2**10, 2**20, 2**31))
        args = self.parser.parse_args(['t.instr', '-n', '1.1M', '--nmin', 'M', '--nmax', '2M'])
        self.assertEqual(args.ncount, (None, 1100000, None))
        self.assertEqual(args.nmin, 1000000)
        self.assertEqual(args.nmax, 2000000)


    def test_parameters(self):
        from restage.range import MRange, Singular, parameters_to_scan, parse_scan_parameters
        args = self.parser.parse_args(['test.instr', 'a=1.0', 'b=2', 'c=3:5', 'd=blah', 'e=/data', '-m'])
        self.assertEqual(args.parameters, ['a=1.0', 'b=2', 'c=3:5', 'd=blah', 'e=/data'])
        parameters = parse_scan_parameters(args.parameters)
        self.assertTrue(isinstance(parameters['a'], Singular))
        self.assertTrue(isinstance(parameters['b'], Singular))
        self.assertTrue(isinstance(parameters['c'], MRange))
        self.assertTrue(isinstance(parameters['d'], Singular))
        self.assertTrue(isinstance(parameters['e'], Singular))
        # Singular parameters should have their maximum repetitions set to the longest MRange
        for v in parameters.values():
            self.assertEqual(len(v), 3)
        n_pts, names, scan = parameters_to_scan(parameters)
        self.assertEqual(n_pts, 3)
        self.assertEqual(names, ['a', 'b', 'c', 'd', 'e'])
        for i, values in enumerate(scan):
            self.assertEqual(len(values), 5)
            self.assertEqual(values[0], 1.0)
            self.assertEqual(values[1], 2)
            self.assertEqual(values[2], 3 + i)
            self.assertEqual(values[3], 'blah')
            self.assertEqual(values[4], '/data')

    def test_mcpl_split_parameters(self):
        args = self.parser.parse_args(['test.instr', 'a=1.0', 'b=2', 'c=3:5', 'd=blah',  'e=/data',
                                       '--mcpl-input-parameters', 'preload:1,v_smear:0.01'])
        self.assertEqual(args.parameters, ['a=1.0', 'b=2', 'c=3:5', 'd=blah', 'e=/data'])
        self.assertEqual(args.mcpl_input_parameters, [('preload', '1'), ('v_smear', '0.01')])
        args = self.parser.parse_args(['new_tst.instr', '--mcpl-output-parameters', 'preload:1',
                                       '--mcpl-input-component=MCPL_input_once'])
        self.assertEqual(args.parameters, [])
        self.assertEqual(args.mcpl_output_parameters, [('preload', '1')])
        self.assertEqual(args.mcpl_input_component, 'MCPL_input_once')


class DictWranglingTestCase(unittest.TestCase):
    def test_regularization(self):
        from restage.splitrun import regular_mccode_runtime_dict
        short_names = dict(s=1, n=2, d=3, t=4, g=5, bufsiz=6, format=7)
        long_names = dict(seed=1, ncount=2, dir=3, trace=4, gravitation=5, bufsiz=6, format=7)
        self.assertEqual(regular_mccode_runtime_dict(short_names), long_names)


class SplitRunTestCase(unittest.TestCase):
    def _skip_without_compiler(self):
        import subprocess
        from mccode_antlr.config import config
        try:
            subprocess.run([config['cc'].get(str), '--version'], check=True)
        except FileNotFoundError:
            self.skipTest(f'Compiler {config["cc"]} not found')

    def _skip_without_mcpl(self):
        import subprocess
        try:
            subprocess.run(['mcpl-config', '--version'], check=True)
        except FileNotFoundError:
            self.skipTest('mcpl-config not found')
        except RuntimeError:
            self.skipTest('mcpl-config failed')

    def _skip_checks(self):
        self._skip_without_compiler()
        self._skip_without_mcpl()

    def _define_instr(self):
        from math import pi, asin, sqrt
        from mccode_antlr.loader.loader import parse_mcstas_instr

        d_spacing = 3.355  # (002) for Highly-ordered Pyrolytic Graphite
        mean_energy = 5.0
        energy_width = 1.0
        hbar_sq_over_m = 2.0722  # meV Angstrom^2
        mean_ki = sqrt(mean_energy / hbar_sq_over_m)
        min_ki = sqrt((mean_energy - energy_width) / hbar_sq_over_m)
        max_ki = sqrt((mean_energy + energy_width) / hbar_sq_over_m)
        min_a1 = asin(pi / d_spacing / max_ki) * 180 / pi
        max_a1 = asin(pi / d_spacing / min_ki) * 180 / pi
        instr = f"""
        DEFINE INSTRUMENT splitRunTest(a1=0, a2=0, virtual_source_x=0.05, virtual_source_y=0.1)
        TRACE
        COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
        COMPONENT source = Source_simple(yheight=0.25, xwidth=0.2, dist=1.5, focus_xw=0.06, focus_yh=0.12,
                                         E0={mean_energy}, dE={energy_width})
                           AT (0, 0, 0) RELATIVE origin
        COMPONENT m0 = PSD_monitor(xwidth=0.1, yheight=0.15, nx=100, ny=160, restore_neutron=1) AT (0, 0, 0.01) RELATIVE PREVIOUS
        COMPONENT guide1 = Guide_gravity(w1 = 0.06, h1 = 0.12, w2 = 0.05, h2 = 0.1, l = 15, m = 8) 
                          AT (0, 0, 1.5) RELATIVE  PREVIOUS
        COMPONENT guide1_end = Arm() AT (0, 0, 15) RELATIVE PREVIOUS
        COMPONENT m1 = PSD_monitor(xwidth=0.1, yheight=0.15, nx=100, ny=160, restore_neutron=1) AT (0, 0, 0.01) RELATIVE PREVIOUS
        COMPONENT monitor = E_monitor(xwidth=0.05, yheight=0.1, nE=50,
                                      Emin={mean_energy - 2 * energy_width}, Emax={mean_energy + 2 * energy_width})
                          AT (0, 0, 0.01) RELATIVE PREVIOUS
        COMPONENT image = PSD_monitor(xwidth=0.1, yheight=0.15, nx=100, ny=160) AT (0, 0, 0.01) RELATIVE PREVIOUS
        COMPONENT guide2 = Guide_gravity(w1 = 0.05, h1 = 0.1, l = 15, m = 8) AT (0, 0, 0.01) RELATIVE PREVIOUS
        COMPONENT guide2_end = Arm() AT (0, 0, 15) RELATIVE PREVIOUS
        COMPONENT aperture = Slit(xwidth=virtual_source_x, yheight=virtual_source_y) AT (0, 0, 0.01) RELATIVE PREVIOUS
        COMPONENT before_split = PSD_monitor(xwidth=0.1, yheight=0.15, nx=100, ny=160) AT (0, 0, 0.01) RELATIVE PREVIOUS
        COMPONENT split_at = Arm() AT (0, 0, 0.0001) RELATIVE PREVIOUS
        COMPONENT after_split = PSD_monitor(xwidth=0.1, yheight=0.15, nx=100, ny=160) AT (0, 0, 0.01) RELATIVE PREVIOUS
        COMPONENT mono_point = Arm() AT (0, 0, 0.8) RELATIVE split_at
        COMPONENT mono = Monochromator_curved(zwidth = 0.02, yheight = 0.02, NH = 13, NV = 7, DM={d_spacing}) 
                         AT (0, 0, 0) RELATIVE  mono_point ROTATED (0, a1, 0) RELATIVE mono_point
        COMPONENT sample_arm = Arm() AT (0, 0, 0) RELATIVE mono_point ROTATED (0, a2, 0) RELATIVE mono_point
        COMPONENT detector = Monitor(xwidth=0.01, yheight=0.05) AT (0, 0, 0.8) RELATIVE sample_arm
        END
        """
        return parse_mcstas_instr(instr), min_a1, max_a1

    def setUp(self) -> None:
        from pathlib import Path
        from tempfile import mkdtemp
        self._skip_checks()
        self.instr, self.min_a1, self.max_a1 = self._define_instr()
        with Path().joinpath('splitRunTest.instr').open('w') as file:
            self.instr.to_file(file)
        self.dir = Path(mkdtemp())

    def tearDown(self) -> None:
        import shutil
        if self.dir.exists():
            shutil.rmtree(self.dir)

    def test_simple_scan(self):
        """This test requires MCPL shared libraries to work

        For some unexplored reason, MCPL shared libraries are not found in their
        default installed location, /usr/local/lib64/libmcpl.so; so this test must
        be invoked with that location specified, e.g.,
            $ LD_LIBRARY_PATH=/usr/local/lib64 pytest test/test_single.py -k test_simple_scan
        """
        # Scanning a1 and a2 with a2=2*a1 should produce approximately the same intensity for all points
        # as long as a1 is between the limits of min_a1 and max_a1
        from restage.splitrun import splitrun
        from restage.range import parse_scan_parameters
        # since the source emits a narrow energy bandwidth, we only detect neutrons over a small (a1,a2) range
        scan = parse_scan_parameters([f'a1={self.min_a1}:0.5:{self.max_a1}', f'a2={2*self.min_a1}:{2*self.max_a1}'])

        # The way that McCode handles directories is extremely finicky. If the _actual_ simulation directory
        # exists, the simulation will fail (even if it is empty!), but if the _parent_ directory does not exist,
        # the simulation will fail. So we need to create the parent directory, but not the simulation directory.
        # The real trick is that the simulation directory is a subdirectory of the one specified here.
        output = self.dir.joinpath('test_simple_scan')
        if not output.exists():
            output.mkdir(parents=True)

        # run the scan
        splitrun(self.instr, scan, precision={}, split_at='split_at', grid=False,
                 ncount=10_000,
                 dir=output,
                 mcpl_output_parameters={'weight_mode': '1'},
                 mcpl_input_component='MCPL_input_once',
                 mcpl_input_parameters={'preload': '1'}
                 )

        # check the scan directory for output
        for x in self.dir.glob('**/*.dat'):
            print(x)

        # It would be nice to check that the produced mccode.sim and mccode.dat files look right.

    @mpi_compiled_test
    def test_parallel_scan(self):
        """This test requires mpicc and MCPL shared libraries to work

        On some systems (Fedora at least) specifying which MPI to use requires using
        the `module` system, e.g.,
            $ module load mpi/openmpi-x86_64
        And for some unexplored reason, MCPL shared libraries are not found in their
        default installed location, /usr/local/lib64/libmcpl.so; so this test must
        be invoked with that location specified, e.g.,
            $ LD_LIBRARY_PATH=/usr/local/lib64 pytest test/test_single.py -k test_parallel_scan

        Another potential issue to address, the `MCPL_*.comp` components may use a
        special `@MCPLFLAGS@` directive to read flags from a configuration file instead
        of using the provided `mcpl-config --show buildflags` command.
        You might need to use `mccode-antlr config save -v` to create/locate the
        mccode-antlr `config.yml` file then edit/add the entry for `flags.mcpl` with
        the output of the tool above.
        """
        from restage.splitrun import splitrun
        from restage.range import parse_scan_parameters
        scan = parse_scan_parameters([f'a1={self.min_a1}:0.5:{self.max_a1}', f'a2={2 * self.min_a1}:{2 * self.max_a1}'])
        output = self.dir.joinpath('test_parallel_scan')
        if not output.exists():
            output.mkdir(parents=True)
        splitrun(self.instr, scan, precision={}, split_at='split_at', grid=False, ncount=100_000, dir=output,
                 parallel=True, process_count=4,
                 mcpl_output_parameters={'weight_mode': '1'},
                 mcpl_input_component='MCPL_input_once',
                 mcpl_input_parameters={'preload': '1'}
                 )

        # check the scan directory for output
        for x in self.dir.glob('**/*.dat'):
            print(x)


if __name__ == '__main__':
    unittest.main()
