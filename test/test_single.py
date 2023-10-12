import unittest


class SingleTestCase(unittest.TestCase):
    def setUp(self):
        from mcbifrost.single import make_single_parser
        self.parser = make_single_parser()

    def test_parsing(self):
        args = self.parser.parse_args(['test.instr', 'a=1', 'b=2', '--split-at=here', '-m'])
        self.assertEqual(args.instrument, ['test.instr'])
        self.assertEqual(args.parameters, ['a=1', 'b=2'])
        self.assertEqual(args.split_at, ['here'])
        self.assertTrue(args.mesh)

    def test_mccode_flags(self):
        args = self.parser.parse_args(['test.instr', '-s', '123456', '-n', '-1', '-d', '/a/dir', '-t', '-g'])
        self.assertEqual(args.seed, [123456])
        self.assertEqual(args.ncount, [-1])
        self.assertEqual(args.dir, ['/a/dir'])
        self.assertEqual(args.trace, True)
        self.assertEqual(args.gravitation, True)

        args = self.parser.parse_args(['test.instr', '-s=99999', '-n=10000', '-d=/b/dir'])
        self.assertEqual(args.seed, [99999])
        self.assertEqual(args.ncount, [10000])
        self.assertEqual(args.dir, ['/b/dir'])
        self.assertEqual(args.trace, False)
        self.assertEqual(args.gravitation, False)

        args = self.parser.parse_args(['test.instr', '--seed', '888', '--ncount', '4', '--dir', '/c/dir', '--trace',
                                       '--gravitation', '--bufsiz', '1000', '--format', 'NEXUS'])
        self.assertEqual(args.seed, [888])
        self.assertEqual(args.ncount, [4])
        self.assertEqual(args.dir, ['/c/dir'])
        self.assertEqual(args.trace, True)
        self.assertEqual(args.gravitation, True)
        self.assertEqual(args.bufsiz, [1000])
        self.assertEqual(args.format, ['NEXUS'])

        args = self.parser.parse_args(['test.instr', '--seed=777', '--ncount=5', '--dir=/d/dir', '--bufsiz=2000',
                                       '--format=RAW'])
        self.assertEqual(args.seed, [777])
        self.assertEqual(args.ncount, [5])
        self.assertEqual(args.dir, ['/d/dir'])
        self.assertEqual(args.trace, False)
        self.assertEqual(args.gravitation, False)
        self.assertEqual(args.bufsiz, [2000])
        self.assertEqual(args.format, ['RAW'])

    def test_parameters(self):
        from mcbifrost.single import parse_single_parameters
        from mcbifrost.range import MRange, Singular, parameters_to_scan
        args = self.parser.parse_args(['test.instr', 'a=1.0', 'b=2', 'c=3:5', 'd=blah', 'e=/data', '-m'])
        self.assertEqual(args.parameters, ['a=1.0', 'b=2', 'c=3:5', 'd=blah', 'e=/data'])
        parameters = parse_single_parameters(args.parameters)
        self.assertTrue(isinstance(parameters['a'], Singular))
        self.assertTrue(isinstance(parameters['b'], Singular))
        self.assertTrue(isinstance(parameters['c'], MRange))
        self.assertTrue(isinstance(parameters['d'], Singular))
        self.assertTrue(isinstance(parameters['e'], Singular))
        # Singular parameters should have their maximum repetitions set to the longest MRange
        for v in parameters.values():
            self.assertEqual(len(v), 3)
        names, scan = parameters_to_scan(parameters)
        self.assertEqual(names, ['a', 'b', 'c', 'd', 'e'])
        for i, values in enumerate(scan):
            self.assertEqual(len(values), 5)
            self.assertEqual(values[0], 1.0)
            self.assertEqual(values[1], 2)
            self.assertEqual(values[2], 3 + i)
            self.assertEqual(values[3], 'blah')
            self.assertEqual(values[4], '/data')


if __name__ == '__main__':
    unittest.main()
