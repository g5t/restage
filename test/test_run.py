import unittest
from mcbifrost.run import make_run_parser


class RunParserTestCase(unittest.TestCase):
    def setUp(self):
        self.parser = make_run_parser()

    def test_instrument(self):
        args = self.parser.parse_args(['test.instr'])
        self.assertEqual(args.instrument, ['test.instr'])

    def test_parameters(self):
        args = self.parser.parse_args(['test.instr', 'a=1', 'b=2'])
        self.assertEqual(args.parameters, ['a=1', 'b=2'])

    def test_runtime_parameters(self):
        args = self.parser.parse_args(['test.instr', '-R', 'a=1', '-R', 'b=2'])
        self.assertEqual(args.R, ['a=1', 'b=2'])

    def test_grid(self):
        args = self.parser.parse_args(['test.instr'])
        self.assertEqual(args.grid, False)
        args = self.parser.parse_args(['test.instr', '-g'])
        self.assertEqual(args.grid, True)

    def test_multiple_parameters(self):
        args = self.parser.parse_args(['test.instr', 'a=1:3', 'b', '4:6', 'c=3', '-R', 'a=15', '-Rb=16'])
        self.assertEqual(args.parameters, ['a=1:3', 'b', '4:6', 'c=3'])
        self.assertEqual(args.R, ['a=15', 'b=16'])

    def test_scan_parameters(self):
        from mcbifrost.run import parse_scan_parameters
        from mcbifrost.range import MRange
        args = self.parser.parse_args(['test.instr', 'a=1:3', 'b', '4:0.2:6', 'c=3'])
        self.assertEqual(args.parameters, ['a=1:3', 'b', '4:0.2:6', 'c=3'])
        ranges = parse_scan_parameters(args.parameters)
        self.assertEqual(list(ranges['a']), [1, 2, 3])
        self.assertEqual(ranges['b'], MRange(4, 6, 0.2))
        self.assertEqual(ranges['c'], MRange(3, 3, 1))

        extra_parameters = ['a=1:4', 'not_a_scan_parameter', 'b=1:10']
        self.assertRaises(ValueError, parse_scan_parameters, extra_parameters)

    def test_scan_points(self):
        from mcbifrost.run import parse_scan_parameters, parameters_to_scan
        args = self.parser.parse_args(['test.instr', 'a=1:3', 'b', '5:0.5:6', 'c=3'])
        ranges = parse_scan_parameters(args.parameters)
        names, points = parameters_to_scan(ranges)
        self.assertEqual(names, ['a', 'b', 'c'])
        self.assertEqual(list(points), [(1, 5, 3), (2, 5.5, 3), (3, 6, 3)])

    def test_grid_scan_points(self):
        from mcbifrost.run import parse_scan_parameters, parameters_to_scan
        grid_parameters = ['a=1:3', 'b=1:4']
        grid_ranges = parse_scan_parameters(grid_parameters)
        self.assertRaises(ValueError, parameters_to_scan, grid_ranges)
        names, points = parameters_to_scan(grid_ranges, grid=True)
        self.assertEqual(names, ['a', 'b'])
        self.assertEqual(list(points), [
            (1, 1), (1, 2), (1, 3), (1, 4),
            (2, 1), (2, 2), (2, 3), (2, 4),
            (3, 1), (3, 2), (3, 3), (3, 4)])


if __name__ == '__main__':
    unittest.main()
