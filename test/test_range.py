import unittest
from restage.range import MRange, Singular


class MRangeTestCase(unittest.TestCase):
    def test_integer_range(self):
        r = MRange(1, 10, 1)
        self.assertEqual(list(r), list(range(1, 11)))
        self.assertEqual(10, (10-1)/1 + 1)
        self.assertEqual(len(r), 10)

    def test_invalid_range(self):
        self.assertRaises(ValueError, MRange, 1, 1, 1)
        self.assertRaises(ZeroDivisionError, MRange, 1, 10, 0)

    def test_integer_range_from_str(self):
        r = MRange.from_str('1:10')
        self.assertEqual(list(r), list(range(1, 11)))
        self.assertEqual(len(r), 10)

    def test_float_range(self):
        r = MRange(1.0, 10.0, 1.0)
        self.assertEqual(list(r), list(range(1, 11)))
        self.assertEqual(len(r), 10)

    def test_float_range_from_str(self):
        r = MRange.from_str('1.0:10.0')
        self.assertEqual(list(r), list(range(1, 11)))
        self.assertEqual(len(r), 10)

    def test_float_range_from_str_with_step(self):
        r = MRange.from_str('1.0:2.0:10.0')
        self.assertEqual(list(r), list(range(1, 11, 2)))
        self.assertEqual(len(r), 5)

        r = MRange.from_str('1.0:0.2:10.0')
        for a, b in zip(list(r), [x/10 for x in range(10, 102, 2)]):
            self.assertAlmostEqual(a, b)
        self.assertEqual(46, (10.0-1.0)/0.2 + 1)
        self.assertEqual(len(r), 46)


class SingularTestCase(unittest.TestCase):
    def test_maximum(self):
        s = Singular(1, 10)
        self.assertEqual(list(s), [1]*10)
        self.assertEqual(len(s), 10)

    def test_two_maxima(self):
        s10 = Singular(1, 10)
        s20 = Singular(2, 20)
        s = zip(s10, s20)
        self.assertEqual(list(s), [(1, 2)]*10)

    def test_one_infinite(self):
        s_inf = Singular(1)
        s_20 = Singular(2, 20)
        s = zip(s_inf, s_20)
        self.assertEqual(list(s), [(1, 2)] * 20)

        s = zip(s_20, s_inf)
        self.assertEqual(list(s), [(2, 1)] * 20)


if __name__ == '__main__':
    unittest.main()
