import unittest
from mcbifrost.run import MStyleRange

class MStyleRangeTestCase(unittest.TestCase):
    def test_integer_range(self):
        r = MStyleRange(1, 10, 1)
        self.assertEqual(list(r), list(range(1, 11)))
        self.assertEqual(10, (10-1)/1 + 1)
        self.assertEqual(len(r), 10)

    def test_integer_range_from_str(self):
        r = MStyleRange.from_str('1:10')
        self.assertEqual(list(r), list(range(1, 11)))
        self.assertEqual(len(r), 10)

    def test_float_range(self):
        r = MStyleRange(1.0, 10.0, 1.0)
        self.assertEqual(list(r), list(range(1, 11)))
        self.assertEqual(len(r), 10)

    def test_float_range_from_str(self):
        r = MStyleRange.from_str('1.0:10.0')
        self.assertEqual(list(r), list(range(1, 11)))
        self.assertEqual(len(r), 10)

    def test_float_range_from_str_with_step(self):
        r = MStyleRange.from_str('1.0:2.0:10.0')
        self.assertEqual(list(r), list(range(1, 11, 2)))
        self.assertEqual(len(r), 5)

        r = MStyleRange.from_str('1.0:0.2:10.0')
        # for a, b in zip(list(r), [x/10 for x in range(10, 112, 2)]):
        #     self.assertAlmostEqual(a, b)
        self.assertEqual(50, (10.0-1.0)/0.2 + 1)
        self.assertEqual(len(r), 50)



if __name__ == '__main__':
    unittest.main()
