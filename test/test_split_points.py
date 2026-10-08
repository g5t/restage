import unittest


def instrument(initialize: str = ''):
    """A beamline with three candidate split points, a, b and c.

    `p` is used before a, `m` between a and b, `q` between b and c, and `a3` -- turning the
    sample -- after c.
    """
    from mccode_antlr.loader import parse_mcstas_instr
    return parse_mcstas_instr(f"""
DEFINE INSTRUMENT splits(p=1, m=0.1, q=0.1, a3=0)
INITIALIZE
%{{
{initialize}
%}}
TRACE
COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
COMPONENT guide = Slit(xwidth=p, yheight=0.1) AT (0, 0, 1) ABSOLUTE
COMPONENT a = Arm() AT (0, 0, 2) ABSOLUTE
COMPONENT mask = Slit(xwidth=m, yheight=0.1) AT (0, 0, 3) ABSOLUTE
COMPONENT b = Arm() AT (0, 0, 4) ABSOLUTE
COMPONENT jaws = Slit(xwidth=q, yheight=0.1) AT (0, 0, 5) ABSOLUTE
COMPONENT c = Arm() AT (0, 0, 6) ABSOLUTE
COMPONENT stage = Arm() AT (0, 0, 7) ABSOLUTE ROTATED (0, a3, 0) ABSOLUTE
COMPONENT sample = Slit(xwidth=0.01, yheight=0.01) AT (0, 0, 0) RELATIVE stage
END
""")


def points(**values):
    """A mesh scan over ``values``, each a list."""
    from itertools import product
    names = list(values)
    return [dict(zip(names, combo)) for combo in product(*values.values())]


class ChooseSplitTest(unittest.TestCase):
    def choose(self, scan, initialize='', **kwargs):
        from restage.split_points import choose_split
        return choose_split(instrument(initialize), 'a,b,c', scan, **kwargs)

    def test_a_sample_rotation_scan_splits_just_before_the_sample(self):
        self.assertEqual(self.choose(points(a3=[0, 1, 2])), 'c')

    def test_a_scan_before_every_candidate_still_shares_at_the_latest(self):
        """Two values before a, ten after c: two primaries up to c serve twenty points."""
        self.assertEqual(self.choose(points(p=[1, 2], a3=list(range(10)))), 'c')

    def test_a_scan_between_candidates_stops_before_it(self):
        self.assertEqual(self.choose(points(m=[0.1, 0.2], q=[0.1, 0.2, 0.3])), 'b')

    def test_no_shared_primary_runs_unsplit(self):
        self.assertIsNone(self.choose(points(p=[1, 2, 3])))

    def test_one_point_splits_at_the_latest(self):
        """Nothing is shared within the run; the split is for the cache to reuse the primary."""
        self.assertEqual(self.choose(points(a3=[0])), 'c')
        self.assertEqual(self.choose([]), 'c')

    def test_candidates_are_taken_in_beam_order(self):
        from restage.split_points import choose_split
        self.assertEqual(choose_split(instrument(), 'c,a,b', points(q=[1, 2])), 'b')

    def test_a_scanned_parameter_in_initialize_is_refused(self):
        from restage.split_points import SplitError
        with self.assertRaisesRegex(SplitError, 'a3'):
            self.choose(points(a3=[0, 1, 2]), initialize='double turned = a3;')

    def test_the_refusal_can_be_overridden_and_then_counts_the_block(self):
        """Every a3 then needs its own primary, so no split shares one."""
        self.assertIsNone(self.choose(points(a3=[0, 1, 2]), initialize='double turned = a3;',
                                      allow_block_parameters=True))

    def test_a_block_parameter_the_primary_already_needs_costs_nothing(self):
        self.assertEqual(self.choose(points(p=[1, 2], a3=[0, 1]), initialize='double w = p;'), 'c')

    def test_extra_primary_parameters_count(self):
        """The energy parameters restage hands the primary whether or not it uses them."""
        self.assertIsNone(self.choose(points(ei=[1, 2, 3]), extra={'ei'}))

    def test_an_unknown_candidate_is_an_error(self):
        from restage.split_points import SplitError, choose_split
        with self.assertRaisesRegex(SplitError, 'nowhere'):
            choose_split(instrument(), 'a,nowhere', points(a3=[0, 1]))

    def test_a_candidate_inside_a_group_is_an_error(self):
        from mccode_antlr.loader import parse_mcstas_instr
        from restage.split_points import SplitError, choose_split
        instr = parse_mcstas_instr("""
DEFINE INSTRUMENT grouped(a3=0)
TRACE
COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
COMPONENT left = Slit(xwidth=0.1, yheight=0.1) AT (0, 0, 1) ABSOLUTE GROUP pair
COMPONENT between = Arm() AT (0, 0, 1) ABSOLUTE
COMPONENT right = Slit(xwidth=0.1, yheight=0.1) AT (0, 0, 1) ABSOLUTE GROUP pair
COMPONENT stage = Arm() AT (0, 0, 2) ABSOLUTE ROTATED (0, a3, 0) ABSOLUTE
END
""")
        with self.assertRaisesRegex(SplitError, 'GROUP'):
            choose_split(instr, 'between', points(a3=[0, 1]))


if __name__ == '__main__':
    unittest.main()
