import unittest


def parameters(names: tuple[str, ...]):
    from itertools import product
    return tuple(x + y for x, y in product(names, ('speed', 'delay')))


CHOPPER_NAMES = ('pulse_shaping', 'frame_overlap', 'bandwidth')
CHOPPERS = tuple(f'{name}_chopper_{no}' for name in CHOPPER_NAMES for no in (1, 2))
OLD_CHOPPERS = tuple(f'{name}{no}' for name in ('ps', 'fo', 'bw') for no in (1, 2))


class BIFROSTEnergyTestCase(unittest.TestCase):
    def setUp(self):
        from mccode_antlr.loader import parse_mcstas_instr
        instr = f"""DEFINE INSTRUMENT this_IS_NOT_BIFROST(
        pulse_shaping_chopper_1speed, pulse_shaping_chopper_1delay, pulse_shaping_chopper_2speed, pulse_shaping_chopper_2delay, frame_overlap_chopper_1speed, frame_overlap_chopper_1delay, bandwidth_chopper_1speed, bandwidth_chopper_1delay, bandwidth_chopper_2speed, bandwidth_chopper_2delay
        )
        TRACE
        COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
        COMPONENT pulse_shaping_chopper_1 = DiskChopper(theta_0=170, radius=0.35, nu=pulse_shaping_chopper_1speed, delay=pulse_shaping_chopper_1delay) AT (0, 0, 1) RELATIVE PREVIOUS
        COMPONENT pulse_shaping_chopper_2 = DiskChopper(theta_0=170, radius=0.35, nu=pulse_shaping_chopper_2speed, delay=pulse_shaping_chopper_2delay) AT (0, 0, 0.02) RELATIVE pulse_shaping_chopper_1
        COMPONENT frame_overlap_chopper_1 = DiskChopper(theta_0=110, radius=0.35, nu=frame_overlap_chopper_1speed, delay=frame_overlap_chopper_1delay) AT (0, 0, 12) RELATIVE pulse_shaping_chopper_2
        COMPONENT frame_overlap_chopper_2 = DiskChopper(theta_0=115, radius=0.35, nu=frame_overlap_chopper_1speed, delay=frame_overlap_chopper_1delay) AT (0, 0, 4) RELATIVE frame_overlap_chopper_1
        COMPONENT bandwidth_chopper_1 = DiskChopper(theta_0=110, radius=0.35, nu=bandwidth_chopper_1speed, delay=bandwidth_chopper_1delay) AT (0, 0, 80) RELATIVE frame_overlap_chopper_2
        COMPONENT bandwidth_chopper_2 = DiskChopper(theta_0=115, radius=0.35, nu=bandwidth_chopper_2speed, delay=bandwidth_chopper_2delay) AT (0, 0, 0.02) RELATIVE bandwidth_chopper_1
        COMPONENT sample = Arm() AT (0, 0, 80) RELATIVE bandwidth_chopper_2
        END
        """
        self.instr = parse_mcstas_instr(instr)

    def test_names(self):
        from restage.energy import get_energy_parameter_names
        energy_names = get_energy_parameter_names(self.instr.name)
        for name in ('e', 'energy', 'ei', 'wavelength', 'lambda', 'time', 't', 'order'):
            self.assertTrue(name in energy_names)

    def test_parameters_to_scan(self):
        from mccode_antlr.run.range import MRange, Singular, parameters_to_scan
        order = Singular(14, 1)
        time = MRange(0.0001, 0.002248, 0.0002)
        ei = MRange(1.7, 24.7, 0.5)
        all_order = list(order)
        all_times = list(time)
        all_ei = list(ei)
        self.assertEqual(len(all_order), 1)
        self.assertEqual(len(all_times), 11)
        self.assertEqual(len(all_ei), 47)
        for x, y in zip(all_order, range(1)):
            o = y * 1 + 14
            self.assertAlmostEqual(x, o)
        for x, y in zip(all_times, range(11)):
            t = y * 0.0002 + 0.0001
            self.assertAlmostEqual(x, t)
        for x, y in zip(all_ei, range(47)):
            e = y * 0.5 + 1.7
            self.assertAlmostEqual(x, e)

        scan_parameters = dict(order=order, time=time, ei=ei)
        npts, names, points = parameters_to_scan(scan_parameters, grid=True)
        self.assertEqual(npts, 47*11)
        self.assertEqual(names, ['order', 'time', 'ei'])
        all_points = list(points)
        self.assertEqual(len(all_points), 47*11)
        for point in points:
            self.assertEqual(len(point), 3)

        # the orientation of the grid is not super important, but it should be consistent
        # with the order of the parameters
        for i, point in enumerate(points):
            row = i // len(all_times)
            col = i % len(all_times)
            self.assertEqual(point[0], all_order[0])
            self.assertAlmostEqual(point[1], all_times[col])
            self.assertAlmostEqual(point[2], all_ei[row])

    def test_translator(self):
        from restage.energy import energy_to_chopper_translator
        from restage.energy import bifrost_translate_energy_to_chopper_parameters
        from mccode_antlr.run.range import MRange, Singular, parameters_to_scan


        translator = energy_to_chopper_translator(self.instr.name)
        self.assertEqual(translator, bifrost_translate_energy_to_chopper_parameters)

        order = Singular(14,  1)
        time = MRange(0.0001, 0.002248, 0.0002)
        ei = MRange(1.7, 24.7, 0.5)
        scan_parameters = dict(order=order, time=time, ei=ei)

        spts, names, points = parameters_to_scan(scan_parameters, grid=True)

        self.assertEqual(47*11, spts)
        self.assertEqual(names, ['order', 'time', 'ei'])

        chopper_parameters = parameters(CHOPPERS)
        for point in points:
            kv = {k: v for k, v in zip(names, point)}
            translated = translator(kv)
            for x in chopper_parameters:
                self.assertTrue(x in translated)

            self.assertEqual(len(translated), len(chopper_parameters))
            self.assertAlmostEqual(translated['bandwidth_chopper_1speed'], 14.0)
            self.assertAlmostEqual(translated['bandwidth_chopper_2speed'], -14.0)
            self.assertAlmostEqual(translated['frame_overlap_chopper_1speed'], 14.0)
            self.assertAlmostEqual(translated['frame_overlap_chopper_1speed'], 14.0)
            self.assertAlmostEqual(translated['pulse_shaping_chopper_1speed'], 14*14.0)
            self.assertAlmostEqual(translated['pulse_shaping_chopper_2speed'], 14*14.0)

    def test_calculations(self):
        """restage's Python arithmetic against chopcal's compiled arithmetic, exactly.

        The two implement the same formulas over constants derived the same way, so they
        agree to the last bit rather than to some physical tolerance. This used to be
        compared with ``delta=max(1.0, abs(expected_phase) * 1e-3)`` -- a floor of a whole
        *degree*, 198 microseconds on a 14 Hz disk -- which was doing all the work for the
        frame-overlap and bandwidth disks and hid up to 0.32 degrees of real disagreement.

        ``rel_tol=1e-15`` is about four ulp. Bit-exact equality holds on every platform
        checked, but chopcal arrives as a compiled wheel and a different compiler could
        contract a multiply-add somewhere; a few ulp absorbs that while still being some
        twelve orders of magnitude tighter than what it replaces. What matters is that a
        tolerance this size cannot hide a *physical* disagreement, which the old one could.
        """
        from math import isclose
        from chopcal import bifrost as mcstas_bifrost_calculation
        from restage.bifrost_choppers import SLIT_CROSSING
        from restage.energy import bifrost_translate_energy_to_chopper_parameters

        smallest_energy = 0.75  # ~4 full source periods to reach the sample
        largest_energy = 25.    # a guess, but depends on the source spectra
        n_energy = 40
        d_energy = (largest_energy - smallest_energy) / n_energy

        # chopcal takes no order: it always starts at 14 and reduces when the requested
        # burst is longer than a slit crossing. That reduction is the way in -- a burst just
        # inside the band that floors to `order` makes chopcal produce exactly that order,
        # so every order restage supports can be compared without chopcal growing an
        # argument. It also checks restage's direct order against chopcal's reduced one,
        # which is a stronger statement than either route alone.
        for order in range(1, 15):
            time = SLIT_CROSSING / order * 0.999
            for energy_index in range(n_energy):
                energy = smallest_energy + energy_index * d_energy

                kv = {'order': order, 'time': time, 'ei': energy}
                translated = bifrost_translate_energy_to_chopper_parameters(kv)
                from_mcstas = mcstas_bifrost_calculation(energy_min=energy, shaping_time=time)

                for o, x in zip(OLD_CHOPPERS, CHOPPERS):
                    chopper = from_mcstas[o]
                    where = f'{o} at order {order}, {energy:.2f} meV'
                    self.assertEqual(translated[f'{x}speed'], chopper.speed, where)
                    self.assertTrue(
                        isclose(translated[f'{x}delay'], chopper.delay, rel_tol=1e-15),
                        f'{where}: {translated[f"{x}delay"]!r} != {chopper.delay!r}')

    def test_the_reduction_warns_rather_than_prints(self):
        """A burst longer than a slit crossing cannot be made, and saying so through
        `warnings` rather than `print` is what lets a caller filter, capture or assert on
        it. A scan is thousands of points and each one would otherwise write a line."""
        import warnings
        from restage.bifrost_choppers import SLIT_CROSSING, calculate

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            result = calculate(14, SLIT_CROSSING / 2 * 1.001, 5.0, CHOPPERS)
        self.assertEqual(len(caught), 1)
        self.assertIn('order is reduced to 1', str(caught[0].message))
        self.assertEqual(result['pulse_shaping_chopper_1speed'], 14.0)

    def test_an_achievable_burst_says_nothing(self):
        import warnings
        from restage.bifrost_choppers import SLIT_CROSSING, calculate

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            calculate(14, SLIT_CROSSING / 14 * 0.999, 5.0, CHOPPERS)
        self.assertEqual(caught, [])


class ChopperConventionHintTestCase(unittest.TestCase):
    """The delay/phase rename, diagnosed rather than merely reported."""

    NAMES = tuple(f'{c}{k}' for c in CHOPPERS for k in ('speed', 'delay'))

    def test_an_instrument_still_named_for_phase_is_recognised(self):
        from restage.energy import chopper_convention_hint
        available = [n.replace('delay', 'phase') for n in self.NAMES]
        hint = chopper_convention_hint(available, self.NAMES)
        self.assertIsNotNone(hint)
        # One example named in full, and how many others share its problem -- six discs
        # would make the message a wall of names for no extra diagnosis.
        self.assertIn('bandwidth_chopper_1phase', hint)
        self.assertIn('and 5 more', hint)
        self.assertIn('DiskChopper', hint)

    def test_a_matching_instrument_needs_no_hint(self):
        from restage.energy import chopper_convention_hint
        self.assertIsNone(chopper_convention_hint(self.NAMES, self.NAMES))

    def test_an_unrelated_missing_parameter_is_not_blamed_on_the_rename(self):
        from restage.energy import chopper_convention_hint
        self.assertIsNone(chopper_convention_hint(['a', 'b'], ['a', 'b', 'sample_rotation']))

    def test_the_hint_reaches_the_error_a_scan_would_raise(self):
        """The primary stage hands the translated dict to a strict `collect_parameter_dict`,
        so this is the message a mismatched instrument actually produces."""
        from restage.instr import collect_parameter_dict
        from mccode_antlr.loader import parse_mcstas_instr
        declared = ', '.join(n.replace('delay', 'phase') for n in self.NAMES)
        instr = parse_mcstas_instr(f"""DEFINE INSTRUMENT old_style({declared})
        TRACE
        COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
        END
        """)
        with self.assertRaises(ValueError) as caught:
            collect_parameter_dict(instr, {n: 0.0 for n in self.NAMES})
        self.assertIn('Rename the instrument', str(caught.exception))


if __name__ == '__main__':
    unittest.main()
