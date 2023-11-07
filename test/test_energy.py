import unittest

class BIFROSTEnergyTestCase(unittest.TestCase):
    def setUp(self):
        from mccode_antlr.loader import parse_mcstas_instr
        instr = f"""DEFINE INSTRUMENT this_IS_NOT_BIFROST(
        ps1speed, ps1phase, ps2speed, ps2phase, fo1speed, fo1phase, bw1speed, bw1phase, bw2speed, bw2phase
        )
        TRACE
        COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
        COMPONENT ps1 = DiskChopper(theta_0=170, radius=0.35, nu=ps1speed, phase=ps1phase) AT (0, 0, 1) RELATIVE PREVIOUS
        COMPONENT ps2 = DiskChopper(theta_0=170, radius=0.35, nu=ps2speed, phase=ps2phase) AT (0, 0, 0.02) RELATIVE ps1
        COMPONENT fo1 = DiskChopper(theta_0=110, radius=0.35, nu=fo1speed, phase=fo1phase) AT (0, 0, 12) RELATIVE ps2
        COMPONENT fo2 = DiskChopper(theta_0=115, radius=0.35, nu=fo1speed, phase=fo1phase) AT (0, 0, 4) RELATIVE fo1
        COMPONENT bw1 = DiskChopper(theta_0=110, radius=0.35, nu=bw1speed, phase=bw1phase) AT (0, 0, 80) RELATIVE fo2
        COMPONENT bw2 = DiskChopper(theta_0=115, radius=0.35, nu=bw2speed, phase=bw2phase) AT (0, 0, 0.02) RELATIVE bw1
        COMPONENT sample = Arm() AT (0, 0, 80) RELATIVE bw2
        END
        """
        self.instr = parse_mcstas_instr(instr)

    def test_names(self):
        from restage.energy import get_energy_parameter_names
        energy_names = get_energy_parameter_names(self.instr.name)
        for name in ('e', 'energy', 'ei', 'wavelength', 'lambda', 'time', 't', 'order'):
            self.assertTrue(name in energy_names)

    def test_parameters_to_scan(self):
        from restage.range import MRange, Singular, parameters_to_scan
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

        parameters = dict(order=order, time=time, ei=ei)
        npts, names, points = parameters_to_scan(parameters, grid=True)
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
        from restage.range import MRange, Singular, parameters_to_scan
        from itertools import product

        translator = energy_to_chopper_translator(self.instr.name)
        self.assertEqual(translator, bifrost_translate_energy_to_chopper_parameters)

        order = Singular(14,  1)
        time = MRange(0.0001, 0.002248, 0.0002)
        ei = MRange(1.7, 24.7, 0.5)
        parameters = dict(order=order, time=time, ei=ei)

        spts, names, points = parameters_to_scan(parameters, grid=True)

        self.assertEqual(47*11, spts)
        self.assertEqual(names, ['order', 'time', 'ei'])

        chopper_pars = [x + y for x, y in product(('ps1', 'ps2', 'fo1', 'fo2', 'bw1', 'bw2'), ('speed', 'phase'))]

        for point in points:
            kv = {k: v for k, v in zip(names, point)}
            translated = translator(kv)
            for x in chopper_pars:
                self.assertTrue(''.join(x) in translated)

            self.assertEqual(len(translated), len(chopper_pars))
            self.assertAlmostEqual(translated['bw1speed'], 14.0)
            self.assertAlmostEqual(translated['bw2speed'], -14.0)
            self.assertAlmostEqual(translated['fo1speed'], 14.0)
            self.assertAlmostEqual(translated['fo1speed'], 14.0)
            self.assertAlmostEqual(translated['ps1speed'], 14*14.0)
            self.assertAlmostEqual(translated['ps2speed'], 14*14.0)

    def test_calculations(self):
        from itertools import product
        from chopcal import bifrost as mcstas_bifrost_calculation
        from restage.energy import bifrost_translate_energy_to_chopper_parameters

        pars = [x+y for x, y in product(('ps1', 'ps2', 'fo1', 'fo2', 'bw1', 'bw2'), ('speed', 'phase'))]

        shortest_time = 0.0001  # this is approximately twice the opening time of the pulse shaping choppers at 15*14 Hz
        # Normal operation  Shortest full-height pulse  Shorter pulses reduce height
        #      /-----\                  /\
        # ----/       \---  -----------/  \------------ -------------/\--------------

        order = 14  # the McStas calculations are for 14th order *only* -- though they can be reduced to lower orders

        # the longest time has both disks (nearly) in phase [in phase if no distance between them]
        # but we reduce that here to ensure the McStas calculation does not reduce the order
        longest_time = (170 / 360) / order / 14 - shortest_time

        smallest_energy = 0.75  # ~4 full source periods to reach the sample, and more than 1 meV energy gain
        largest_energy = 25.  # a guess, but depends on the source spectra

        n_time, n_energy = 100, 100
        d_time, d_energy = (longest_time - shortest_time) / n_time, (largest_energy - smallest_energy) / n_energy
        for time_index, energy_index in product(range(n_time), range(n_energy)):
            time = shortest_time + time_index * d_time
            energy = smallest_energy + energy_index * d_energy

            kv = {'order': order, 'time': time, 'ei': energy}
            translated = bifrost_translate_energy_to_chopper_parameters(kv)
            from_mcstas = mcstas_bifrost_calculation(energy, 0., time)
            for x in pars:
                self.assertAlmostEqual(from_mcstas[x], translated[x])


if __name__ == '__main__':
    unittest.main()
