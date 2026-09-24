"""Collector files across restage's two stages: merged per primary, assembled per scan.

The files are written here with h5py in libreadout's layout, so the assembly -- which is
restage's own code -- is tested without the C++ library. The steps that use the library's
append and concatenate, and the check that an assembled file is one the library accepts,
are skipped when `mcstas_readout` cannot be loaded.
"""
from __future__ import annotations

import unittest
from functools import lru_cache
from pathlib import Path
from tempfile import TemporaryDirectory

try:
    import h5py
    import numpy as np
except ImportError:  # pragma: no cover - exercised only without the optional extra
    h5py = None

#: libreadout's canonical BM0 record: ring, FEN, time, weight, channel, C-aligned.
BM0 = {'names': ['ring', 'FEN', 'time', 'weight', 'channel'],
       'formats': ['u1', 'u1', '<f8', '<f8', 'u1'],
       'offsets': [0, 1, 8, 16, 24], 'itemsize': 32}
BM0_DESCRIPTION = 'uint8_t ring; uint8_t FEN; double time; double weight; uint8_t channel;'


def write_collector(path: Path, group: str, times, parameters: dict, normalization=100,
                    detector='DetectorType::CBM0'):
    """A single-point collector file holding one BM0 group with records at `times`."""
    dtype = np.dtype(BM0)
    with h5py.File(path, 'w') as file:
        for key, value in library_identity().items():
            file.attrs[key] = value
        g = file.create_group(group)
        g.attrs['detector'] = detector
        g.attrs['type'] = 'Readouts'
        records = np.zeros(len(times), dtype)
        records['ring'] = 22
        records['time'] = times
        records['weight'] = 1.0
        readouts = g.create_dataset('readouts', data=records)
        readouts.attrs['description'] = BM0_DESCRIPTION
        g.create_dataset('cues', data=np.array([len(times)], np.uint32))
        g.create_dataset('weights', data=np.array([float(len(times))]))
        g.create_dataset('normalizations', data=np.array([normalization], np.uint64))
        p = file.create_group('parameters')
        p.attrs['type'] = 'Parameters'
        for name, value in parameters.items():
            p.create_dataset(name, data=np.array([value]))
    return path


@lru_cache(maxsize=None)
def library_identity() -> dict:
    """The root attributes this libreadout stamps on the files it writes.

    It refuses to combine files stamped by another build of itself -- version *and*
    revision -- so synthetic inputs have to carry its own. There is no accessor for the
    revision, so a throwaway file is written through the C API and read back.
    """
    readout = library()
    if readout is None:
        return {'program': 'libreadout', 'version': '0.6.0', 'revision': 'test'}
    import ctypes
    lib = readout._lib.load()
    lib.collector_new.restype = ctypes.c_void_p
    lib.collector_new.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int, ctypes.c_uint64]
    lib.collector_free.argtypes = [ctypes.c_void_p]
    with TemporaryDirectory() as tmp:
        probe = Path(tmp) / 'probe.h5'
        lib.collector_free(lib.collector_new(str(probe).encode(), b'probe', 0xf0, 1))
        with h5py.File(probe, 'r') as file:
            return {k: file.attrs[k] for k in ('program', 'version', 'revision')}


def library():
    """mcstas_readout with a loadable libreadout, or None."""
    try:
        import mcstas_readout
        from mcstas_readout import _lib
        _lib.load()
    except Exception:
        return None
    return mcstas_readout


@unittest.skipIf(h5py is None, 'h5py is not installed')
class AssemblePrimaryGroupsTest(unittest.TestCase):
    """The primaries' groups: stored once per primary, viewed once per point.

    Four points served by two primaries, A A B A -- the shape of a scan whose primary
    parameters change once and change back. No secondary collectors, so this is restage's
    code alone.
    """

    def setUp(self):
        self._tmp = TemporaryDirectory()
        root = Path(self._tmp.name)
        self.a, self.b = root / 'primary_a', root / 'primary_b'
        for d in (self.a, self.b):
            d.mkdir()
        write_collector(self.a / 'run.h5', 'monitor', [0.1, 0.2, 0.3], {'speed': 14.0}, 100)
        write_collector(self.b / 'run.h5', 'monitor', [0.5, 0.6], {'speed': 28.0}, 50)
        self.points = []
        for i, primary in enumerate((self.a, self.a, self.b, self.a)):
            point = root / 'scan' / str(i)
            point.mkdir(parents=True)
            self.points.append((point, primary))
        self.out = root / 'scan'

    def tearDown(self):
        self._tmp.cleanup()

    def assemble(self):
        from restage.collectors import assemble_collector_scan
        return assemble_collector_scan(self.points, self.out)

    def test_each_primary_is_stored_once(self):
        (path,) = self.assemble()
        with h5py.File(path, 'r') as file:
            group = file['monitor']
            self.assertEqual(group['stored_readouts'].shape, (5,))
            self.assertTrue(group['readouts'].is_virtual)
            self.assertEqual(group['readouts'].shape, (11,))

    def test_each_point_sees_its_own_primary(self):
        (path,) = self.assemble()
        with h5py.File(path, 'r') as file:
            group = file['monitor']
            np.testing.assert_array_equal(group['cues'][...], [3, 6, 8, 11])
            times = group['readouts']['time']
            np.testing.assert_allclose(times, [.1, .2, .3, .1, .2, .3, .5, .6, .1, .2, .3])
            np.testing.assert_array_equal(group['normalizations'][...], [100, 100, 50, 100])
            np.testing.assert_allclose(group['weights'][...], [3, 3, 2, 3])

    def test_record_layout_and_identity_are_kept(self):
        """Replay decides sendability by exact datatype, and routing by group attributes."""
        (path,) = self.assemble()
        with h5py.File(path, 'r') as file:
            group = file['monitor']
            self.assertEqual(group['readouts'].dtype, np.dtype(BM0))
            self.assertEqual(group['readouts'].attrs['description'], BM0_DESCRIPTION)
            self.assertEqual(group.attrs['detector'], 'DetectorType::CBM0')
            self.assertEqual(file.attrs['program'], 'libreadout')

    def test_primary_parameters_are_given_per_point(self):
        """Replay publishes a point's parameters; the discs' settings are the primary's."""
        (path,) = self.assemble()
        with h5py.File(path, 'r') as file:
            np.testing.assert_allclose(file['parameters/speed'][...], [14, 14, 28, 14])

    def test_the_stored_blocks_say_where_they_came_from(self):
        (path,) = self.assemble()
        with h5py.File(path, 'r') as file:
            stored = file['monitor/stored_readouts']
            self.assertEqual(list(stored.attrs['sources']), [str(self.a), str(self.b)])
            np.testing.assert_array_equal(stored.attrs['starts'], [0, 3])

    def test_an_existing_file_is_not_overwritten(self):
        self.out.joinpath('run.h5').touch()
        from restage.collectors import assemble_collector_scan
        with self.assertRaises(RuntimeError):
            assemble_collector_scan(self.points, self.out)

    def test_the_library_accepts_the_assembled_file(self):
        readout = library()
        if readout is None:
            self.skipTest('mcstas_readout is not available')
        (path,) = self.assemble()
        self.assertEqual(readout.validate_collector_file(path), 4)


@unittest.skipIf(h5py is None, 'h5py is not installed')
class AssembleWithSecondaryTest(unittest.TestCase):
    """Secondary files concatenated by the library, primary groups added alongside."""

    def setUp(self):
        self.readout = library()
        if self.readout is None:
            self.skipTest('mcstas_readout is not available')
        self._tmp = TemporaryDirectory()
        root = Path(self._tmp.name)
        primary = root / 'primary'
        primary.mkdir()
        write_collector(primary / 'run.h5', 'monitor', [0.1, 0.2], {'speed': 14.0}, 100)
        self.points = []
        for i, a3 in enumerate((0.0, 10.0, 20.0)):
            point = root / 'scan' / str(i)
            point.mkdir(parents=True)
            write_collector(point / 'run.h5', 'detector', [0.3] * (i + 1), {'a3': a3}, 10)
            self.points.append((point, primary))
        self.out = root / 'scan'

    def tearDown(self):
        self._tmp.cleanup()

    def test_one_valid_file_with_both_stages_in_every_point(self):
        from restage.collectors import assemble_collector_scan
        (path,) = assemble_collector_scan(self.points, self.out)
        self.assertEqual(self.readout.validate_collector_file(path), 3)
        with h5py.File(path, 'r') as file:
            np.testing.assert_array_equal(file['detector/cues'][...], [1, 3, 6])
            np.testing.assert_array_equal(file['monitor/cues'][...], [2, 4, 6])
            self.assertEqual(file['monitor/stored_readouts'].shape, (2,))
            np.testing.assert_allclose(file['parameters/a3'][...], [0, 10, 20])
            np.testing.assert_allclose(file['parameters/speed'][...], [14, 14, 14])

    def test_the_split_filename_is_not_a_point_parameter(self):
        """Where restage kept the intermediate rays is not something replay should publish."""
        from restage.collectors import assemble_collector_scan
        for point, _ in self.points:
            with h5py.File(point / 'run.h5', 'a') as file:
                file['parameters'].create_dataset('mcpl_filename', data=np.array([b'/cache/x.mcpl']))
        (path,) = assemble_collector_scan(self.points, self.out)
        self.assertEqual(self.readout.validate_collector_file(path), 3)
        with h5py.File(path, 'r') as file:
            self.assertNotIn('mcpl_filename', file['parameters'])
            self.assertIn('a3', file['parameters'])

    def test_a_point_without_its_file_is_an_error(self):
        from restage.collectors import assemble_collector_scan
        self.points[1][0].joinpath('run.h5').unlink()
        with self.assertRaises(RuntimeError):
            assemble_collector_scan(self.points, self.out)


@unittest.skipIf(h5py is None, 'h5py is not installed')
class MergePassesTest(unittest.TestCase):
    """The primary's repeated passes, appended by the library into its cache directory."""

    def setUp(self):
        self.readout = library()
        if self.readout is None:
            self.skipTest('mcstas_readout is not available')
        self._tmp = TemporaryDirectory()
        self.work = Path(self._tmp.name)
        self.passes = []
        for i, n in enumerate((3, 5)):
            d = self.work / str(i)
            d.mkdir()
            write_collector(d / 'run.h5', 'monitor', [0.1] * n, {'speed': 14.0}, 1000 * (i + 1))
            self.passes.append(d)

    def tearDown(self):
        self._tmp.cleanup()

    def test_records_and_normalizations_are_summed(self):
        from restage.collectors import merge_collector_passes
        (path,) = merge_collector_passes(self.passes, self.work)
        self.assertEqual(path, self.work / 'run.h5')
        with h5py.File(path, 'r') as file:
            self.assertEqual(file['monitor/readouts'].shape, (8,))
            np.testing.assert_array_equal(file['monitor/normalizations'][...], [3000])

    def test_a_single_pass_is_copied(self):
        from restage.collectors import merge_collector_passes
        (path,) = merge_collector_passes(self.passes[:1], self.work)
        with h5py.File(path, 'r') as file:
            self.assertEqual(file['monitor/readouts'].shape, (3,))


class WithoutTheExtraTest(unittest.TestCase):
    """Without h5py or mcstas_readout the scan still finishes, and says what it skipped."""

    def test_missing_modules_warn_and_do_nothing(self):
        from unittest.mock import patch
        from zenlog import log
        from restage import collectors
        warnings = []
        with TemporaryDirectory() as tmp:
            point = Path(tmp)
            point.joinpath('run.h5').touch()
            with (patch.object(collectors, '_h5py', lambda: None),
                  patch.object(collectors, '_readout', lambda: None),
                  patch.object(log, 'warning', warnings.append)):
                self.assertEqual(collectors.assemble_collector_scan([(point, point)], point), [])
                self.assertEqual(collectors.merge_collector_passes([point], point), [])
        self.assertEqual(len(warnings), 2)
        self.assertIn('h5py and mcstas_readout', warnings[0])

    def test_no_hdf5_files_means_no_warning(self):
        from unittest.mock import patch
        from zenlog import log
        from restage import collectors
        warnings = []
        with TemporaryDirectory() as tmp:
            with (patch.object(collectors, '_h5py', lambda: None),
                  patch.object(log, 'warning', warnings.append)):
                collectors.assemble_collector_scan([(Path(tmp), Path(tmp))], Path(tmp))
        self.assertEqual(warnings, [])


if __name__ == '__main__':
    unittest.main()
