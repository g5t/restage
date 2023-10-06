
class MRange:
    """A range of values for a parameter in a MATLAB style.
    The range is inclusive of the start and stop values, and the step is the difference between items in the range.
    """
    def __init__(self, start, stop, step):
        self.start = start
        self.stop = stop
        self.step = step

    def __iter__(self):
        def range_gen(start, stop, step):
            v = start
            i = 0
            while (step > 0 and v + step <= stop) or (step < 0 and v + step >= stop):
                v = i * step + start
                i += 1
                yield v
        return range_gen(self.start, self.stop, self.step)

    def __getitem__(self, index: int):
        if index < 0 or index >= len(self):
            raise IndexError(f'Index {index} out of range')
        return index * self.step + self.start

    def __str__(self):
        return f'{self.start}:{self.step}:{self.stop}'

    def __repr__(self):
        return f'MStyleRange({self})'

    def __len__(self):
        return int((self.stop - self.start) / self.step) + 1

    @classmethod
    def from_str(cls, string):
        """Parse a string in MATLAB style into a range.
        The string should be of the form start:step:stop
        """
        def float_or_int(s):
            try:
                return int(s)
            except ValueError:
                pass
            return float(s)

        if string.count(':') > 2:
            raise ValueError(f'Range string {string} contains more than two colons')
        step = '1'
        if ':' not in string:
            start, stop = string, string
        elif string.count(':') == 1:
            start, stop = string.split(':')
        else:
            start, step, stop = string.split(':')
        return cls(float_or_int(start), float_or_int(stop), float_or_int(step))


class Singular:
    """A singular range parameter for use with other range parameters in, e.g., a zip.

    Note:
        The Singular range value will be repeated up to `maximum` times in an iterator.
        If `maximum` is None, the Singular range will be repeated forever.
        Therefore, care must be taken to ensure that the Singular range is used in a zip with a range that is
        not infinite.
    """
    def __init__(self, value, maximum=None):
        self.value = value
        self.maximum = maximum

    def __iter__(self):
        def forever():
            while True:
                yield self.value

        def until():
            i = 0
            while i < self.maximum:
                i += 1
                yield self.value

        return until() if self.maximum is not None else forever()

    def __len__(self):
        return self.maximum
