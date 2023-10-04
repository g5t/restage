
class MStyleRange:
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

    def __str__(self):
        return f'{self.start}:{self.step}:{self.stop}'

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

