
def parse_run():
    from .range import MStyleRange
    from argparse import ArgumentParser
    parser = ArgumentParser('mcbifrost_run')
    parser.add_argument('instrument', nargs=1, type=str, default=None)
    parser.add_argument('-N', '--numpoints', nargs=1, type=int, default=None)
    parser.add_argument('--ei', nargs=1, type=MStyleRange.from_str, default=None)


def run():
    pass
