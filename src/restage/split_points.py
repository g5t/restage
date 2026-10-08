"""Where to split an instrument for a scan, out of the places its author allows.

Splitting pays when scan points share primaries: one primary simulation, written to MCPL,
serves every point whose primary parameters match it. A primary depends on every
instrument parameter a component up to the split uses, and on every parameter the
instrument-level DECLARE, INITIALIZE, SAVE and FINAL blocks mention, because both halves
of a split receive those blocks whole.

Which places are safe is the author's knowledge, not something read off the instrument:
an MCPL file carries a ray's state but none of its USERVARS, so a split where a user
variable set upstream is still read downstream silently loses it, and nothing short of
reading the C can tell. The author therefore names candidate split points; restage only
picks among them. Where an MCPL component sits does not matter -- neither MCPL_output nor
MCPL_input propagates a ray, so the split point only sets the frame both halves share.

Given candidates ordered along the beam, a later one never needs fewer primaries than an
earlier one, and needs no more secondary work. So the choice is the latest candidate whose
primaries are still shared -- fewer distinct primaries than scan points. If even the
earliest is not shared, the scan is better run unsplit.
"""
from __future__ import annotations

#: The instrument-level blocks both halves of a split receive in full.
INSTRUMENT_BLOCKS = ('declare', 'initialize', 'save', 'final')


class SplitError(RuntimeError):
    """An instrument that cannot be split the way it was asked to be."""


def candidate_names(split_at) -> list[str]:
    """``--split-at`` as a list of component names: one, or several separated by commas."""
    if isinstance(split_at, str):
        split_at = split_at.split(',')
    return [name.strip() for name in split_at if name.strip()]


def block_parameters(instr) -> set[str]:
    """Instrument parameters an instrument-level block mentions."""
    blocks = [block for section in INSTRUMENT_BLOCKS for block in getattr(instr, section)]
    return {p.name for p in instr.parameters if any(p.name in block for block in blocks)}


def primary_parameters(instr, index: int, blocks: bool = True) -> set[str]:
    """The parameters the primary of a split after component ``index`` depends on.

    The same test `Instr.split` uses to decide which parameters each half keeps; with
    ``blocks=False``, only what the components themselves use.
    """
    prefix = instr.components[:index + 1]
    used = {p.name for p in instr.parameters if any(c.parameter_used(p.name) for c in prefix)}
    return used | block_parameters(instr) if blocks else used


def _check_boundary(instr, index: int) -> None:
    """Refuse a candidate inside something that only works whole.

    A GROUP's members are alternatives offered in turn, and Union geometries and processes
    belong to the master that follows them; neither survives being cut in two.
    """
    components = instr.components
    name = components[index].name
    before, after = components[:index], components[index + 1:]
    if before and after and before[-1].group and before[-1].group == after[0].group:
        raise SplitError(f'Split point {name} is inside GROUP {before[-1].group}')
    union_before = any(c.type.name.startswith('Union_') and c.type.name != 'Union_master'
                       for c in before)
    if union_before and any(c.type.name == 'Union_master' for c in after):
        raise SplitError(f'Split point {name} separates Union components from their master')


def _primaries(points: list[dict], names: set[str]) -> int:
    """How many distinct primaries ``points`` need, if each depends on ``names``."""
    ordered = sorted(names)
    return len({tuple(point.get(n) for n in ordered) for point in points})


def choose_split(instr, candidates, points: list[dict], extra: set[str] = frozenset(),
                 allow_block_parameters: bool = False) -> str | None:
    """The candidate to split ``instr`` at for the scan ``points``, or None to run unsplit.

    ``points`` holds each scan point's parameter values, translated as the simulations
    will receive them. ``extra`` names parameters restage gives the primary regardless of
    use -- the energy parameters it turns into chopper settings.

    A single point (or none) is not a scan, and the split there is for restage's cache to
    reuse the primary in a later run; the latest candidate is taken.

    Raises SplitError when the instrument-level blocks mention a scanned parameter the
    chosen primary would otherwise not depend on: both halves receive those blocks, so the
    mention multiplies the primaries the scan needs -- the instrument should be fixed.
    ``allow_block_parameters`` accepts the cost instead, and the choice is made with the
    blocks' dependencies counted.
    """
    index = {c.name: i for i, c in enumerate(instr.components)}
    names = candidate_names(candidates)
    missing = [name for name in names if name not in index]
    if missing:
        raise SplitError(f'No split-point component named {", ".join(missing)} in {instr.name}')
    ordered = sorted(set(names), key=index.get)
    for name in ordered:
        _check_boundary(instr, index[name])
    points = points or [{}]

    def count(name, blocks):
        return _primaries(points, primary_parameters(instr, index[name], blocks) | set(extra))

    def latest(blocks):
        shared = [n for n in ordered if len(points) <= 1 or count(n, blocks) < len(points)]
        return shared[-1] if shared else None

    ideal = latest(blocks=False)
    if ideal is None:
        return None
    if count(ideal, True) > count(ideal, False) and not allow_block_parameters:
        varied = sorted(n for n in block_parameters(instr) - primary_parameters(instr, index[ideal], False)
                        if _primaries(points, {n}) > 1)
        raise SplitError(
            f'Splitting {instr.name} at {ideal} would share primaries between scan points, but '
            f'its instrument-level DECLARE/INITIALIZE/SAVE/FINAL mention {", ".join(varied)}, '
            f'which the scan varies. Both halves of a split receive those blocks, so every '
            f'value of them needs its own primary. Move the use into the components that need '
            f'it, or accept the cost with --allow-block-parameters.')
    return latest(blocks=True)
