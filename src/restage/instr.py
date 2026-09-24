"""
Utilities for interfacing with mccode_antlr.instr.Instr objects
"""
from __future__ import annotations

from pathlib import Path
from typing import Union
from mccode_antlr.instr import Instr


def load_instr(filepath: Union[str, Path]) -> Instr:
    """Loads an Instr object from a .instr file or a HDF5 file"""
    if not isinstance(filepath, Path):
        filepath = Path(filepath)
    if not filepath.exists() or not filepath.is_file():
        raise ValueError(f'The provided {filepath=} does not exist or is not a file')

    if filepath.suffix == '.instr':
        from mccode_antlr.loader import load_mcstas_instr
        instr = load_mcstas_instr(filepath)
        _warn_about_search(filepath)
        return instr
    elif filepath.suffix.lower() == '.json':
        from mccode_antlr.io.json import load_json
        return load_json(filepath)

    from mccode_antlr.io import load_hdf5
    return load_hdf5(filepath)


#: What to do instead of relying on SEARCH, said wherever restage finds it relied on.
SEARCH_ADVICE = ('Save the instrument as JSON with explicit registries -- add a '
                 'mccode_antlr.reader.LocalRegistry for each searched directory to '
                 'instr.registries, then mccode_antlr.io.json.save_json -- and run that instead.')


def search_statements(filepath: Union[str, Path]) -> list[str]:
    """The SEARCH statements of an instrument file, outside its comments."""
    import re
    text = Path(filepath).read_text()
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.S)
    text = re.sub(r'//[^\n]*', '', text)
    return [line.strip() for line in text.splitlines() if re.match(r'\s*SEARCH\b', line)]


def _warn_about_search(filepath: Path) -> None:
    """Say, as the file is read, that restage cannot use what SEARCH found.

    mccode-antlr resolves components through SEARCH while it reads a file but keeps no
    record of it on the instrument -- deliberately in part: a SEARCH SHELL line runs a
    command, which a file handed to a simulation service should not get to do, and is
    not portable anyway. restage compiles from the instrument it was given, the split
    halves included, so anything found only that way cannot be found again.
    """
    from zenlog import log
    found = search_statements(filepath)
    if found:
        log.warning(f'{filepath} finds components through {len(found)} SEARCH statement(s), '
                    f'e.g. {found[0]!r}. mccode-antlr does not keep SEARCH paths on the '
                    f'instrument, so restage cannot find those components again when it '
                    f'compiles. {SEARCH_ADVICE}')


def collect_parameter_dict(instr: Instr, kwargs: dict, strict: bool = True) -> dict:
    """
    Collects the parameters from an Instr object, and updates any parameters specified in kwargs
    :param instr: Instr object
    :param kwargs: dict of parameters set by the user in, e.g., a scan
    :param strict: if True, raises an error if a parameter is specified in kwargs that is not in instr
    :return: dict of parameters from instr and kwargs
    """
    from mccode_antlr.common.expression import Expr
    parameters = {p.name: p.value for p in instr.parameters}
    for k, v in parameters.items():
        if not isinstance(v, Expr):
            raise ValueError(f"Parameter {k} is not a valid parameter name")
        if not v.is_singular:
            raise ValueError(f"Parameter {k} is not singular, and cannot be set")
        if v.is_op:
            raise ValueError(f"Parameter {k} is an operation, and cannot be set")
        parameters[k] = v

    for k, v in kwargs.items():
        if k not in parameters:
            if strict:
                from .energy import chopper_convention_hint
                message = (f"Parameter {k} is not a valid parameter name. "
                           f"Valid names are: {', '.join(parameters)}")
                hint = chopper_convention_hint(parameters, kwargs)
                raise ValueError(f'{message}\n{hint}' if hint else message)
            continue
        if not isinstance(v, Expr):
            expected_type = parameters[k].data_type
            v = Expr(v, expected_type)
        parameters[k] = v

    return parameters


def collect_parameter(instr: Instr, **kwargs) -> dict:
    """
    Collects the parameters from an Instr object, and updates any parameters specified in kwargs
    :param instr: Instr object
    :param kwargs: parameters set by the user in, e.g., a scan
    :return: dict of parameters from instr and kwargs
    """
    return collect_parameter_dict(instr, kwargs)


