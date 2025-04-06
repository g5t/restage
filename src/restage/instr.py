"""
Utilities for interfacing with mccode_antlr.instr.Instr objects
"""
from __future__ import annotations

from pathlib import Path
from typing import Union
from mccode_antlr.instr import Instr
from mccode_antlr.reader import Registry


def load_instr(filepath: Union[str, Path], extra_registries: list[Registry] | None = None) -> Instr:
    """Loads an Instr object from a .instr file or a HDF5 file"""
    from mccode_antlr.io import load_hdf5
    from mccode_antlr.loader import load_mcstas_instr

    if not isinstance(filepath, Path):
        filepath = Path(filepath)
    if not filepath.exists() or not filepath.is_file():
        raise ValueError('The provided filepath does not exist or is not a file')

    # FIXME this hack should be removed ASAP
    if extra_registries is None:
        from mccode_antlr.reader import GitHubRegistry
        mcpl_input_once_registry = GitHubRegistry(
            name='mcpl_input_once',
            url='https://github.com/g5t/mccode-mcpl-input-once',
            version='main',
            filename='pooch-registry.txt'
        )
        extra_registries = [mcpl_input_once_registry]

    if filepath.suffix == '.instr':
        return load_mcstas_instr(filepath, registries=extra_registries)

    instr = load_hdf5(filepath)
    instr.registries += tuple(extra_registries)
    return instr


def collect_parameter_dict(instr: Instr, kwargs: dict, strict: bool = True) -> dict:
    """
    Collects the parameters from an Instr object, and updates any parameters specified in kwargs
    :param instr: Instr object
    :param kwargs: dict of parameters set by the user in, e.g., a scan
    :param strict: if True, raises an error if a parameter is specified in kwargs that is not in instr
    :return: dict of parameters from instr and kwargs
    """
    from mccode_antlr.common.expression import Value
    parameters = {p.name: p.value for p in instr.parameters}
    for k, v in parameters.items():
        if not v.is_singular:
            raise ValueError(f"Parameter {k} is not singular, and cannot be set")
        if v.is_op:
            raise ValueError(f"Parameter {k} is an operation, and cannot be set")
        if not isinstance(v.first, Value):
            raise ValueError(f"Parameter {k} is not a valid parameter name")
        parameters[k] = v.first

    for k, v in kwargs.items():
        if k not in parameters:
            if strict:
                raise ValueError(f"Parameter {k} is not a valid parameter name")
            continue
        if not isinstance(v, Value):
            expected_type = parameters[k].data_type
            v = Value(v, expected_type)
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


