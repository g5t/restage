"""
Utilities for interfacing with mccode_antlr.instr.Instr objects
"""
from pathlib import Path
from typing import Union
from mccode_antlr.instr import Instr
from mccode_antlr.reader import Registry


def special_load_mcstas_instr(filepath: Path, extra_registries: list[Registry] | None = None):
    from antlr4 import CommonTokenStream, InputStream
    from mccode_antlr.grammar import McInstrParser, McInstrLexer
    from mccode_antlr.instr import InstrVisitor
    from mccode_antlr.reader import Reader, MCSTAS_REGISTRY
    contents = filepath.read_text()
    parser = McInstrParser(CommonTokenStream(McInstrLexer(InputStream(contents))))
    registries = [MCSTAS_REGISTRY] if extra_registries is None else [MCSTAS_REGISTRY] + extra_registries
    reader = Reader(registries=registries)
    visitor = InstrVisitor(reader, str(filepath))
    instr = visitor.visitProg(parser.prog())
    instr.flags = tuple(reader.c_flags)
    instr.registries = tuple(registries)
    return instr


def load_instr(filepath: Union[str, Path], extra_registries: list[Registry] | None = None) -> Instr:
    """Loads an Instr object from a .instr file or a HDF5 file"""
    from mccode_antlr.io import load_hdf5

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
        return special_load_mcstas_instr(filepath, extra_registries=extra_registries)

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


