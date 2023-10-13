"""
Utilities for interfacing with mccode.instr.Instr objects
"""
from mccode.instr import Instr


def collect_parameter_dict(instr: Instr, kwargs: dict) -> dict:
    """
    Collects the parameters from an Instr object, and updates any parameters specified in kwargs
    :param instr: Instr object
    :param kwargs: dict of parameters set by the user in, e.g., a scan
    :return: dict of parameters from instr and kwargs
    """
    from mccode.common.expression import Value
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
            raise ValueError(f"Parameter {k} is not a valid parameter name")
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
