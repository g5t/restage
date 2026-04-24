"""SQLModel table definitions for the restage cache database.

These models replace the hand-crafted dataclasses that previously lived in
``tables.py`` and drove manual SQL string construction.  SQLModel (built on
SQLAlchemy + Pydantic v2) gives us:

* Typed, validated model fields
* Automatic table creation via ``SQLModel.metadata.create_all(engine)``
* Parameterised queries through the ORM (no SQL-injection risk)
* Easy schema inspection for migration support

The following models exist:

* :class:`InstrModel`         — one row per compiled instrument binary
* :class:`SimulationTableModel` — one row per instrument, records parameter names
* :class:`NexusStructureModel`  — one row per instrument NeXus structure
* :class:`SimulationModel`     — one row per cached simulation run

The legacy names ``InstrEntry``, ``SimulationTableEntry``, and
``NexusStructureEntry`` are re-exported from :mod:`restage.tables` as aliases
for backward compatibility.
"""
from __future__ import annotations

from typing import Optional, Any

from pydantic import field_validator
from sqlalchemy import Column, JSON
from sqlmodel import SQLModel, Field


# ---------------------------------------------------------------------------
# Utility functions (also re-exported via tables.py)
# ---------------------------------------------------------------------------

def uuid() -> str:
    from uuid import uuid4
    return str(uuid4()).replace('-', '')


def utc_timestamp() -> float:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).timestamp()


def str_hash(string: str) -> str:
    from hashlib import sha3_256
    return sha3_256(string.encode('utf-8')).hexdigest()


def instr_json_hash(instr) -> str:
    """Return a stable, process-independent sha3-256 hex digest of an ``Instr`` object.

    Uses ``mccode_antlr.io.json.to_json`` to serialise the instrument to bytes,
    then hashes those bytes.  Unlike Python's built-in ``hash(instr)``, this
    digest is identical across process invocations regardless of
    ``PYTHONHASHSEED``.
    """
    from hashlib import sha3_256
    from mccode_antlr.io.json import to_json
    return sha3_256(to_json(instr)).hexdigest()


def _default_mccode_version() -> str:
    from mccode_antlr import __version__
    return __version__


def _default_eniius_version() -> str:
    try:
        from eniius import __version__
        return __version__
    except ImportError:
        return 'unknown'


# ---------------------------------------------------------------------------
# SQLModel table models
# ---------------------------------------------------------------------------

class InstrModel(SQLModel, table=True):
    """One row per compiled instrument binary.

    The instrument is identified by ``instr_hash``, a sha3-256 digest of
    ``mccode_antlr.io.json.to_json(instr)`` — stable across Python processes.
    The full JSON serialisation lives at ``json_path`` on the filesystem
    (alongside the compiled binary) rather than in the database, to avoid
    bloating the DB with ~1.5 MB per instrument.
    """
    __tablename__ = 'instr_file'

    id: str = Field(default_factory=uuid, primary_key=True)
    instr_hash: str = Field(index=True)
    json_path: str
    mpi: bool
    acc: bool
    binary_path: str
    mccode_version: str = Field(default_factory=_default_mccode_version)
    creation: float = Field(default_factory=utc_timestamp)
    last_access: float = Field(default_factory=utc_timestamp)

    @field_validator('binary_path', 'json_path', mode='before')
    @classmethod
    def _coerce_path(cls, v):
        return str(v) if v is not None else v

    def model_post_init(self, __context: Any) -> None:
        # SQLModel table=True models may not run field_validators for some Pydantic versions;
        # coerce path-like fields to str here as a fallback.
        if not isinstance(self.binary_path, str):
            object.__setattr__(self, 'binary_path', str(self.binary_path))
        if not isinstance(self.json_path, str):
            object.__setattr__(self, 'json_path', str(self.json_path))

    @classmethod
    def from_instr(cls, instr, mpi: bool = False, acc: bool = False,
                   binary_path: str = '', json_path: str = '',
                   mccode_version: str = '') -> 'InstrModel':
        """Construct an :class:`InstrModel` from an ``mccode_antlr.instr.Instr`` object."""
        return cls(
            instr_hash=instr_json_hash(instr),
            json_path=json_path,
            mpi=mpi,
            acc=acc,
            binary_path=binary_path,
            mccode_version=mccode_version or _default_mccode_version(),
        )

    def load_instr(self):
        """Reconstruct the ``Instr`` object from the stored JSON file.

        Returns ``None`` if ``json_path`` does not exist (e.g. a shared
        read-only DB whose binary files live on another machine).
        """
        from pathlib import Path
        from zenlog import log
        p = Path(self.json_path)
        if not p.exists():
            log.warn(f'InstrModel.load_instr: json_path {p} does not exist; cannot reconstruct Instr')
            return None
        from mccode_antlr.io.json import load_json
        return load_json(p)


class SimulationTableModel(SQLModel, table=True):
    """One row per instrument, recording the parameter names for that instrument's simulations.

    The ``id`` is intentionally set to the same UUID as the parent
    :class:`InstrModel` (1-to-1 relationship), matching the convention
    established by the original ``pst_<id>`` dynamic table approach.
    ``name`` is kept as informational metadata (formerly the dynamic table name).
    """
    __tablename__ = 'simulation_tables'

    id: str = Field(default_factory=uuid, primary_key=True)
    name: str = ''
    parameters: Optional[list[str]] = Field(default=None, sa_column=Column(JSON))
    creation: float = Field(default_factory=utc_timestamp)
    last_access: float = Field(default_factory=utc_timestamp)

    def model_post_init(self, __context: Any) -> None:
        if not self.name:
            self.name = f'primary_instr_table_{self.id}'
        if ' ' in self.name:
            from zenlog import log
            log.warn(f'"{self.name}" is not a valid SQL table name, spaces replaced by underscores')
            self.name = self.name.replace(' ', '_')

    @property
    def table_name(self) -> str:
        """Compatibility shim — previously returned the dynamic SQL table name."""
        return self.name


class NexusStructureModel(SQLModel, table=True):
    """One row per instrument NeXus structure."""
    __tablename__ = 'nexus_structures'

    id: str = Field(primary_key=True)
    json_contents: str
    eniius_version: str = Field(default_factory=_default_eniius_version)


class SimulationModel(SQLModel, table=True):
    """One row per cached simulation run.

    Replaces the previous dynamic ``pst_<id>`` per-instrument tables.
    Parameter values are stored as a JSON dict ``{name: raw_value}`` where
    raw values are ``float | int | str``.  The business-logic
    :class:`~restage.tables.SimulationEntry` dataclass handles tolerance-based
    matching and ``mccode_antlr.common.Expr`` reconstruction.
    """
    __tablename__ = 'simulations'

    id: str = Field(default_factory=uuid, primary_key=True)
    table_id: str = Field(foreign_key='simulation_tables.id', index=True)
    parameter_values: Optional[dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    seed: Optional[int] = None
    ncount: Optional[int] = None
    output_path: str = ''
    gravitation: bool = False
    creation: float = Field(default_factory=utc_timestamp)
    last_access: float = Field(default_factory=utc_timestamp)
