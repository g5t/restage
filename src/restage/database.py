from __future__ import annotations

import sqlite3
from pathlib import Path

from sqlmodel import SQLModel, Session, select, create_engine

from .models import (
    InstrModel, SimulationTableModel, NexusStructureModel, SimulationModel,
    utc_timestamp,
)
from .tables import SimulationEntry, InstrEntry, SimulationTableEntry, NexusStructureEntry


class Database:
    """SQLModel-backed cache database.

    Wraps a single SQLite file.  The public method signatures are deliberately
    kept identical to the previous ``sqlite3``-based implementation so that
    :mod:`restage.cache` and existing tests require minimal changes.

    Schema is managed by SQLModel/SQLAlchemy via
    ``SQLModel.metadata.create_all``.  On open, tables whose column list does
    not match the current model definition are dropped and recreated (writable
    databases only; read-only databases raise ``ValueError`` on mismatch).
    """

    def __init__(self, db_file: Path,
                 instr_file_table: str | None = None,
                 nexus_structures_table: str | None = None,
                 simulations_table: str | None = None,
                 readonly: bool = False):
        from os import access, W_OK
        self.db_file = db_file
        self.readonly = readonly or not access(db_file.parent, W_OK)
        # Table name attributes kept for API compat; values are fixed by the models.
        self.instr_file_table = 'instr_file'
        self.nexus_structures_table = 'nexus_structures'
        self.simulations_table = 'simulation_tables'
        self.verbose = False

        if self.readonly:
            def _ro_creator():
                return sqlite3.connect(f'file:{db_file}?mode=ro', uri=True)
            self.engine = create_engine('sqlite+pysqlite://', creator=_ro_creator)
        else:
            self.engine = create_engine(f'sqlite:///{db_file}')
            SQLModel.metadata.create_all(self.engine)

        self._validate_schema(db_file)

    def _validate_schema(self, db_file: Path) -> None:
        """Drop/recreate tables whose columns no longer match the current models."""
        from sqlalchemy import inspect as sa_inspect
        from zenlog import log

        inspector = sa_inspect(self.engine)
        existing_tables = set(inspector.get_table_names())

        expected: dict[str, type[SQLModel]] = {
            'instr_file': InstrModel,
            'nexus_structures': NexusStructureModel,
            'simulation_tables': SimulationTableModel,
            'simulations': SimulationModel,
        }

        needs_recreate: list[str] = []
        for table_name, model_cls in expected.items():
            expected_cols = list(model_cls.__table__.c.keys())
            if table_name in existing_tables:
                actual_cols = [c['name'] for c in inspector.get_columns(table_name)]
                if actual_cols != expected_cols:
                    if not self.readonly:
                        log.warn(
                            f'Table {table_name} in {db_file} has columns {actual_cols} '
                            f'but expected {expected_cols}; dropping and recreating.'
                        )
                        needs_recreate.append(table_name)
                    else:
                        raise ValueError(
                            f'Table {table_name} in readonly database {db_file} has outdated schema '
                            f'(columns {actual_cols}, expected {expected_cols})'
                        )
            elif self.readonly:
                raise ValueError(f'Table {table_name} does not exist in readonly database {db_file}')

        if needs_recreate:
            with self.engine.begin() as conn:
                for table_name in needs_recreate:
                    conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{table_name}"')
            SQLModel.metadata.create_all(self.engine)

    def _session(self) -> Session:
        return Session(self.engine, expire_on_commit=False)

    def announce(self, msg: str) -> None:
        if self.verbose:
            print(msg)

    # ------------------------------------------------------------------
    # Table existence helpers
    # ------------------------------------------------------------------

    def table_exists(self, table_name: str) -> bool:
        from sqlalchemy import inspect as sa_inspect
        return table_name in sa_inspect(self.engine).get_table_names()

    def check_table_exists(self, table_name: str) -> None:
        if not self.table_exists(table_name):
            raise RuntimeError(f"Table {table_name} does not exist")

    # ------------------------------------------------------------------
    # InstrModel (InstrEntry)
    # ------------------------------------------------------------------

    def insert_instr_file(self, instr_file: InstrEntry) -> None:
        if self.readonly:
            raise ValueError('Cannot insert into readonly database')
        with self._session() as session:
            session.add(instr_file)
            session.commit()

    def retrieve_instr_file(self, instr_id: str) -> list[InstrEntry]:
        with self._session() as session:
            return list(session.exec(select(InstrModel).where(InstrModel.id == instr_id)).all())

    def query_instr_file(self, search: dict) -> list[InstrEntry]:
        with self._session() as session:
            stmt = select(InstrModel)
            for k, v in search.items():
                stmt = stmt.where(getattr(InstrModel, k) == v)
            return list(session.exec(stmt).all())

    def all_instr_files(self) -> list[InstrEntry]:
        with self._session() as session:
            return list(session.exec(select(InstrModel)).all())

    def delete_instr_file(self, instr_id: str) -> None:
        if self.readonly:
            raise ValueError('Cannot delete from readonly database')
        with self._session() as session:
            obj = session.get(InstrModel, instr_id)
            if obj is not None:
                session.delete(obj)
                session.commit()

    # ------------------------------------------------------------------
    # NexusStructureModel (NexusStructureEntry)
    # ------------------------------------------------------------------

    def insert_nexus_structure(self, nexus_structure: NexusStructureEntry) -> None:
        if self.readonly:
            raise ValueError('Cannot insert into readonly database')
        with self._session() as session:
            session.add(nexus_structure)
            session.commit()

    def retrieve_nexus_structure(self, id: str) -> list[NexusStructureEntry]:
        with self._session() as session:
            return list(session.exec(select(NexusStructureModel).where(NexusStructureModel.id == id)).all())

    # ------------------------------------------------------------------
    # SimulationTableModel (SimulationTableEntry)
    # ------------------------------------------------------------------

    def insert_simulation_table(self, entry: SimulationTableEntry) -> None:
        if self.readonly:
            raise ValueError('Cannot insert into readonly database')
        with self._session() as session:
            session.add(entry)
            session.commit()

    def retrieve_simulation_table(self, primary_id: str,
                                   update_access_time: bool = True) -> list[SimulationTableEntry]:
        with self._session() as session:
            results = list(session.exec(
                select(SimulationTableModel).where(SimulationTableModel.id == primary_id)
            ).all())
            if not self.readonly and update_access_time and results:
                now = utc_timestamp()
                for r in results:
                    r.last_access = now
                session.commit()
            return results

    def retrieve_all_simulation_tables(self) -> list[SimulationTableEntry]:
        with self._session() as session:
            return list(session.exec(select(SimulationTableModel)).all())

    def delete_simulation_table(self, primary_id: str) -> None:
        if self.readonly:
            raise ValueError('Cannot delete from readonly database')
        with self._session() as session:
            # Delete all simulations for this table first
            sims = session.exec(
                select(SimulationModel).where(SimulationModel.table_id == primary_id)
            ).all()
            for s in sims:
                session.delete(s)
            table = session.get(SimulationTableModel, primary_id)
            if table is not None:
                session.delete(table)
            session.commit()

    def query_simulation_table(self, entry: SimulationTableEntry,
                                use_id: bool = False,
                                use_name: bool = True,
                                use_parameters: bool = True) -> list[SimulationTableEntry]:
        """Query the simulation-tables table.

        The filter priority mirrors the original implementation: ``use_id`` takes
        precedence over ``use_name``, which takes precedence over ``use_parameters``.
        For the ``use_parameters`` case, filtering is done in Python since JSON
        column equality comparisons are not reliably portable across SQLite versions.
        """
        with self._session() as session:
            if use_id:
                return list(session.exec(
                    select(SimulationTableModel).where(SimulationTableModel.id == entry.id)
                ).all())
            elif use_name:
                return list(session.exec(
                    select(SimulationTableModel).where(SimulationTableModel.name == entry.name)
                ).all())
            elif use_parameters:
                all_entries = session.exec(select(SimulationTableModel)).all()
                return [e for e in all_entries if e.parameters == entry.parameters]
            else:
                raise RuntimeError("At least one of use_id, use_name, or use_parameters must be True")

    # ------------------------------------------------------------------
    # SimulationModel (SimulationEntry)
    # ------------------------------------------------------------------

    def insert_simulation(self, simulation: SimulationTableEntry, parameters: SimulationEntry) -> None:
        if self.readonly:
            raise ValueError('Cannot insert into readonly database')
        if not self.retrieve_simulation_table(simulation.id, update_access_time=False):
            # The `simulation` object may have been loaded from a different database session
            # (e.g. a read-only DB). Re-using that detached object in a new session would
            # not trigger an INSERT because SQLAlchemy sees the primary key is already set.
            # Create a fresh transient copy so the ORM always emits INSERT here.
            local_table = SimulationTableModel(
                id=simulation.id,
                name=simulation.name,
                parameters=list(simulation.parameters or []),
            )
            self.insert_simulation_table(local_table)
        with self._session() as session:
            session.add(parameters.to_model(simulation.id))
            session.commit()

    def retrieve_simulation(self, primary_id: str, pars: SimulationEntry) -> list[SimulationEntry]:
        """Retrieve all simulations for *primary_id* that match *pars* within tolerance.

        All rows for the given ``table_id`` are loaded and filtered in Python via
        :meth:`~restage.tables.SimulationEntry.matches_candidate`.  For typical cache
        sizes (hundreds to low thousands of rows per table) this is fast; if scale
        becomes a concern, the JSON ``parameter_values`` column could be indexed or the
        query could be moved to a dedicated table.
        """
        matches = self.retrieve_simulation_table(primary_id)
        if len(matches) != 1:
            raise RuntimeError(f"Expected exactly one match for id={primary_id}, got {matches}")
        param_names = matches[0].parameters or []
        with self._session() as session:
            sim_models = session.exec(
                select(SimulationModel).where(SimulationModel.table_id == primary_id)
            ).all()
            candidates = [SimulationEntry.from_model(s, param_names) for s in sim_models]
            results = [c for c in candidates if pars.matches_candidate(c)]
            if not self.readonly and results:
                now = utc_timestamp()
                ids = {r.id for r in results}
                for s in sim_models:
                    if s.id in ids:
                        s.last_access = now
                session.commit()
            return results

    def delete_simulation(self, primary_id: str, simulation_id: str) -> None:
        if self.readonly:
            raise ValueError('Cannot delete from readonly database')
        with self._session() as session:
            obj = session.get(SimulationModel, simulation_id)
            if obj is not None:
                session.delete(obj)
                session.commit()

    def retrieve_all_simulations(self, primary_id: str) -> list[SimulationEntry]:
        matches = self.retrieve_simulation_table(primary_id)
        if len(matches) != 1:
            raise RuntimeError(f"Expected exactly one match for id={primary_id}, got {matches}")
        param_names = matches[0].parameters or []
        with self._session() as session:
            sim_models = session.exec(
                select(SimulationModel).where(SimulationModel.table_id == primary_id)
            ).all()
            return [SimulationEntry.from_model(s, param_names) for s in sim_models]

    # ------------------------------------------------------------------
    # Schema inspection helpers (kept for API compatibility)
    # ------------------------------------------------------------------

    def retrieve_column_names(self, table_name: str) -> list[str]:
        """Return the column names for a table.

        For the ``simulations`` table the returned names include the parameter names
        stored in the corresponding :class:`~restage.models.SimulationTableModel` row.
        For other tables the SQLAlchemy inspector is used.
        """
        self.check_table_exists(table_name)
        from sqlalchemy import inspect as sa_inspect
        return [c['name'] for c in sa_inspect(self.engine).get_columns(table_name)]

    def table_has_columns(self, table_name: str, columns: list[str]) -> bool:
        self.check_table_exists(table_name)
        actual = self.retrieve_column_names(table_name)
        return actual[:len(columns)] == columns

