from __future__ import annotations

from dataclasses import dataclass, field
from mccode_antlr.common import Expr

# Re-export SQLModel table models and utility functions so existing imports continue to work.
from .models import (
    uuid, utc_timestamp, str_hash, instr_json_hash,
    InstrModel, SimulationTableModel, NexusStructureModel, SimulationModel,
)

# Backward-compatible type aliases: these names now point to SQLModel table models.
InstrEntry = InstrModel
SimulationTableEntry = SimulationTableModel
NexusStructureEntry = NexusStructureModel

COMMON_COLUMNS = ['seed', 'ncount', 'output_path', 'gravitation', 'creation', 'last_access']


@dataclass
class SimulationEntry:
    """Business-logic dataclass representing one cached simulation run.

    This is *not* a SQLModel table model — persistence is handled via
    :class:`~restage.models.SimulationModel`.  Use :meth:`to_model` /
    :meth:`from_model` to convert, and :meth:`matches_candidate` for the
    Python-side tolerance check that replaces the old SQL ``BETWEEN`` query.
    """
    parameter_values: dict[str, Expr]
    seed: int | None = None
    ncount: int | None = None
    output_path: str = field(default_factory=str)
    gravitation: bool = False
    precision: dict[str, float] = field(default_factory=dict)
    id: str = field(default_factory=uuid)
    creation: float = field(default_factory=utc_timestamp)
    last_access: float = field(default_factory=utc_timestamp)

    def __post_init__(self):
        from zenlog import log
        for k, v in self.parameter_values.items():
            if not isinstance(v, Expr):
                self.parameter_values[k] = Expr.best(v)

        for k, v in self.parameter_values.items():
            if v.is_float and k not in self.precision:
                # Find the best matching precision, e.g., k='ps1speed' would select 'speed' from ('speed', 'phase', ...)
                best = [p for p in self.precision.keys() if p in k]
                if len(best) > 1:
                    log.info(f"SimulationEntry.__post_init__:: Multiple precision matches for {k}: {best}")
                if len(best):
                    # This abs is probably overkill, but it's worth protecting against a user-specified negative value
                    self.precision[k] = abs(self.precision[best[0]])
                elif self.parameter_values[k].has_value:
                    # This abs is *crucial* since a negative parameter value would have a negative precision otherwise
                    self.precision[k] = abs(self.parameter_values[k].value / 10000)
                else:
                    log.info(f'SimulationEntry.__post_init__:: No precision match for value-less {k}, using 0.1;'
                             ' consider specifying precision dict during initialization')
                    self.precision[k] = 0.1

    def __hash__(self):
        return hash((tuple(self.parameter_values.values()), self.seed, self.ncount, self.gravitation))

    def matches_candidate(self, candidate: 'SimulationEntry') -> bool:
        """Return ``True`` if *candidate* (a stored simulation) falls within this entry's tolerances.

        ``self`` is the search query; ``candidate`` is what was retrieved from the DB.
        Replaces the old SQL ``BETWEEN`` query with Python-side filtering so that
        parameter values can remain in a normalised JSON column.
        """
        if set(self.parameter_values.keys()) != set(candidate.parameter_values.keys()):
            return False
        for k, v in self.parameter_values.items():
            cv = candidate.parameter_values[k]
            if v.is_float:
                if abs(cv.value - v.value) > self.precision[k]:
                    return False
            elif cv.value != v.value:
                return False
        if self.seed is not None and candidate.seed != self.seed:
            return False
        if self.ncount is not None and candidate.ncount != self.ncount:
            return False
        if self.gravitation and not candidate.gravitation:
            return False
        return True

    def to_model(self, table_id: str) -> SimulationModel:
        """Serialise to a :class:`~restage.models.SimulationModel` for DB persistence.

        Parameter values are stored as ``{name: expr.to_dict()}`` so that all
        ``Expr`` type information (float / int / string, sympy repr) survives the
        JSON round-trip.
        """
        return SimulationModel(
            id=self.id,
            table_id=table_id,
            parameter_values={k: v.to_dict() for k, v in self.parameter_values.items()},
            seed=self.seed,
            ncount=self.ncount,
            output_path=self.output_path,
            gravitation=self.gravitation,
            creation=self.creation,
            last_access=self.last_access,
        )

    @classmethod
    def from_model(cls, model: SimulationModel, param_names: list[str]) -> 'SimulationEntry':
        """Reconstruct a :class:`SimulationEntry` from a persisted :class:`~restage.models.SimulationModel`.

        *param_names* is the ordered list of parameter names stored in the parent
        :class:`~restage.models.SimulationTableModel`.  Each value is restored via
        ``Expr.from_dict`` so that type information (float/int/string) is preserved.
        """
        pv_raw = model.parameter_values or {}
        parameter_values = {k: Expr.from_dict(pv_raw[k]) for k in param_names if k in pv_raw}
        return cls(
            parameter_values=parameter_values,
            seed=model.seed,
            ncount=model.ncount,
            output_path=model.output_path or '',
            gravitation=bool(model.gravitation),
            id=model.id,
            creation=model.creation,
            last_access=model.last_access,
        )

    def parameter_distance(self, other: 'SimulationEntry') -> float:
        if not isinstance(other, SimulationEntry):
            raise RuntimeError(f"Cannot compare {self} to {other}")
        if self.parameter_values.keys() != other.parameter_values.keys():
            raise RuntimeError(f"Cannot compare {self} to {other}")
        total = 0
        for k in self.parameter_values.keys():
            if self.parameter_values[k].is_float or self.parameter_values[k].is_int:
                total += abs(self.parameter_values[k].value - other.parameter_values[k].value)
            elif self.parameter_values[k] != other.parameter_values[k]:
                total += 10 * len(self.parameter_values)
        return total


def best_simulation_entry_match_index(candidates: list[SimulationEntry], pivot: SimulationEntry) -> int:
    # There are many reasons a query could have returned multiple matches.
    #   there could be multiple points repeated within the uncertainty we used to select the primary simulation
    #   the same simulation could be repeated with different seed values, and we haven't specified a seed here
    #   the same simulation could be repeated with different particle counts, as no ncount was specified here
    #   the same simulation could be repeated with and without gravity, and that flag is not specified here?
    # select the best one:
    #   1. check for difference between simulated and requested parameters and sort by that
    #   2. if any are the same, pick the one with the most particles (maybe we can sample from it if fewer are needed?)
    #   ... come up with some heuristic for picking the best seed?
    if len(candidates) < 2:
        return 0
    # sort the candidate indexes by parameter-distance from the pivot
    distances = [c.parameter_distance(pivot) for c in candidates]
    indexes = sorted(range(len(candidates)), key=lambda index: distances[index])
    # if there are multiple candidates with the same distance, pick the one with the most particles
    last_distance = distances[indexes[0]]
    best = 0
    for i in indexes[1:]:
        if distances[i] != last_distance:
            break
        if (candidates[i].ncount or 0) > (candidates[best].ncount or 0):
            best = i
    return indexes[best]


def best_simulation_entry_match(candidates: list[SimulationEntry], pivot: SimulationEntry) -> SimulationEntry:
    return candidates[best_simulation_entry_match_index(candidates, pivot)]
