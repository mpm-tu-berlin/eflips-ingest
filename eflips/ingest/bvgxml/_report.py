"""
What one import kept, estimated and dropped.

The previous implementation scattered this across interleaved warnings, so the only way to
find out whether an import had lost half its rotations was to read the whole log. Every
decision that discards or invents data now lands here, and :meth:`IngestReport.summary`
prints it once at the end.
"""
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Iterable, List

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters for type checking
    from eflips.ingest.bvgxml._read import PreparedInput
    from eflips.ingest.bvgxml._rotations import RawRotation
    from eflips.ingest.bvgxml._routes import ResolvedRoute, RouteTable
    from eflips.ingest.bvgxml._schedule import Trip


@dataclass
class IngestReport:
    """Counters and examples describing one import."""

    # What was written
    n_rotations: int = 0
    n_trips: int = 0
    n_routes: int = 0
    n_fahrten_total: int = 0

    # Where data was invented. The ``input_routes`` counts are per Route element of the
    # input, of which several routinely resolve to one written route.
    n_estimated_distance: int = 0
    n_input_routes_reconstructed: int = 0
    n_reconstruction_failed: int = 0
    n_input_routes_zero_duration: int = 0

    # Which input files never made it as far as being interpreted
    n_files_read: int = 0
    n_files_empty: int = 0
    files_invalid: Dict[str, str] = field(default_factory=dict)

    # What was dropped
    n_degenerate_routes: int = 0
    n_trips_on_degenerate_routes: int = 0
    n_truncated_rotations: int = 0
    n_empty_rotations: int = 0
    truncation_reasons: Dict[str, int] = field(default_factory=dict)
    truncated_examples: List[str] = field(default_factory=list)

    # Invariant violations — expected to stay at zero. Counted separately from the
    # examples, which are capped: reporting ``len(examples)`` would say "5" however many
    # there really are, and the whole point of these two is to notice when they are not 0.
    n_discontinuities: int = 0
    n_overlaps: int = 0
    discontinuities: List[str] = field(default_factory=list)
    overlaps: List[str] = field(default_factory=list)

    #: How many examples of each kind to keep for the log.
    example_limit: int = 5

    def absorb_routes(self, table: "RouteTable") -> None:
        """
        Take the counters that are naturally per *input* route.

        Several input routes routinely share one output route — the export repeats a route
        once per file the line appears in — so these counts are larger than anything a query
        against the written rows would return. :meth:`absorb_written_routes` supplies the
        one figure the summary invites the reader to look up.
        """
        self.n_input_routes_reconstructed = table.n_reconstructed
        self.n_reconstruction_failed = table.n_reconstruction_failed
        self.n_degenerate_routes = table.n_degenerate
        self.n_input_routes_zero_duration = table.n_zero_duration

    def absorb_written_routes(self, routes: "Iterable[ResolvedRoute]") -> None:
        """Count the routes that are actually written, so the summary matches the database."""
        self.n_estimated_distance = sum(1 for route in routes if route.distance_estimated)

    def absorb_prepared_input(self, prepared: "PreparedInput") -> None:
        self.n_files_read = len(prepared.files)
        self.n_files_empty = len(prepared.skipped_empty)
        self.files_invalid = dict(prepared.skipped_invalid)

    def note_truncated_rotation(self, rotation: "RawRotation", reason: str) -> None:
        self.n_truncated_rotations += 1
        self.truncation_reasons[reason] = self.truncation_reasons.get(reason, 0) + 1
        if len(self.truncated_examples) < self.example_limit:
            self.truncated_examples.append(f"{rotation.name} ({reason})")

    def note_empty_rotation(self) -> None:
        self.n_empty_rotations += 1

    def note_discontinuity(self, rotation: "RawRotation", current: "Trip", following: "Trip") -> None:
        self.n_discontinuities += 1
        if len(self.discontinuities) < self.example_limit:
            self.discontinuities.append(
                f"{rotation.name}: trip {current.fahrt_id} ends at a different station than "
                f"trip {following.fahrt_id} starts at"
            )

    def note_overlap(self, rotation: "RawRotation", current: "Trip", following: "Trip") -> None:
        self.n_overlaps += 1
        if len(self.overlaps) < self.example_limit:
            self.overlaps.append(
                f"{rotation.name}: trip {current.fahrt_id} arrives at "
                f"{current.arrival.isoformat()}, after trip {following.fahrt_id} departs "
                f"at {following.departure.isoformat()}"
            )

    @property
    def n_dropped_trips(self) -> int:
        return max(0, self.n_fahrten_total - self.n_trips)

    def summary(self) -> str:
        """A multi-line summary suitable for a single log record."""
        lines = [
            f"BVG-XML ingest: {self.n_rotations} rotations, {self.n_trips} trips, " f"{self.n_routes} routes.",
        ]

        if self.n_files_empty:
            lines.append(
                f"  {self.n_files_empty} input files carry no timetable for their line and "
                f"day and were skipped; {self.n_files_read} were read."
            )
        if self.files_invalid:
            lines.append(
                f"  WARNING: {len(self.files_invalid)} input files could not be read and "
                f"were skipped. Any vehicle rotation reaching into them is counted as "
                f"incomplete below. Examples:"
            )
            for name in sorted(self.files_invalid)[: self.example_limit]:
                lines.append(f"    {name}: {self.files_invalid[name]}")

        if self.n_dropped_trips:
            lines.append(
                f"  {self.n_dropped_trips} of {self.n_fahrten_total} trips in the input " f"were not imported."
            )
        if self.n_truncated_rotations:
            lines.append(f"  {self.n_truncated_rotations} vehicle rotations were dropped as " f"incomplete:")
            for reason, count in sorted(self.truncation_reasons.items(), key=lambda kv: -kv[1]):
                lines.append(f"    {count:6d} × {reason}")
        if self.n_empty_rotations:
            lines.append(f"  {self.n_empty_rotations} vehicle rotations contained no usable trips.")
        if self.n_degenerate_routes:
            lines.append(
                f"  {self.n_degenerate_routes} routes never leave one station and were "
                f"dropped, with {self.n_trips_on_degenerate_routes} trips on them."
            )

        if self.n_estimated_distance:
            lines.append(
                f"  {self.n_estimated_distance} of the written routes have an estimated "
                f"distance (their names start with 'CHECK DISTANCE: ')."
            )
        if self.n_input_routes_zero_duration:
            lines.append(
                f"  {self.n_input_routes_zero_duration} input routes carry no driving time "
                f"in the export; their duration was derived from their length."
            )
        if self.n_input_routes_reconstructed or self.n_reconstruction_failed:
            lines.append(
                f"  {self.n_input_routes_reconstructed} input routes had omitted stops "
                f"restored from the Streckennetz; {self.n_reconstruction_failed} could not "
                f"be restored."
            )

        if self.n_discontinuities:
            lines.append(
                f"  WARNING: {self.n_discontinuities} pairs of consecutive trips do not "
                f"meet at the same station. This should not happen; please report it. "
                f"Examples:"
            )
            lines.extend(f"    {example}" for example in self.discontinuities)
        if self.n_overlaps:
            lines.append(
                f"  WARNING: {self.n_overlaps} pairs of consecutive trips overlap in time. "
                f"This should not happen; please report it. Examples:"
            )
            lines.extend(f"    {example}" for example in self.overlaps)

        return "\n".join(lines)
