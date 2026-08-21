"""Current active gateway state folded from compacted Masterdata records."""

from __future__ import annotations

from gateway_dashboard_provisioner.model import GatewaySource


class GatewayRegistry:
    """Maintain the latest active source for every compacted source key."""

    def __init__(self) -> None:
        self._sources: dict[str, GatewaySource] = {}

    @property
    def sources(self) -> tuple[GatewaySource, ...]:
        """Return active sources in deterministic source-ID order."""

        return tuple(self._sources[source_id] for source_id in sorted(self._sources))

    def upsert(self, source: GatewaySource) -> bool:
        """Replace one source and report whether the active snapshot changed."""

        previous = self._sources.get(source.source_id)
        if previous == source:
            return False
        self._sources[source.source_id] = source
        return True

    def remove(self, source_id: str) -> bool:
        """Apply a compacted-topic tombstone and report whether it changed state."""

        if source_id not in self._sources:
            return False
        del self._sources[source_id]
        return True