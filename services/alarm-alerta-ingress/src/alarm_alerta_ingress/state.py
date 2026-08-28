"""In-memory desired-state registry rebuilt from compacted Alarm records."""

from __future__ import annotations

from alarm_alerta_ingress.model import DesiredAlarm


class AlarmRegistry:
    """Maintain the latest active desired state for every canonical Alarm key."""

    def __init__(self) -> None:
        self._alarms: dict[str, DesiredAlarm] = {}

    @property
    def alarms(self) -> tuple[DesiredAlarm, ...]:
        """Return active desired state in deterministic key order."""

        return tuple(self._alarms[key] for key in sorted(self._alarms))

    def get(self, alarm_key: str) -> DesiredAlarm | None:
        """Return the current desired state for one key, if active."""

        return self._alarms.get(alarm_key)

    def remove(self, alarm_key: str) -> bool:
        """Apply one same-key tombstone and report whether state changed."""

        if alarm_key not in self._alarms:
            return False
        del self._alarms[alarm_key]
        return True

    def upsert(self, alarm: DesiredAlarm) -> bool:
        """Apply one active desired state and report whether state changed."""

        previous = self._alarms.get(alarm.identity.alarm_key)
        if previous == alarm:
            return False
        self._alarms[alarm.identity.alarm_key] = alarm
        return True