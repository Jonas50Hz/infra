"""Developer-owned transformations for one WAMA stream processor."""

from __future__ import annotations

from processor_template.generated.rtd_schema_pb2 import MCCSMeasurementValue


def transform(measurement: MCCSMeasurementValue) -> MCCSMeasurementValue | None:
    """Return a derived measurement, or None while this template is uncustomized."""

    del measurement
    return None