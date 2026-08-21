"""Electrical calculation authored for the WAMA frequency-scale processor."""

HERTZ_TO_MILLIHERTZ = 1_000.0


def frequency_millihertz(frequency_hz: float) -> float:
    """Convert the declared PMU frequency from hertz to millihertz."""

    return frequency_hz * HERTZ_TO_MILLIHERTZ