"""Small approved formula used only by the registry lifecycle verifier."""


def frequency_millihertz(frequency_hz: float) -> float:
    """Convert the reviewed frequency source to millihertz."""

    return frequency_hz * 1_000.0