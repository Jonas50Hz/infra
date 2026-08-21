"""Electrical calculations authored for the WAMA apparent-power processor."""


def apparent_power_l1(voltage_l1: float, current_l1: float) -> float:
    """Calculate phase-one apparent power in volt-amperes."""

    return voltage_l1 * current_l1


def apparent_power_l2(voltage_l2: float, current_l2: float) -> float:
    """Calculate phase-two apparent power in volt-amperes."""

    return voltage_l2 * current_l2


def apparent_power_l3(voltage_l3: float, current_l3: float) -> float:
    """Calculate phase-three apparent power in volt-amperes."""

    return voltage_l3 * current_l3