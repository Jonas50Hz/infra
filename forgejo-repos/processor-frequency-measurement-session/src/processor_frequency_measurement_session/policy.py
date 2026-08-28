"""EE-editable frequency capture policy for approved WAMA PoC PMU sources."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from types import MappingProxyType


SERVICE_NAME = "processor-frequency-measurement-session"
REQUEST_ORIGIN = SERVICE_NAME
CAPTURE_REASON = "frequency_gt_50_2_hz"
FREQUENCY_THRESHOLD_HZ = 50.2
WINDOW_PADDING = timedelta(seconds=10)
PROCESSING_DELAY_SECONDS = 10.0
MAX_SESSION_DURATION = timedelta(hours=24)


@dataclass(frozen=True)
class FrequencyCapturePolicy:
    """One reviewed frequency MRID and its complete bounded session MRID set."""

    frequency_mrid: str
    capture_mrids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.frequency_mrid:
            raise ValueError("frequency_mrid must not be empty")
        if len(self.capture_mrids) != 8:
            raise ValueError("capture_mrids must contain exactly eight MRIDs")
        if tuple(sorted(set(self.capture_mrids))) != self.capture_mrids:
            raise ValueError("capture_mrids must be sorted and unique")
        if self.frequency_mrid not in self.capture_mrids:
            raise ValueError("capture_mrids must include frequency_mrid")


CAPTURE_POLICIES = (
    FrequencyCapturePolicy(
        frequency_mrid="urn:wama:poc:pmu:bay-01:frequency",
        capture_mrids=(
            "urn:wama:poc:pmu:bay-01:current-l1",
            "urn:wama:poc:pmu:bay-01:current-l2",
            "urn:wama:poc:pmu:bay-01:current-l3",
            "urn:wama:poc:pmu:bay-01:frequency",
            "urn:wama:poc:pmu:bay-01:rocof",
            "urn:wama:poc:pmu:bay-01:voltage-l1",
            "urn:wama:poc:pmu:bay-01:voltage-l2",
            "urn:wama:poc:pmu:bay-01:voltage-l3",
        ),
    ),
    FrequencyCapturePolicy(
        frequency_mrid="urn:wama:poc:pmu:bay-02:frequency",
        capture_mrids=(
            "urn:wama:poc:pmu:bay-02:current-l1",
            "urn:wama:poc:pmu:bay-02:current-l2",
            "urn:wama:poc:pmu:bay-02:current-l3",
            "urn:wama:poc:pmu:bay-02:frequency",
            "urn:wama:poc:pmu:bay-02:rocof",
            "urn:wama:poc:pmu:bay-02:voltage-l1",
            "urn:wama:poc:pmu:bay-02:voltage-l2",
            "urn:wama:poc:pmu:bay-02:voltage-l3",
        ),
    ),
    FrequencyCapturePolicy(
        frequency_mrid="urn:wama:poc:pmu:bay-03:frequency",
        capture_mrids=(
            "urn:wama:poc:pmu:bay-03:current-l1",
            "urn:wama:poc:pmu:bay-03:current-l2",
            "urn:wama:poc:pmu:bay-03:current-l3",
            "urn:wama:poc:pmu:bay-03:frequency",
            "urn:wama:poc:pmu:bay-03:rocof",
            "urn:wama:poc:pmu:bay-03:voltage-l1",
            "urn:wama:poc:pmu:bay-03:voltage-l2",
            "urn:wama:poc:pmu:bay-03:voltage-l3",
        ),
    ),
    FrequencyCapturePolicy(
        frequency_mrid="urn:wama:poc:pmu:bay-04:frequency",
        capture_mrids=(
            "urn:wama:poc:pmu:bay-04:current-l1",
            "urn:wama:poc:pmu:bay-04:current-l2",
            "urn:wama:poc:pmu:bay-04:current-l3",
            "urn:wama:poc:pmu:bay-04:frequency",
            "urn:wama:poc:pmu:bay-04:rocof",
            "urn:wama:poc:pmu:bay-04:voltage-l1",
            "urn:wama:poc:pmu:bay-04:voltage-l2",
            "urn:wama:poc:pmu:bay-04:voltage-l3",
        ),
    ),
    FrequencyCapturePolicy(
        frequency_mrid="urn:wama:poc:pmu:bay-05:frequency",
        capture_mrids=(
            "urn:wama:poc:pmu:bay-05:current-l1",
            "urn:wama:poc:pmu:bay-05:current-l2",
            "urn:wama:poc:pmu:bay-05:current-l3",
            "urn:wama:poc:pmu:bay-05:frequency",
            "urn:wama:poc:pmu:bay-05:rocof",
            "urn:wama:poc:pmu:bay-05:voltage-l1",
            "urn:wama:poc:pmu:bay-05:voltage-l2",
            "urn:wama:poc:pmu:bay-05:voltage-l3",
        ),
    ),
)

POLICIES_BY_FREQUENCY_MRID: Mapping[str, FrequencyCapturePolicy] = MappingProxyType(
    {policy.frequency_mrid: policy for policy in CAPTURE_POLICIES}
)

if len(POLICIES_BY_FREQUENCY_MRID) != len(CAPTURE_POLICIES):
    raise ValueError("frequency_mrid entries must be unique")


def policy_for(frequency_mrid: str) -> FrequencyCapturePolicy | None:
    """Return the explicitly reviewed policy for one frequency MRID."""

    return POLICIES_BY_FREQUENCY_MRID.get(frequency_mrid)