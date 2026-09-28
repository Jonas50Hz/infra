"""Container entry point for one approved Masterdata reconciliation."""

from __future__ import annotations

import sys

from gateway_c37_118_onboarding.publisher import PublisherError, Settings, reconcile


def main() -> int:
    """Reconcile the checked-out Git catalog with Kafka's compacted state."""

    try:
        plan = reconcile(Settings.from_environment())
    except PublisherError as error:
        print(f"Masterdata publisher failed: {error}", file=sys.stderr)
        return 1
    print(
        "Masterdata reconciliation completed: "
        f"{len(plan.upserts)} upsert(s), {len(plan.tombstones)} tombstone(s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())