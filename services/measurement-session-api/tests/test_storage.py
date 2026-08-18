"""Tests for manifest hashing and object metadata checks before streaming."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import unittest

from measurement_session_common.contract import DEFAULT_SESSION_BUCKET, MANIFEST_MEDIA_TYPE
from measurement_session_common.manifest import ArtifactDescriptor, canonical_manifest_bytes
from measurement_session_api.catalog import CatalogSession, ManifestReference
from measurement_session_api.storage import (
    IncompleteMeasurementDataError,
    ObjectStoreError,
    SeaweedSessionStore,
)


class _S3:
    def __init__(self, objects):
        self.objects = objects

    def head_object(self, Bucket: str, Key: str):
        item = self.objects[(Bucket, Key)]
        return {
            "ContentLength": len(item["body"]),
            "ContentType": item["content_type"],
            "Metadata": item["metadata"],
        }

    def get_object(self, Bucket: str, Key: str):
        return {"Body": BytesIO(self.objects[(Bucket, Key)]["body"])}


class SeaweedSessionStoreTests(unittest.TestCase):
    """Prove detail and download paths fail closed on integrity mismatch."""

    def test_validates_manifest_hash_and_artifact_metadata(self) -> None:
        session_id = "4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237"
        artifact_payload = _complete_waveform()
        artifact = ArtifactDescriptor(
            artifact_id="waveform",
            object_key=f"sessions/{session_id}/artifacts/waveform.csv",
            content_type="text/csv",
            size_bytes=len(artifact_payload),
            sha256_hex=sha256(artifact_payload).hexdigest(),
        )
        manifest_payload = canonical_manifest_bytes(session_id, (artifact,))
        manifest_key = f"sessions/{session_id}/manifest.json"
        session = _session(session_id, manifest_key, manifest_payload)
        store = SeaweedSessionStore(
            _S3(
                {
                    (DEFAULT_SESSION_BUCKET, manifest_key): {
                        "body": manifest_payload,
                        "content_type": MANIFEST_MEDIA_TYPE,
                        "metadata": {"sha256": sha256(manifest_payload).hexdigest()},
                    },
                    (DEFAULT_SESSION_BUCKET, artifact.object_key): {
                        "body": artifact_payload,
                        "content_type": "text/csv",
                        "metadata": {"sha256": artifact.sha256_hex},
                    },
                }
            )
        )

        descriptor, stream = store.artifact(session, "waveform")
        try:
            payload = stream.body.read()
        finally:
            stream.body.close()

        self.assertEqual(descriptor, artifact)
        self.assertEqual(payload, artifact_payload)

    def test_rejects_waveform_missing_declared_measurements(self) -> None:
        session_id = "4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237"
        artifact_payload = (
            b"timestamp,voltage_l1,voltage_l2,voltage_l3\n"
            b"2026-08-18T09:00:00Z,230.1,229.8,230.4\n"
            b"2026-08-18T09:00:05Z,230.0,229.9,230.2\n"
        )
        artifact = ArtifactDescriptor(
            artifact_id="waveform",
            object_key=f"sessions/{session_id}/artifacts/waveform.csv",
            content_type="text/csv",
            size_bytes=len(artifact_payload),
            sha256_hex=sha256(artifact_payload).hexdigest(),
        )
        manifest_payload = canonical_manifest_bytes(session_id, (artifact,))
        manifest_key = f"sessions/{session_id}/manifest.json"
        session = _session(session_id, manifest_key, manifest_payload)
        store = SeaweedSessionStore(
            _S3(
                {
                    (DEFAULT_SESSION_BUCKET, manifest_key): {
                        "body": manifest_payload,
                        "content_type": MANIFEST_MEDIA_TYPE,
                        "metadata": {"sha256": sha256(manifest_payload).hexdigest()},
                    },
                    (DEFAULT_SESSION_BUCKET, artifact.object_key): {
                        "body": artifact_payload,
                        "content_type": "text/csv",
                        "metadata": {"sha256": artifact.sha256_hex},
                    },
                }
            )
        )

        with self.assertRaisesRegex(IncompleteMeasurementDataError, "every finalized-session"):
            store.verified_manifest(session)

    def test_rejects_manifest_digest_mismatch(self) -> None:
        session_id = "4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237"
        payload = b"{}"
        manifest_key = f"sessions/{session_id}/manifest.json"
        session = _session(session_id, manifest_key, payload)
        store = SeaweedSessionStore(
            _S3(
                {
                    (DEFAULT_SESSION_BUCKET, manifest_key): {
                        "body": payload,
                        "content_type": MANIFEST_MEDIA_TYPE,
                        "metadata": {"sha256": sha256(payload).hexdigest()},
                    }
                }
            )
        )
        session = CatalogSession(
            **{**session.__dict__, "manifest": ManifestReference(
                bucket=session.manifest.bucket,
                object_key=session.manifest.object_key,
                byte_length=session.manifest.byte_length,
                media_type=session.manifest.media_type,
                sha256=b"x" * 32,
            )}
        )

        with self.assertRaisesRegex(ObjectStoreError, "metadata"):
            store.verified_manifest(session)


def _session(session_id: str, manifest_key: str, payload: bytes) -> CatalogSession:
    return CatalogSession(
        session_id=session_id,
        source_mrid="urn:wama:poc:pmu:bay-01",
        started_at=datetime(2026, 8, 18, 9, 0, tzinfo=timezone.utc),
        ended_at=datetime(2026, 8, 18, 9, 1, tzinfo=timezone.utc),
        finalized_at=datetime(2026, 8, 18, 9, 2, tzinfo=timezone.utc),
        metadata=(("asset", "bay-01"),),
        measurement_count=3,
        artifact_count=1,
        manifest=ManifestReference(
            bucket=DEFAULT_SESSION_BUCKET,
            object_key=manifest_key,
            byte_length=len(payload),
            media_type=MANIFEST_MEDIA_TYPE,
            sha256=sha256(payload).digest(),
        ),
        contract_sha256=b"y" * 32,
    )


def _complete_waveform() -> bytes:
    return (
        b"timestamp,voltage_l1,voltage_l2,voltage_l3\n"
        b"2026-08-18T09:00:00Z,230.1,229.8,230.4\n"
        b"2026-08-18T09:00:30Z,230.05,229.85,230.3\n"
        b"2026-08-18T09:01:00Z,230.0,229.9,230.2\n"
    )