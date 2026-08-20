"""Immutable SeaweedFS operations for session artifacts and receipts."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any, BinaryIO

from botocore.exceptions import BotoCoreError, ClientError

from measurement_session_common.contract import (
    BLOBMETA_MEDIA_TYPE,
    MAX_BLOBMETA_SERIALIZED_BYTES,
)


class ObjectStoreError(RuntimeError):
    """Raised when the object store cannot prove an immutable object is intact."""


class SeaweedSessionStore:
    """Write once or verify exact prior bytes for all session evidence."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def read_receipt(self, bucket: str, object_key: str) -> bytes | None:
        """Read and integrity-check one small Blobmeta receipt when it exists."""

        try:
            head = self._client.head_object(Bucket=bucket, Key=object_key)
        except ClientError as error:
            if _is_missing(error):
                return None
            raise ObjectStoreError("Unable to read Blobmeta receipt metadata") from error
        except (BotoCoreError, OSError) as error:
            raise ObjectStoreError("Unable to read Blobmeta receipt metadata") from error
        if head.get("ContentType") != BLOBMETA_MEDIA_TYPE:
            raise ObjectStoreError("Blobmeta receipt has an unexpected media type")
        if head.get("ContentLength", 0) > MAX_BLOBMETA_SERIALIZED_BYTES:
            raise ObjectStoreError("Blobmeta receipt exceeds its supported size")
        try:
            response = self._client.get_object(Bucket=bucket, Key=object_key)
            body: BinaryIO = response["Body"]
            try:
                payload = body.read(MAX_BLOBMETA_SERIALIZED_BYTES + 1)
            finally:
                body.close()
        except (BotoCoreError, ClientError, OSError) as error:
            raise ObjectStoreError("Unable to read Blobmeta receipt") from error
        if len(payload) > MAX_BLOBMETA_SERIALIZED_BYTES:
            raise ObjectStoreError("Blobmeta receipt exceeds its supported size")
        digest = sha256(payload).hexdigest()
        if head.get("ContentLength") != len(payload) or head.get("Metadata", {}).get("sha256") != digest:
            raise ObjectStoreError("Blobmeta receipt metadata does not match its bytes")
        return payload

    def put_or_verify_file(
        self,
        bucket: str,
        object_key: str,
        path: Path,
        content_type: str,
        digest: bytes,
        size_bytes: int,
    ) -> None:
        """Write an artifact once, or prove a replay has identical bytes."""

        if self._exists(bucket, object_key):
            self.verify_object(bucket, object_key, content_type, digest, size_bytes)
            return
        try:
            with path.open("rb") as body:
                self._client.put_object(
                    Bucket=bucket,
                    Key=object_key,
                    Body=body,
                    ContentType=content_type,
                    Metadata={"sha256": digest.hex()},
                )
        except (BotoCoreError, ClientError, OSError) as error:
            raise ObjectStoreError("Unable to write session artifact") from error
        self.verify_object(bucket, object_key, content_type, digest, size_bytes)

    def put_or_verify_bytes(
        self,
        bucket: str,
        object_key: str,
        payload: bytes,
        content_type: str,
    ) -> None:
        """Write small immutable receipt bytes or verify a replay exactly matches."""

        digest = sha256(payload).digest()
        if self._exists(bucket, object_key):
            self.verify_object(bucket, object_key, content_type, digest, len(payload))
            return
        try:
            self._client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=payload,
                ContentType=content_type,
                Metadata={"sha256": digest.hex()},
            )
        except (BotoCoreError, ClientError, OSError) as error:
            raise ObjectStoreError("Unable to write Blobmeta receipt") from error
        self.verify_object(bucket, object_key, content_type, digest, len(payload))

    def verify_object(
        self,
        bucket: str,
        object_key: str,
        content_type: str,
        digest: bytes,
        size_bytes: int,
    ) -> None:
        """Validate immutable object metadata and stream its bytes for a full hash."""

        try:
            head = self._client.head_object(Bucket=bucket, Key=object_key)
            metadata = head.get("Metadata", {})
            if (
                head.get("ContentLength") != size_bytes
                or head.get("ContentType") != content_type
                or metadata.get("sha256") != digest.hex()
            ):
                raise ObjectStoreError("Object metadata does not match immutable session evidence")
            response = self._client.get_object(Bucket=bucket, Key=object_key)
            body: BinaryIO = response["Body"]
            actual_digest = sha256()
            actual_size = 0
            try:
                while chunk := body.read(64 * 1024):
                    actual_digest.update(chunk)
                    actual_size += len(chunk)
            finally:
                body.close()
        except ObjectStoreError:
            raise
        except (BotoCoreError, ClientError, OSError) as error:
            raise ObjectStoreError("Unable to verify immutable session object") from error
        if actual_size != size_bytes or actual_digest.digest() != digest:
            raise ObjectStoreError("Object bytes do not match immutable session evidence")

    def _exists(self, bucket: str, object_key: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=object_key)
        except ClientError as error:
            if _is_missing(error):
                return False
            raise ObjectStoreError("Unable to read object metadata") from error
        except (BotoCoreError, OSError) as error:
            raise ObjectStoreError("Unable to read object metadata") from error
        return True


def _is_missing(error: ClientError) -> bool:
    code = str(error.response.get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchKey", "NotFound"}