"""Server-side dispatcher for firmware updates.

The actual transport-specific transaction is implemented by the target UAV.
This extension owns target discovery, release validation, per-vehicle mutual
exclusion and asynchronous FW-UPLOAD command execution.
"""

from __future__ import annotations

from base64 import b64decode
from binascii import Error as Base64Error
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from hashlib import md5, sha256
from hmac import compare_digest
from json import JSONDecodeError, loads
from re import fullmatch
from typing import Any, AsyncIterator, Iterator, Mapping
from zlib import decompress, error as ZlibError

from trio import sleep_forever

from flockwave.server.ext.base import Extension
from flockwave.server.message_handlers import create_mapper, create_multi_object_message_handler
from flockwave.server.model import Client, FlockwaveMessage
from flockwave.server.model.commands import Progress


@dataclass(frozen=True)
class FirmwareUpdateTarget:
    id: str
    name: str


@dataclass(frozen=True)
class FirmwareUpdateRequest:
    blob: bytes
    manifest: Mapping[str, Any]
    minimum_battery_voltage: float
    source_format: str
    target_id: str


class FirmwareUpdateExtension(Extension):
    def __init__(self) -> None:
        super().__init__()
        self._active_object_ids: set[str] = set()
        self._minimum_battery_voltage = 7.0
        self._targets: dict[str, FirmwareUpdateTarget] = {}

    def configure(self, configuration: Mapping[str, Any]) -> None:
        self._minimum_battery_voltage = float(
            configuration.get("minimum_battery_voltage", 7.0)
        )
        if self._minimum_battery_voltage <= 0:
            raise ValueError("minimum_battery_voltage must be positive")

    def exports(self) -> dict[str, Any]:
        return {
            "create_target": self.create_target,
            "use_target": self.use_target,
        }

    def create_target(self, *, id: str, name: str) -> FirmwareUpdateTarget:
        if not id or not name:
            raise ValueError("firmware update targets need a non-empty ID and name")
        return FirmwareUpdateTarget(id=id, name=name)

    @contextmanager
    def use_target(self, target: FirmwareUpdateTarget) -> Iterator[FirmwareUpdateTarget]:
        existing = self._targets.get(target.id)
        if existing is not None and existing != target:
            raise KeyError(f"Firmware update target ID already registered: {target.id}")
        self._targets[target.id] = target
        try:
            yield target
        finally:
            if self._targets.get(target.id) == target:
                self._targets.pop(target.id, None)

    async def run(self) -> None:
        assert self.app is not None

        upload_mapper = create_mapper(
            "FW-UPLOAD",
            self.app.object_registry,
            context_getter=self._extract_update_request,
            getter=self._perform_update,
            filter=self._supports_any_registered_target,
            description="firmware-updatable object",
            cmd_manager=self.app.command_execution_manager,
        )

        with ExitStack() as stack:
            stack.enter_context(
                self.app.message_hub.use_message_handlers(
                    {
                        "FW-OBJECT-LIST": self._handle_object_list,
                        "FW-TARGET-INF": self._handle_target_info,
                        "FW-TARGET-LIST": self._handle_target_list,
                        "FW-UPLOAD": create_multi_object_message_handler(upload_mapper),
                    }
                )
            )
            await sleep_forever()

    def _extract_update_request(
        self, message: FlockwaveMessage | None
    ) -> FirmwareUpdateRequest:
        if message is None:
            raise RuntimeError("firmware update request is missing")

        target_id = message.body.get("target")
        if not isinstance(target_id, str) or target_id not in self._targets:
            raise RuntimeError("unknown firmware update target")

        source_format = message.body.get("format", "abin")
        if source_format not in ("abin", "apj", "bin"):
            raise RuntimeError("unsupported firmware image format")

        encoded_blob = message.body.get("blob")
        if not isinstance(encoded_blob, str):
            raise RuntimeError("firmware image is missing")
        try:
            source_blob = b64decode(encoded_blob, validate=True)
        except (Base64Error, ValueError):
            raise RuntimeError("firmware image is not valid Base64") from None

        blob, manifest = self._prepare_release(
            source_blob, source_format, message.body.get("manifest")
        )

        return FirmwareUpdateRequest(
            blob=blob,
            manifest=manifest,
            minimum_battery_voltage=self._minimum_battery_voltage,
            source_format=source_format,
            target_id=target_id,
        )

    @classmethod
    def _prepare_release(
        cls, source: bytes, source_format: str, manifest: object
    ) -> tuple[bytes, Mapping[str, Any]]:
        if source_format == "apj" and manifest is None:
            if not source:
                raise RuntimeError("firmware image is empty")
            body, git_identity = cls._decode_apj(source)
            if (
                not isinstance(git_identity, str)
                or fullmatch(r"(?:[0-9a-fA-F]{8}|[0-9a-fA-F]{40})", git_identity)
                is None
            ):
                raise RuntimeError("APJ image has an invalid git_identity")

            blob = cls._build_abin(body, git_identity)
            manifest = cls._create_release_manifest(blob, git_identity)
        else:
            if not isinstance(manifest, dict):
                raise RuntimeError("release manifest is missing")
            blob = cls._normalize_to_abin(source, source_format, manifest)

        cls._validate_release(blob, manifest)
        return blob, manifest

    @classmethod
    def _normalize_to_abin(
        cls, source: bytes, source_format: str, manifest: Mapping[str, Any]
    ) -> bytes:
        if not source:
            raise RuntimeError("firmware image is empty")
        if source_format == "abin":
            return source

        body = source
        if source_format == "apj":
            body, git_identity = cls._decode_apj(source)
            manifest_git_sha = str(manifest.get("gitSha", ""))
            if isinstance(git_identity, str) and not compare_digest(
                git_identity.lower(), manifest_git_sha[: len(git_identity)].lower()
            ):
                raise RuntimeError("APJ Git identity does not match release manifest")

        git_sha = manifest.get("gitSha")
        return cls._build_abin(body, git_sha)

    @staticmethod
    def _decode_apj(source: bytes) -> tuple[bytes, object]:
        try:
            apj = loads(source)
        except (JSONDecodeError, UnicodeDecodeError):
            raise RuntimeError("APJ image is not valid JSON") from None
        if not isinstance(apj, dict) or apj.get("magic") != "APJFWv1":
            raise RuntimeError("APJ image has an invalid magic value")
        if apj.get("board_id") != 5602:
            raise RuntimeError("APJ image is not for APJ board ID 5602")
        encoded_image = apj.get("image")
        if not isinstance(encoded_image, str):
            raise RuntimeError("APJ image payload is missing")
        try:
            body = decompress(b64decode(encoded_image, validate=True))
        except (Base64Error, ValueError, ZlibError):
            raise RuntimeError("APJ firmware payload is invalid") from None
        if apj.get("image_size") != len(body):
            raise RuntimeError("APJ firmware size does not match its metadata")
        return body, apj.get("git_identity")

    @staticmethod
    def _build_abin(body: bytes, git_sha: object) -> bytes:
        if (
            not isinstance(git_sha, str)
            or fullmatch(r"(?:[0-9a-fA-F]{8}|[0-9a-fA-F]{40})", git_sha) is None
        ):
            raise RuntimeError("release manifest has an invalid gitSha")
        body_md5 = md5(body, usedforsecurity=False).hexdigest()
        header = f"git version: {git_sha.lower()}\nMD5: {body_md5}\n--\n".encode(
            "ascii"
        )
        return header + body

    @staticmethod
    def _create_release_manifest(blob: bytes, git_sha: str) -> dict[str, Any]:
        return {
            "schemaVersion": 1,
            "vehicleType": "ArduCopter",
            "boardName": "DPH_FC_088",
            "apjBoardId": 5602,
            "gitSha": git_sha.lower(),
            "abinSize": len(blob),
            "abinSha256": sha256(blob).hexdigest(),
        }

    @staticmethod
    def _validate_release(blob: bytes, manifest: object) -> None:
        if not isinstance(manifest, dict):
            raise RuntimeError("release manifest is missing")

        expected_values = {
            "schemaVersion": 1,
            "vehicleType": "ArduCopter",
            "boardName": "DPH_FC_088",
            "apjBoardId": 5602,
        }
        for key, expected in expected_values.items():
            if manifest.get(key) != expected:
                raise RuntimeError(f"release manifest has an invalid {key}")

        git_sha = manifest.get("gitSha")
        image_sha = manifest.get("abinSha256")
        if (
            not isinstance(git_sha, str)
            or fullmatch(r"(?:[0-9a-fA-F]{8}|[0-9a-fA-F]{40})", git_sha) is None
        ):
            raise RuntimeError("release manifest has an invalid gitSha")
        if not isinstance(image_sha, str) or fullmatch(r"[0-9a-fA-F]{64}", image_sha) is None:
            raise RuntimeError("release manifest has an invalid abinSha256")
        if manifest.get("abinSize") != len(blob):
            raise RuntimeError("firmware image size does not match the release manifest")

        observed_sha = sha256(blob).hexdigest()
        if not compare_digest(observed_sha, image_sha.lower()):
            raise RuntimeError("firmware SHA-256 does not match the release manifest")

    async def _perform_update(
        self, object: Any, request: FirmwareUpdateRequest
    ) -> AsyncIterator[Progress]:
        if object.id in self._active_object_ids:
            raise RuntimeError("another firmware update is already active for this vehicle")
        if not self._supports_target(object, request.target_id):
            raise RuntimeError("vehicle does not support the selected firmware target")

        self._active_object_ids.add(object.id)
        try:
            async for event in object.handle_firmware_update(
                request.target_id,
                request.blob,
                manifest=request.manifest,
                minimum_battery_voltage=request.minimum_battery_voltage,
            ):
                yield event
        finally:
            self._active_object_ids.discard(object.id)

    def _handle_object_list(
        self, message: FlockwaveMessage, sender: Client, hub: Any
    ) -> dict[str, list[str]]:
        assert self.app is not None
        requested_targets = message.body.get("supports")
        if requested_targets is None:
            requested_targets = list(self._targets)
        if not isinstance(requested_targets, list) or not all(
            isinstance(item, str) for item in requested_targets
        ):
            return {"ids": []}

        ids = []
        for object_id in self.app.object_registry.ids:
            object = self.app.object_registry.find_by_id(object_id)
            if all(self._supports_target(object, target) for target in requested_targets):
                ids.append(object_id)
        return {"ids": ids}

    def _handle_target_info(
        self, message: FlockwaveMessage, sender: Client, hub: Any
    ) -> dict[str, dict[str, dict[str, str]]]:
        result = {}
        for target_id in message.get_ids():
            target = self._targets.get(target_id)
            if target is not None:
                result[target_id] = {"id": target.id, "name": target.name}
        return {"result": result}

    def _handle_target_list(
        self, message: FlockwaveMessage, sender: Client, hub: Any
    ) -> dict[str, list[str]]:
        supported_by = message.body.get("supportedBy")
        if not supported_by:
            return {"ids": sorted(self._targets)}
        if not isinstance(supported_by, list):
            return {"ids": []}

        assert self.app is not None
        targets = []
        for target_id in sorted(self._targets):
            if all(
                object_id in self.app.object_registry
                and self._supports_target(
                    self.app.object_registry.find_by_id(object_id), target_id
                )
                for object_id in supported_by
            ):
                targets.append(target_id)
        return {"ids": targets}

    def _supports_any_registered_target(self, object: Any) -> bool:
        return any(self._supports_target(object, target) for target in self._targets)

    @staticmethod
    def _supports_target(object: Any, target_id: str) -> bool:
        checker = getattr(object, "can_handle_firmware_update_target", None)
        return callable(checker) and bool(checker(target_id))


schema = {
    "type": "object",
    "properties": {
        "minimum_battery_voltage": {
            "type": "number",
            "exclusiveMinimum": 0,
            "default": 7.0,
        }
    },
    "additionalProperties": False,
}
