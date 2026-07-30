from base64 import b64encode
from hashlib import md5, sha256
from json import dumps
from types import SimpleNamespace
from zlib import compress

import pytest

from flockwave.server.ext.firmware_update.extension import FirmwareUpdateExtension
from flockwave.server.ext.mavlink.autopilots.ardupilot import ArduPilot
from flockwave.server.ext.mavlink.enums import MAVState


GIT_SHA = "0123456789abcdef0123456789abcdef01234567"
BODY = b"DPH_FC_088 firmware body"


def make_abin(body: bytes = BODY, git_sha: str = GIT_SHA) -> bytes:
    digest = md5(body, usedforsecurity=False).hexdigest()
    return f"git version: {git_sha}\nMD5: {digest}\n--\n".encode() + body


def make_manifest(abin: bytes) -> dict:
    return {
        "schemaVersion": 1,
        "vehicleType": "ArduCopter",
        "boardName": "DPH_FC_088",
        "apjBoardId": 5602,
        "gitSha": GIT_SHA,
        "firmwareVersion": "4.6.2-custom",
        "abinSize": len(abin),
        "abinSha256": sha256(abin).hexdigest(),
        "createdAt": "2026-07-16T12:00:00Z",
        "releaseNotes": "Test release",
    }


def test_bin_is_converted_to_expected_abin() -> None:
    expected = make_abin()
    manifest = make_manifest(expected)

    observed = FirmwareUpdateExtension._normalize_to_abin(BODY, "bin", manifest)

    assert observed == expected
    FirmwareUpdateExtension._validate_release(observed, manifest)


def test_apj_is_unpacked_and_converted_to_expected_abin() -> None:
    expected = make_abin()
    manifest = make_manifest(expected)
    apj = dumps(
        {
            "magic": "APJFWv1",
            "board_id": 5602,
            "image": b64encode(compress(BODY)).decode(),
            "image_size": len(BODY),
            "git_identity": GIT_SHA[:8],
        }
    ).encode()

    observed = FirmwareUpdateExtension._normalize_to_abin(apj, "apj", manifest)

    assert observed == expected
    FirmwareUpdateExtension._validate_release(observed, manifest)


def test_apj_release_manifest_is_derived_when_omitted() -> None:
    expected = make_abin(git_sha=GIT_SHA[:8])
    apj = dumps(
        {
            "magic": "APJFWv1",
            "board_id": 5602,
            "image": b64encode(compress(BODY)).decode(),
            "image_size": len(BODY),
            "git_identity": GIT_SHA[:8],
        }
    ).encode()

    observed, manifest = FirmwareUpdateExtension._prepare_release(apj, "apj", None)

    assert observed == expected
    assert ArduPilot._validate_abin(observed, manifest) == GIT_SHA[:8]
    assert manifest == {
        "schemaVersion": 1,
        "vehicleType": "ArduCopter",
        "boardName": "DPH_FC_088",
        "apjBoardId": 5602,
        "gitSha": GIT_SHA[:8],
        "abinSize": len(expected),
        "abinSha256": sha256(expected).hexdigest(),
    }


def test_apj_without_manifest_requires_git_identity() -> None:
    apj = dumps(
        {
            "magic": "APJFWv1",
            "board_id": 5602,
            "image": b64encode(compress(BODY)).decode(),
            "image_size": len(BODY),
        }
    ).encode()

    with pytest.raises(RuntimeError, match="git_identity"):
        FirmwareUpdateExtension._prepare_release(apj, "apj", None)


def test_apj_for_wrong_board_is_rejected() -> None:
    expected = make_abin()
    manifest = make_manifest(expected)
    apj = dumps(
        {
            "magic": "APJFWv1",
            "board_id": 1,
            "image": b64encode(compress(BODY)).decode(),
            "image_size": len(BODY),
        }
    ).encode()

    with pytest.raises(RuntimeError, match="board ID 5602"):
        FirmwareUpdateExtension._normalize_to_abin(apj, "apj", manifest)


def test_release_hash_mismatch_is_rejected() -> None:
    abin = make_abin()
    manifest = make_manifest(abin)
    manifest["abinSha256"] = "0" * 64

    with pytest.raises(RuntimeError, match="SHA-256"):
        FirmwareUpdateExtension._validate_release(abin, manifest)


def test_ardupilot_accepts_generated_abin() -> None:
    abin = make_abin()

    assert ArduPilot._validate_abin(abin, make_manifest(abin)) == GIT_SHA


def test_ardupilot_rejects_corrupt_abin_body() -> None:
    abin = make_abin()

    with pytest.raises(RuntimeError, match="MD5"):
        ArduPilot._validate_abin(abin + b"corruption", make_manifest(abin))


def test_ardupilot_matches_binary_prefix_from_short_apj_git_identity() -> None:
    version = SimpleNamespace(flight_custom_version=bytes.fromhex(GIT_SHA[:16]))

    assert ArduPilot._matches_firmware_hash(version, GIT_SHA[:8])


def test_standby_heartbeat_proves_landed_when_altitude_is_unavailable() -> None:
    heartbeat = SimpleNamespace(system_status=MAVState.STANDBY.value)

    assert ArduPilot._is_vehicle_landed(heartbeat, None)


def test_active_heartbeat_without_altitude_does_not_prove_landed() -> None:
    heartbeat = SimpleNamespace(system_status=MAVState.ACTIVE.value)

    assert not ArduPilot._is_vehicle_landed(heartbeat, None)


def test_two_cell_li_ion_voltage_threshold_defaults_to_seven_volts() -> None:
    extension = FirmwareUpdateExtension()

    extension.configure({})

    assert extension._minimum_battery_voltage == 7.0
