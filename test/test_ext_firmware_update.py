from base64 import b64encode
from hashlib import md5, sha256
from json import dumps
from zlib import compress

import pytest

from flockwave.server.ext.firmware_update.extension import FirmwareUpdateExtension
from flockwave.server.ext.mavlink.autopilots.ardupilot import ArduPilot


GIT_SHA = "0123456789abcdef0123456789abcdef01234567"
BODY = b"DPH_FC_088 firmware body"


def make_abin(body: bytes = BODY) -> bytes:
    digest = md5(body, usedforsecurity=False).hexdigest()
    return f"git version: {GIT_SHA}\nMD5: {digest}\n--\n".encode() + body


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
            "git_identity": GIT_SHA,
        }
    ).encode()

    observed = FirmwareUpdateExtension._normalize_to_abin(apj, "apj", manifest)

    assert observed == expected
    FirmwareUpdateExtension._validate_release(observed, manifest)


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


def test_two_cell_li_ion_voltage_threshold_defaults_to_seven_volts() -> None:
    extension = FirmwareUpdateExtension()

    extension.configure({})

    assert extension._minimum_battery_voltage == 7.0
