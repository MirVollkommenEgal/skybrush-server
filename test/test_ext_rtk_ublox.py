from struct import pack, unpack_from
from types import SimpleNamespace

import pytest
import trio

from flockwave.gps.rtk import RTKSurveySettings
from flockwave.gps.ubx.packet import UBXPacket
from flockwave.gps.vectors import ECEFCoordinate

from flockwave.server.ext.rtk import extension as rtk
from flockwave.server.ext.rtk.allystar import AllystarPositionECEFPacket
from flockwave.server.ext.rtk.preset import RTKConfigurationPreset
from flockwave.server.ext.rtk.statistics import ReceiverPosition, RTKStatistics
from flockwave.server.ext.rtk.ublox import (
    UBXAcknowledgementTracker,
    UBXRTKBaseConfiguratorWithValset,
)


def valset_values(packet):
    assert packet[:4] == b"\xb5\x62\x06\x8a"
    assert packet[6:10] == b"\x00\x01\x00\x00"  # RAM only
    result = {}
    offset = 10
    while offset < len(packet) - 2:
        key = unpack_from("<I", packet, offset)[0]
        offset += 4
        size = {1: 1, 2: 1, 3: 2, 4: 4, 5: 8}[key >> 28]
        result[key] = int.from_bytes(packet[offset : offset + size], "little")
        offset += size
    return result


def pvt(tow=1000, flags=0x83, fix=3):
    payload = bytearray(92)
    payload[:4] = pack("<I", tow)
    payload[20:24] = bytes((fix, flags, 0, 25))
    return UBXPacket(1, 7, payload)


def hpecef(tow=1000, invalid=0):
    payload = bytes(4) + pack(
        "<IiiibbbBI", tow, 123, -456, 789, 12, -34, 56, invalid, 200
    )
    return UBXPacket(1, 0x13, payload)


@pytest.mark.trio
async def test_guided_setup_is_rover_then_fixed_then_output():
    tracker = UBXAcknowledgementTracker()
    packets = []

    async def write(packet):
        packets.append(packet)
        tracker.notify(UBXPacket(5, 1, b"\x06\x8a"))

    await UBXRTKBaseConfiguratorWithValset(RTKSurveySettings()).configure_rover(
        write, tracker
    )
    rover = [valset_values(p) for p in packets]
    assert rover[0][0x20030001] == 0  # Rover, never survey-in
    assert rover[0][0x10780004] == 0  # No base RTCM output
    assert rover[0][0x10770004] == 1  # RTCM input enabled
    assert rover[1][0x20910009] == rover[1][0x20910031] == 1
    assert rover[1][0x2091008B] == 0
    assert all(p.get(0x20030001) != 1 for p in rover)

    settings = RTKSurveySettings(
        position=ECEFCoordinate(1.2345, -2.3456, 3.4567), accuracy=0.025
    )
    await UBXRTKBaseConfiguratorWithValset(settings).configure_fixed(write, tracker)
    fixed, output = [valset_values(p) for p in packets[2:]]
    assert fixed[0x20030001] == 2
    assert 0x10780004 not in fixed
    assert 0x2091008B not in fixed  # No SVIN status overwrites the final accuracy
    assert fixed[0x4003000F] == 250
    for key, hp_key, expected in [
        (0x40030003, 0x20030006, 12345),
        (0x40030004, 0x20030007, -23456),
        (0x40030005, 0x20030008, 34567),
    ]:
        coarse = fixed[key]
        if coarse >= 2**31:
            coarse -= 2**32
        assert coarse * 100 + fixed[hp_key] == expected
    assert output == {0x10780004: 1}


@pytest.mark.trio
@pytest.mark.parametrize("response", [None, 0])
async def test_mode_transition_requires_ack(response):
    tracker = UBXAcknowledgementTracker()
    packets = []

    async def write(packet):
        packets.append(packet)
        tracker.notify(UBXPacket(5, 1, b"\x06\x01"))  # Unrelated ACK
        if response is not None:
            tracker.notify(UBXPacket(5, response, b"\x06\x8a"))

    with pytest.raises(RuntimeError, match="not acknowledged|ACK-NAK"):
        await tracker.send(b"configuration", write, timeout=0.01)
    assert len(packets) == 1


@pytest.mark.trio
async def test_rejected_coordinates_never_enable_output():
    tracker = UBXAcknowledgementTracker()
    packets = []

    async def write(packet):
        packets.append(packet)
        tracker.notify(UBXPacket(5, 0, b"\x06\x8a"))

    settings = RTKSurveySettings(position=ECEFCoordinate(1, 2, 3))
    with pytest.raises(RuntimeError, match="ACK-NAK"):
        await UBXRTKBaseConfiguratorWithValset(settings).configure_fixed(write, tracker)
    assert len(packets) == 1
    assert 0x10780004 not in valset_values(packets[0])


@pytest.mark.parametrize("reverse", [False, True])
def test_position_uses_matching_epoch_fixed_solution_and_high_precision(reverse):
    stats = RTKStatistics()
    packets = [pvt(), hpecef()]
    for packet in reversed(packets) if reverse else packets:
        stats.notify(packet, forwarded=False)
    solution = stats.receiver_position
    assert solution is not None and solution.rtk_fixed
    assert solution.position.x == pytest.approx(1.2312)
    assert solution.position.y == pytest.approx(-4.5634)
    assert solution.position.z == pytest.approx(7.8956)
    assert solution.accuracy == pytest.approx(0.02)
    assert stats.json["satelliteCount"].json == 25
    # Replaying the same epoch must not create another sample.
    stats.notify(hpecef())
    assert stats.receiver_position is solution


@pytest.mark.parametrize(
    "flags,fix", [(0x43, 3), (0x03, 3), (0x82, 3), (0x81, 3), (0x83, 5)]
)
def test_float_invalid_or_time_only_solution_is_not_averaged(flags, fix):
    stats = RTKStatistics()
    stats.notify(hpecef())
    stats.notify(pvt(flags=flags, fix=fix))
    assert stats.receiver_position is None


def test_invalid_position_epoch_mismatch_and_fix_loss():
    stats = RTKStatistics()
    stats.notify(pvt(tow=1000))
    stats.notify(hpecef(tow=2000))
    assert stats.receiver_position is None
    stats.notify(pvt(tow=2000))
    assert stats.receiver_position is not None
    stats.notify(hpecef(tow=3000, invalid=1))
    assert stats.receiver_position is None
    stats.notify(hpecef(tow=4000))
    stats.notify(pvt(tow=4000))
    assert stats.receiver_position is not None
    stats.notify(pvt(tow=5000, flags=0x43))
    assert stats.receiver_position is None


def test_guided_accuracy_is_not_overwritten_by_receiver_packets():
    stats = RTKStatistics()
    with stats.guided_survey():
        stats.set_to_surveying_with_accuracy(0.025)
        stats.notify(AllystarPositionECEFPacket(ECEFCoordinate(1, 2, 3), 9))
        stats.notify(UBXPacket(1, 0x3B, bytes(40)))
        assert stats.json["survey"].json == {"flags": 3, "accuracy": 0.025}
        stats.set_to_fixed_with_accuracy(0.025)
    assert stats.json["survey"].json == {"flags": 5, "accuracy": 0.025}


def test_aborted_guided_survey_clears_active_state():
    stats = RTKStatistics()
    with pytest.raises(RuntimeError), stats.guided_survey():
        raise RuntimeError("aborted")
    assert stats.json["survey"].json == {"flags": 0}


@pytest.mark.trio
async def test_average_publishes_mean_accuracy_and_returns_mean_coordinates(
    monkeypatch,
):
    ext = rtk.RTKExtension()
    preset = RTKConfigurationPreset(
        id="usb",
        base_type="ublox",
        ntrip_assist_duration=2,
        ntrip_assist_timeout=10,
        ntrip_assist_stability=0.05,
    )
    now = [0.0]
    updates = []
    monkeypatch.setattr(rtk, "monotonic", lambda: now[0])

    async def sleep(_):
        now[0] += 1
        ext._statistics.receiver_position = ReceiverPosition(
            ECEFCoordinate(now[0] * 0.01, 0, 0), now[0] * 0.01, now[0], True
        )
        if now[0] > 1:
            updates.append(ext._statistics.json["survey"].json.copy())

    monkeypatch.setattr(rtk, "sleep", sleep)
    result = await ext._average_stable_antenna_position(preset)
    assert result.sample_count == 3
    assert result.position.x == pytest.approx(0.02)
    assert result.accuracy == pytest.approx(0.02)
    assert updates == [{"flags": 3, "accuracy": 0.01}, {"flags": 3, "accuracy": 0.015}]


@pytest.mark.trio
@pytest.mark.parametrize(
    "case", ["stale", "float", "corrections_lost", "poor_accuracy", "drifting"]
)
async def test_average_does_not_converge_with_unusable_measurements(monkeypatch, case):
    ext = rtk.RTKExtension()
    preset = RTKConfigurationPreset(
        id="usb",
        base_type="ublox",
        ntrip_assist_duration=2,
        ntrip_assist_timeout=6,
        ntrip_assist_stability=0.05,
    )
    now = [0.0]
    monkeypatch.setattr(rtk, "monotonic", lambda: now[0])

    async def sleep(_):
        now[0] += 1
        if case == "stale" and now[0] > 1:
            return
        ext._statistics.receiver_position = ReceiverPosition(
            ECEFCoordinate(now[0] if case == "drifting" else 1, 2, 3),
            5 if case == "poor_accuracy" else 0.02,
            now[0],
            case != "float",
        )

    monkeypatch.setattr(rtk, "sleep", sleep)
    result = await ext._average_stable_antenna_position(
        preset, corrections_fresh=lambda: case != "corrections_lost"
    )
    assert result is None


@pytest.mark.trio
async def test_fix_loss_resets_averaging_window(monkeypatch):
    ext = rtk.RTKExtension()
    preset = RTKConfigurationPreset(
        id="usb", ntrip_assist_duration=2, ntrip_assist_timeout=10
    )
    now = [0.0]
    monkeypatch.setattr(rtk, "monotonic", lambda: now[0])

    async def sleep(_):
        now[0] += 1
        ext._statistics.receiver_position = (
            None
            if now[0] == 3
            else ReceiverPosition(ECEFCoordinate(1, 2, 3), 0.02, now[0], True)
        )

    monkeypatch.setattr(rtk, "sleep", sleep)
    result = await ext._average_stable_antenna_position(preset)
    assert now[0] == 6
    assert result.sample_count == 3
    assert result.duration == 2


@pytest.mark.trio
@pytest.mark.parametrize("reject_fixed", [False, True])
async def test_complete_assisted_survey_keeps_forwarding_blocked_until_base_ack(
    monkeypatch, reject_fixed
):
    ext = rtk.RTKExtension()
    preset = RTKConfigurationPreset(
        id="usb",
        base_type="ublox",
        ntrip_assist="ntrip://example/base",
        ntrip_assist_duration=2,
        ntrip_assist_timeout=10,
    )
    now = [0.0]
    monkeypatch.setattr(rtk, "monotonic", lambda: now[0])
    monkeypatch.setattr("flockwave.server.ext.rtk.statistics.monotonic", lambda: now[0])
    events = []

    class Receiver:
        async def wait_until_connected(self):
            await trio.sleep(0)

        async def write(self, data):
            assert not ext._rtcm_forwarding_enabled
            if data.startswith(b"\xb5\x62"):
                values = valset_values(data)
                events.append(values)
                reject = reject_fixed and values.get(0x20030001) == 2
                ext._ubx_acknowledgements.notify(
                    UBXPacket(5, 0 if reject else 1, b"\x06\x8a")
                )
            else:
                assert events[0][0x20030001] == 0
                assert len(events) >= 2  # Both rover setup commands already ACKed
                events.append("correction")

    class Ntrip:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            events.append("ntrip_closed")

        async def read(self, _):
            await trio.sleep(0)
            return b"correction"

    async def sleep(_):
        await trio.sleep(0)
        now[0] += 1
        tow = int(now[0] * 1000)
        ext._statistics.notify(pvt(tow))
        payload = bytes(4) + pack(
            "<IiiibbbBI", tow, 390000000, 90000000, 500000000, 0, 0, 0, 0, 200
        )
        ext._statistics.notify(UBXPacket(1, 0x13, payload))

    monkeypatch.setattr(rtk, "sleep", sleep)
    monkeypatch.setattr(rtk, "create_connection", lambda _: Ntrip())
    started = []
    await ext._run_survey(
        preset, Receiver(), task_status=SimpleNamespace(started=started.append)
    )
    assert started
    commands = [event for event in events if isinstance(event, dict)]
    assert commands[0][0x20030001] == 0
    assert commands[2][0x20030001] == 2
    assert events.index("ntrip_closed") < events.index(commands[2])
    if reject_fixed:
        assert len(commands) == 3
        assert not ext._rtcm_forwarding_enabled
        assert ext._statistics.json["survey"].json == {"flags": 0}
    else:
        assert commands[3] == {0x10780004: 1}
        assert ext._rtcm_forwarding_enabled
        assert ext._statistics.json["survey"].json == {"flags": 5, "accuracy": 0.02}


@pytest.mark.parametrize("guided", [False, True])
def test_detected_usb_source_inherits_guided_survey_settings(monkeypatch, guided):
    from logging import getLogger
    from flockwave.server.ext.rtk.registry import RTKPresetRegistry

    ext = rtk.RTKExtension()
    ext.log = getLogger("test.rtk")
    configuration = {"add_serial_ports": [115200]}
    if guided:
        configuration["serial_ntrip_assist"] = {
            "source": "ntrip://example/base",
            "duration": 60,
            "timeout": 600,
            "stability": 0.05,
        }
    ext.configure(configuration)
    ext._registry = RTKPresetRegistry()
    port = SimpleNamespace(
        device="/dev/cu.usbmodem123",
        description="u-blox X20",
        name="usbmodem123",
        hwid="USB",
        vid=0x1546,
        pid=0x01A9,
    )
    monkeypatch.setattr(rtk, "list_serial_ports", lambda: [port])
    ext._update_dynamic_presets(first=True)
    presets = list(ext._registry)
    assert len(presets) == 1
    preset = presets[0]
    assert preset.sources == ["serial:/dev/cu.usbmodem123?baud=115200"]
    assert preset.auto_survey
    if guided:
        assert preset.base_type == "ublox"
        assert preset.ntrip_assist == "ntrip://example/base"
        assert preset.ntrip_assist_duration == 60
        assert preset.ntrip_assist_timeout == 600
        assert preset.ntrip_assist_stability == 0.05
    else:
        assert preset.ntrip_assist is None
