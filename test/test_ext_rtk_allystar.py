import pytest

from flockwave.gps.rtk import RTKMessageSet, RTKSurveySettings
from flockwave.gps.vectors import ECEFCoordinate

from flockwave.server.ext.rtk.allystar import (
    AllystarAcknowledgementPacket,
    AllystarAcknowledgementTracker,
    AllystarBinaryParser,
    AllystarConfigurationError,
    AllystarRTKBaseConfigurator,
)


def _packet_ids(packets: list[bytes]) -> list[tuple[int, int]]:
    return [(packet[2], packet[3]) for packet in packets]


def _create_allystar_packet(group_id: int, sub_id: int, payload: bytes) -> bytes:
    return AllystarRTKBaseConfigurator._create_packet(group_id, sub_id, payload)


def test_ack_and_nak_packets_are_parsed() -> None:
    parser = AllystarBinaryParser()

    packets = parser.feed(
        _create_allystar_packet(0x05, 0x01, b"\x06\x12")
        + _create_allystar_packet(0x05, 0x00, b"\x06\x14")
    )

    assert packets == [
        AllystarAcknowledgementPacket(True, 0x06, 0x12),
        AllystarAcknowledgementPacket(False, 0x06, 0x14),
    ]


def test_fragmented_ack_packet_is_parsed() -> None:
    parser = AllystarBinaryParser()
    encoded = _create_allystar_packet(0x05, 0x01, b"\x06\x14")

    packets = []
    for byte in encoded:
        packets.extend(parser.feed(bytes((byte,))))

    assert packets == [AllystarAcknowledgementPacket(True, 0x06, 0x14)]


@pytest.mark.trio
async def test_acknowledgement_tracker_rejects_nak() -> None:
    tracker = AllystarAcknowledgementTracker()
    tracker.prepare(0x06, 0x14)
    tracker.notify(AllystarAcknowledgementPacket(False, 0x06, 0x14))

    with pytest.raises(AllystarConfigurationError, match="ACK-NAK"):
        await tracker.wait()


@pytest.mark.trio
async def test_acknowledgement_tracker_ignores_unrelated_ack() -> None:
    tracker = AllystarAcknowledgementTracker()
    tracker.prepare(0x06, 0x14)
    tracker.notify(AllystarAcknowledgementPacket(True, 0x06, 0x12))

    with pytest.raises(AllystarConfigurationError, match="not acknowledged"):
        await tracker.wait(timeout=0.01)


def test_ntrip_assist_setup_does_not_start_receiver_survey() -> None:
    settings = RTKSurveySettings(message_set=RTKMessageSet.MSM7)
    configurator = AllystarRTKBaseConfigurator(settings)

    packets = configurator._create_configuration_packets(
        configure_position=False,
        enable_rtcm_output=False,
    )

    assert (0x06, 0x12) not in _packet_ids(packets)
    assert (0x06, 0x14) not in _packet_ids(packets)

    rtcm_rates = [
        packet[8]
        for packet in packets
        if packet[2:4] == b"\x06\x01"
        and len(packet) == 11
        and packet[6] == 0xF8
    ]
    assert rtcm_rates
    assert set(rtcm_rates) == {0}


def test_regular_setup_starts_survey_and_fixed_setup_uses_ecef() -> None:
    survey_packets = AllystarRTKBaseConfigurator(
        RTKSurveySettings(message_set=RTKMessageSet.MSM7)
    )._create_configuration_packets()
    assert (0x06, 0x12) in _packet_ids(survey_packets)
    assert (0x06, 0x14) not in _packet_ids(survey_packets)

    fixed_packets = AllystarRTKBaseConfigurator(
        RTKSurveySettings(
            position=ECEFCoordinate(x=1, y=2, z=3),
            message_set=RTKMessageSet.MSM7,
        )
    )._create_configuration_packets()
    assert (0x06, 0x12) not in _packet_ids(fixed_packets)
    assert (0x06, 0x14) in _packet_ids(fixed_packets)


def test_receiver_update_rate_is_configured_to_one_hz() -> None:
    packets = AllystarRTKBaseConfigurator(
        RTKSurveySettings(message_set=RTKMessageSet.MSM7)
    )._create_configuration_packets()

    rate_packet = next(packet for packet in packets if packet[2:4] == b"\x06\x44")

    assert rate_packet == bytes.fromhex(
        "F1 D9 06 44 10 00 00 00 01 00 01 00 00 00 "
        "E8 03 00 00 00 00 00 00 47 13"
    )
