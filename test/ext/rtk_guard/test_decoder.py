from bitstring import BitArray, pack
from flockwave.gps.rtcm.packets import RTCMV3MSMPacket

from flockwave.server.ext.rtk_guard.decoder import RTCMObservationDecoder
from flockwave.server.ext.rtk_guard.model import SatKey


def test_msm7_decoder_preserves_phase_lock_half_cycle_and_signal_identity():
    payload = BitArray()
    payload += pack("uint:12, uint:12, uint:30", 1077, 42, 123_000)
    payload += pack(
        "uint:1, uint:3, uint:7, uint:2, uint:2, uint:1, uint:3",
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    )
    payload += BitArray("0b11" + "0" * 62)  # G01, G02
    payload += BitArray("0b1" + "0" * 31)  # signal 1
    payload += BitArray("0b11")  # both cells present
    payload += pack("uint:8, uint:8", 70, 71)
    payload += pack("uint:4, uint:4", 0, 0)
    payload += pack("uint:10, uint:10", 512, 256)
    payload += pack("int:14, int:14", 0, 0)
    payload += pack("int:20, int:20", 100, 200)
    payload += pack("int:24, int:24", 300, 400)
    payload += pack("uint:10, uint:10", 511, 512)
    payload += pack("bool, bool", False, True)
    payload += pack("uint:10, uint:10", 720, 736)
    payload += pack("int:15, int:15", 0, 0)

    packet = RTCMV3MSMPacket(1077)
    packet.bytes = payload.tobytes()
    decoded = RTCMObservationDecoder.decode_packet(packet)

    assert decoded is not None
    assert decoded.station_id == 42
    assert decoded.epoch == 123
    first = decoded.observations[SatKey("GPS", 1, 1)]
    second = decoded.observations[SatKey("GPS", 2, 1)]
    assert first.lock_time == 511
    assert first.half_cycle is False
    assert first.cnr == 45
    assert first.phase_range is not None
    assert second.lock_time == 512
    assert second.half_cycle is True
    assert second.cnr == 46
