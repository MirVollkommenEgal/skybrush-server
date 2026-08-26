"""RTCM3 observation decoder used by the guard.

``flockwave.gps`` intentionally exposes only the MSM fields needed by the
normal RTK status display.  The guard needs the raw code/phase and lock fields,
so this adapter decodes those fields from the already CRC-validated payload.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Optional

from bitstring import ConstBitStream
from flockwave.gps.rtcm.packets import (
    RTCMV3MSMPacket,
    RTCMV3Packet,
    RTCMV3StationaryAntennaPacket,
)
from flockwave.gps.rtcm.parsers import RTCMV3Parser

from .model import DecodedRTCM, SatKey, SatelliteObservation


_C_M_PER_MS = 299792.458
_CONSTELLATIONS = {
    107: "GPS",
    108: "GLONASS",
    109: "GALILEO",
    111: "QZSS",
    112: "BEIDOU",
}


class RTCMObservationDecoder:
    """Incremental decoder for RTCM3 position and MSM4/MSM7 messages."""

    def __init__(self) -> None:
        self._parser = RTCMV3Parser()

    def feed(self, data: bytes) -> Iterable[DecodedRTCM]:
        for packet in self._parser.feed(data):
            decoded = self.decode_packet(packet)
            if decoded is not None:
                yield decoded

    @staticmethod
    def decode_packet(packet: RTCMV3Packet) -> Optional[DecodedRTCM]:
        if isinstance(packet, RTCMV3StationaryAntennaPacket):
            position = packet.position
            return DecodedRTCM(
                message_type=packet.packet_type or 0,
                station_id=packet.station_id,
                reported_ecef=(position.x, position.y, position.z),
            )
        if isinstance(packet, RTCMV3MSMPacket) and packet.packet_type is not None:
            return _decode_msm(packet)
        return None


def _read_signed(stream: ConstBitStream, width: int, scale: float) -> Optional[float]:
    raw = stream.read(f"int:{width}")
    if raw == -(1 << (width - 1)):
        return None
    return raw * scale


def _decode_msm(packet: RTCMV3MSMPacket) -> DecodedRTCM:
    packet_type = packet.packet_type or 0
    msm_kind = packet_type % 10
    high_resolution = msm_kind in (6, 7)
    constellation = _CONSTELLATIONS.get(packet_type // 10, "UNKNOWN")
    if packet.bytes is None:
        raise ValueError("RTCM packet has no payload")

    bits = ConstBitStream(packet.bytes)
    bits.read(12)  # message type
    station_id = bits.read("uint:12")
    epoch = bits.read("uint:30") * 0.001
    bits.read(19)  # multiple-message and clock/smoothing metadata

    satellite_mask = bits.read(64)
    satellite_ids = [i + 1 for i, enabled in enumerate(satellite_mask) if enabled]
    signal_mask = bits.read(32)
    signal_ids = [i + 1 for i, enabled in enumerate(signal_mask) if enabled]
    cell_mask = bits.read(len(satellite_ids) * len(signal_ids))
    cells = [
        (satellite_id, signal_id)
        for enabled, (satellite_id, signal_id) in zip(
            cell_mask,
            (
                (satellite_id, signal_id)
                for satellite_id in satellite_ids
                for signal_id in signal_ids
            ),
        )
        if enabled
    ]

    rough_integer = [bits.read("uint:8") for _ in satellite_ids]
    if high_resolution:
        for _ in satellite_ids:
            bits.read(4)  # extended satellite information
    rough_modulo = [bits.read("uint:10") for _ in satellite_ids]
    if high_resolution:
        for _ in satellite_ids:
            bits.read(14)  # rough phase range rate

    if high_resolution:
        pseudoranges = [_read_signed(bits, 20, 2**-29) for _ in cells]
        phases = [_read_signed(bits, 24, 2**-31) for _ in cells]
        locks = [bits.read("uint:10") for _ in cells]
    else:
        pseudoranges = [_read_signed(bits, 15, 2**-24) for _ in cells]
        phases = [_read_signed(bits, 22, 2**-29) for _ in cells]
        locks = [bits.read("uint:4") for _ in cells]
    half_cycles = [bits.read("bool") for _ in cells]
    if high_resolution:
        cnrs = [bits.read("uint:10") * 0.0625 for _ in cells]
    else:
        cnrs = [float(bits.read("uint:6")) for _ in cells]
    if msm_kind in (5, 7):
        for _ in cells:
            bits.read(15)  # fine phase range rate

    satellite_index = {satellite_id: i for i, satellite_id in enumerate(satellite_ids)}
    observations: dict[SatKey, SatelliteObservation] = {}
    for cell, fine_code, fine_phase, lock, half_cycle, cnr in zip(
        cells, pseudoranges, phases, locks, half_cycles, cnrs
    ):
        satellite_id, signal_id = cell
        index = satellite_index[satellite_id]
        integer, modulo = rough_integer[index], rough_modulo[index]
        rough = None if integer == 255 else integer + modulo / 1024
        observations[SatKey(constellation, satellite_id, signal_id)] = (
            SatelliteObservation(
                epoch=epoch,
                pseudorange=(rough + fine_code) * _C_M_PER_MS
                if rough is not None and fine_code is not None
                else None,
                phase_range=(rough + fine_phase) * _C_M_PER_MS
                if rough is not None and fine_phase is not None
                else None,
                cnr=cnr,
                lock_time=lock,
                half_cycle=half_cycle,
            )
        )

    return DecodedRTCM(
        message_type=packet_type,
        station_id=station_id,
        constellation=constellation,
        epoch=epoch,
        observations=observations,
    )
