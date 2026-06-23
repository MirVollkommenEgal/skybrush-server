"""Helpers for configuring Allystar HD9300/HD9400 RTK receivers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from math import sqrt
from struct import pack, unpack_from
from typing import Any, Callable, Iterable

from flockwave.gps.enums import GNSSType
from flockwave.gps.rtk import RTKMessageSet, RTKSurveySettings
from flockwave.gps.vectors import ECEFCoordinate


@dataclass
class AllystarPVERRPacket:
    """Positioning error estimate from Allystar NAV-PVERR."""

    horizontal_accuracy: float
    vertical_accuracy: float

    @property
    def accuracy(self) -> float:
        """Conservative position accuracy estimate in meters."""
        return max(self.horizontal_accuracy, self.vertical_accuracy)


@dataclass
class AllystarPositionECEFPacket:
    """ECEF position solution from Allystar NAV-POSECEF."""

    position_ecef: ECEFCoordinate
    accuracy: float


class AllystarBinaryParser:
    """Minimal parser for Allystar binary packets needed by the RTK extension."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[Any]:
        result: list[Any] = []
        self._buffer.extend(data)

        while True:
            start = self._buffer.find(b"\xF1\xD9")
            if start < 0:
                self._buffer.clear()
                break
            if start > 0:
                del self._buffer[:start]
            if len(self._buffer) < 8:
                break

            payload_length = int.from_bytes(self._buffer[4:6], "little")
            packet_length = 8 + payload_length
            if len(self._buffer) < packet_length:
                break

            packet = bytes(self._buffer[:packet_length])
            del self._buffer[:packet_length]
            if not self._is_checksum_valid(packet):
                continue

            if packet[2] == 0x01 and packet[3] == 0x26 and payload_length == 28:
                result.append(self._parse_pverr_payload(packet[6:-2]))
            elif packet[2] == 0x01 and packet[3] == 0x01 and payload_length == 20:
                result.append(self._parse_posecef_payload(packet[6:-2]))

        return result

    @staticmethod
    def _is_checksum_valid(packet: bytes) -> bool:
        ck_a = ck_b = 0
        for byte in packet[2:-2]:
            ck_a = (ck_a + byte) & 0xFF
            ck_b = (ck_b + ck_a) & 0xFF
        return packet[-2:] == bytes((ck_a, ck_b))

    @staticmethod
    def _parse_pverr_payload(payload: bytes) -> AllystarPVERRPacket:
        stdlat, stdlon, stdalt = unpack_from("<III", payload, 4)
        horizontal_accuracy = sqrt(stdlat * stdlat + stdlon * stdlon) / 1000.0
        vertical_accuracy = stdalt / 1000.0
        return AllystarPVERRPacket(
            horizontal_accuracy=horizontal_accuracy,
            vertical_accuracy=vertical_accuracy,
        )

    @staticmethod
    def _parse_posecef_payload(payload: bytes) -> AllystarPositionECEFPacket:
        ecef_x, ecef_y, ecef_z, accuracy = unpack_from("<iiiI", payload, 4)
        return AllystarPositionECEFPacket(
            position_ecef=ECEFCoordinate(
                x=ecef_x / 100.0,
                y=ecef_y / 100.0,
                z=ecef_z / 100.0,
            ),
            accuracy=accuracy / 100.0,
        )


class AllystarParserDecorator:
    """Runs the regular GPS parser and Allystar binary parser on the same data."""

    def __init__(self, parser: Callable[[bytes], Iterable[Any]]) -> None:
        self._parser = parser
        self._allystar_parser = AllystarBinaryParser()

    def __call__(self, data: bytes) -> list[Any]:
        return [*self._parser(data), *self._allystar_parser.feed(data)]


class AllystarRTKBaseConfigurator:
    """Configurator for Allystar receivers using the binary CFG protocol."""

    #: NMEA messages that are not needed on an RTK correction stream.
    _NMEA_MESSAGES: Sequence[int] = (
        0x00,  # GGA
        0x01,  # GLL
        0x02,  # GSA
        0x03,  # GRS
        0x04,  # GSV
        0x05,  # RMC
        0x06,  # VTG
        0x07,  # ZDA
        0x20,  # TXT
    )

    #: RTCM3 ephemeris output message identifiers published in the Allystar
    #: protocol specification.
    _EPHEMERIS_MESSAGES: Sequence[tuple[GNSSType, int]] = (
        (GNSSType.GPS, 0x13),  # RTCM3 1019
        (GNSSType.GLONASS, 0x14),  # RTCM3 1020
        (GNSSType.BEIDOU, 0x2A),  # RTCM3 1042
        (GNSSType.QZSS, 0x2C),  # RTCM3 1044
        (GNSSType.GALILEO, 0x2D),  # RTCM3 1046
    )

    #: RTCM3 output message identifiers published in the Allystar protocol
    #: specification. MSM4/MSM5 output IDs are listed as TBD there, so we use
    #: MSM7 whenever the server asks for high precision.
    _MSM7_MESSAGES: Sequence[tuple[GNSSType, int]] = (
        (GNSSType.GPS, 0x4D),  # RTCM3 1077
        (GNSSType.GLONASS, 0x57),  # RTCM3 1087
        (GNSSType.GALILEO, 0x61),  # RTCM3 1097
        (GNSSType.QZSS, 0x75),  # RTCM3 1117
        (GNSSType.BEIDOU, 0x7F),  # RTCM3 1127
    )

    def __init__(
        self, settings: RTKSurveySettings, *, enable_moving_base_messages: bool = False
    ):
        self.settings = settings
        self.enable_moving_base_messages = enable_moving_base_messages

    async def run(
        self,
        write: Callable[[bytes], Any],
        sleep: Callable[[float], Any],
    ) -> None:
        for packet in self._create_configuration_packets():
            await write(packet)
            await sleep(0.1)

    def _create_configuration_packets(self) -> list[bytes]:
        settings = self.settings
        packets: list[bytes] = self._create_nmea_disable_packets()
        # Minimum 4 satellites, maximum 32 satellites.
        packets.append(self._create_packet(0x06, 0x11, pack("<BB", 4, 32)))
        packets.append(
            self._create_packet(0x06, 0x01, pack("<BBB", 0x01, 0x01, 1))
        )  # NAV-POSECEF
        packets.append(
            self._create_packet(0x06, 0x01, pack("<BBB", 0x01, 0x26, 1))
        )  # NAV-PVERR

        if settings.position is not None:
            packets.append(self._create_fixed_ecef_packet(settings.position))
        else:
            duration = max(int(round(settings.duration)), 1)
            accuracy_mm = max(int(round(settings.accuracy * 1000)), 1)
            packets.append(
                self._create_packet(0x06, 0x12, pack("<II", duration, accuracy_mm))
            )

        packets.extend(self._create_rtcm_output_packets())
        return packets

    def _create_fixed_ecef_packet(self, position: ECEFCoordinate) -> bytes:
        return self._create_packet(
            0x06,
            0x14,
            pack(
                "<iii",
                int(round(position.x * 100)),
                int(round(position.y * 100)),
                int(round(position.z * 100)),
            ),
        )

    def _create_rtcm_output_packets(self) -> list[bytes]:
        settings = self.settings
        packets = [
            self._create_message_rate_packet(0x05, 5),  # RTCM3 1005
        ]
        if self.enable_moving_base_messages:
            packets.append(
                self._create_message_rate_packet(0x41, 1)  # Allystar 4065/0 PVT
            )

        for gnss_type, message_id in self._EPHEMERIS_MESSAGES:
            packets.append(
                self._create_message_rate_packet(
                    message_id, 10 if settings.uses_gnss(gnss_type) else 0
                )
            )

        if settings.message_set is not RTKMessageSet.MSM7:
            return packets

        for gnss_type, message_id in self._MSM7_MESSAGES:
            packets.append(
                self._create_message_rate_packet(
                    message_id, 1 if settings.uses_gnss(gnss_type) else 0
                )
            )

        return packets

    def _create_nmea_disable_packets(self) -> list[bytes]:
        return [
            self._create_packet(0x06, 0x01, pack("<BBB", 0xF0, message_id, 0))
            for message_id in self._NMEA_MESSAGES
        ]

    def _create_message_rate_packet(self, message_id: int, period: int) -> bytes:
        return self._create_packet(0x06, 0x01, pack("<BBB", 0xF8, message_id, period))

    @staticmethod
    def _create_packet(class_id: int, subclass_id: int, payload: bytes = b"") -> bytes:
        packet = bytearray([0xF1, 0xD9, class_id, subclass_id])
        packet.extend(pack("<H", len(payload)))
        packet.extend(payload)

        ck_a = ck_b = 0
        for byte in packet[2:]:
            ck_a = (ck_a + byte) & 0xFF
            ck_b = (ck_b + ck_a) & 0xFF
        packet.extend([ck_a, ck_b])
        return bytes(packet)


def distance_between_ecef_coordinates(
    first: ECEFCoordinate, second: ECEFCoordinate
) -> float:
    """Returns the distance between two ECEF coordinates in meters."""

    return sqrt(
        (first.x - second.x) ** 2
        + (first.y - second.y) ** 2
        + (first.z - second.z) ** 2
    )


def average_ecef_coordinates(coordinates: Sequence[ECEFCoordinate]) -> ECEFCoordinate:
    """Returns the average of the given ECEF coordinates."""

    if not coordinates:
        raise ValueError("cannot average an empty coordinate list")

    count = len(coordinates)
    return ECEFCoordinate(
        x=sum(coord.x for coord in coordinates) / count,
        y=sum(coord.y for coord in coordinates) / count,
        z=sum(coord.z for coord in coordinates) / count,
    )
