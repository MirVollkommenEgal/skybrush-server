"""u-blox configuration helpers for USB RTK bases and guided surveys.

Protocol reference: u-blox X20 HPG 2.10 Interface Description,
UBXDOC-304424225-21263, sections UBX-NAV-HPPOSECEF and CFG-TMODE.
"""

from collections.abc import Sequence
from struct import pack
from typing import Any, Callable

from trio import Event, TooSlowError, fail_after

from flockwave.gps.enums import GNSSType
from flockwave.gps.rtk import RTKMessageSet
from flockwave.gps.ubx.packet import UBXPacket
from flockwave.gps.ubx.rtk_config import UBXRTKBaseConfigurator


class UBXAcknowledgementTracker:
    """Wait for each VALSET before issuing the next mode transition."""

    def __init__(self):
        self._event = Event()
        self._pending = False
        self._acknowledged = False

    def notify(self, packet: UBXPacket) -> None:
        if (
            self._pending
            and packet.class_id == 0x05
            and packet.subclass_id in (0x00, 0x01)
            and packet.payload == b"\x06\x8a"
        ):
            self._acknowledged = packet.subclass_id == 0x01
            self._event.set()

    async def send(self, packet: bytes, write: Callable, timeout: float = 3) -> None:
        self._event = Event()
        self._acknowledged = False
        self._pending = True
        try:
            with fail_after(timeout):
                await write(packet)
                await self._event.wait()
            if not self._acknowledged:
                raise RuntimeError("u-blox CFG-VALSET rejected with ACK-NAK")
        except TooSlowError:
            raise RuntimeError("u-blox CFG-VALSET was not acknowledged") from None
        finally:
            self._pending = False


class UBXRTKBaseConfiguratorWithValset(UBXRTKBaseConfigurator):
    """U-blox RTK base configurator with Generation 9/10 VALSET support.

    Newer u-blox receivers, including X20-class devices, may reject legacy
    CFG-MSG and CFG-TMODE3 commands. The base class still handles older M8/F9
    receivers, then this class sends the equivalent configuration database
    updates for receivers that require UBX-CFG-VALSET.
    """

    async def run(
        self,
        write: Callable[[bytes], Any],
        sleep: Callable[[float], Any],
    ):
        await super().run(write, sleep)
        await sleep(0.2)
        await write(self._create_valset_message(self._create_valset_items()))

    async def configure_rover(
        self, write: Callable, acknowledgements: UBXAcknowledgementTracker
    ) -> None:
        """Disable time mode and RTCM output before injecting corrections.

        Deliberately bypass the legacy base configurator: it starts survey-in
        and enables RTCM output, which is unsuitable for assisted positioning.
        All settings are volatile (RAM only).
        """
        await acknowledgements.send(
            self._create_valset_message(
                [
                    (0x10770001, "L001", 1),  # CFG-USBINPROT-UBX
                    (0x10780001, "L001", 1),  # CFG-USBOUTPROT-UBX (ACKs)
                    (0x10770004, "L001", 1),  # CFG-USBINPROT-RTCM3X
                    (0x10780004, "L001", 0),  # CFG-USBOUTPROT-RTCM3X
                    (0x20030001, "E001", 0),  # CFG-TMODE-MODE, disabled = rover
                ]
            ),
            write,
        )
        await acknowledgements.send(
            self._create_valset_message(
                [
                    (0x30210001, "U002", 1000),  # CFG-RATE-MEAS
                    (0x30210002, "U002", 1),  # CFG-RATE-NAV
                    (0x20110021, "E001", 2),  # CFG-NAVSPG-DYNMODEL, stationary
                    (0x20910009, "U001", 1),  # CFG-MSGOUT-UBX_NAV_PVT_USB
                    (0x20910031, "U001", 1),  # CFG-MSGOUT-UBX_NAV_HPPOSECEF_USB
                    (0x2091008B, "U001", 0),  # CFG-MSGOUT-UBX_NAV_SVIN_USB
                ]
            ),
            write,
        )

    async def configure_fixed(
        self, write: Callable, acknowledgements: UBXAcknowledgementTracker
    ) -> None:
        """Load averaged ECEF coordinates, confirm fixed mode, then enable RTCM."""
        if self.settings.position is None:
            raise ValueError("A guided base setup requires fixed coordinates")
        items = self._create_valset_items()
        # Output stays disabled until the coordinate and mode update is ACKed.
        items = [item for item in items if item[0] not in (0x10780004, 0x2091008B)]
        await acknowledgements.send(self._create_valset_message(items), write)
        await acknowledgements.send(
            self._create_valset_message([(0x10780004, "L001", 1)]), write
        )

    def _create_valset_items(self) -> list[tuple[int, str, int]]:
        settings = self.settings
        use_high_precision = settings.message_set is RTKMessageSet.MSM7

        items: list[tuple[int, str, int]] = [
            (0x10770001, "L001", 1),  # CFG-USBINPROT-UBX
            (0x10780001, "L001", 1),  # CFG-USBOUTPROT-UBX
            (0x10780004, "L001", 1),  # CFG-USBOUTPROT-RTCM3X
            (0x30210001, "U002", 1000),  # CFG-RATE-MEAS, 1 Hz
            (0x20110021, "E001", 2),  # CFG-NAVSPG-DYNMODEL, stationary
            (0x2091005E, "U001", 5),  # CFG-MSGOUT-UBX_NAV_TIMEUTC_USB
            (0x2091008B, "U001", 1),  # CFG-MSGOUT-UBX_NAV_SVIN_USB
            (0x209102C0, "U001", 5),  # CFG-MSGOUT-RTCM_3X_TYPE1005_USB
        ]

        for gnss_type, msm4_key, msm7_key in (
            (GNSSType.GPS, 0x20910361, 0x209102CF),
            (GNSSType.GLONASS, 0x20910366, 0x209102D4),
            (GNSSType.GALILEO, 0x2091036B, 0x2091031B),
            (GNSSType.BEIDOU, 0x20910370, 0x209102D9),
        ):
            enabled = settings.uses_gnss(gnss_type)
            items.append(
                (msm4_key, "U001", 0 if use_high_precision or not enabled else 1)
            )
            items.append((msm7_key, "U001", 1 if use_high_precision and enabled else 0))

        items.append(
            (
                0x20910306,  # CFG-MSGOUT-RTCM_3X_TYPE1230_USB
                "U001",
                5 if settings.uses_gnss(GNSSType.GLONASS) else 0,
            )
        )

        position = settings.position
        if position is not None:
            position_in_one_tenth_of_mm = position * 10000
            coords_in_one_tenth_of_mm = (
                int(round(position_in_one_tenth_of_mm.x)),
                int(round(position_in_one_tenth_of_mm.y)),
                int(round(position_in_one_tenth_of_mm.z)),
            )
            coords, coords_hp = zip(
                *(divmod(coord, 100) for coord in coords_in_one_tenth_of_mm)
            )
            items.extend(
                [
                    (0x20030002, "E001", 0),  # CFG-TMODE-POS_TYPE, ECEF
                    (0x40030003, "I004", coords[0]),  # CFG-TMODE-ECEF_X
                    (0x40030004, "I004", coords[1]),  # CFG-TMODE-ECEF_Y
                    (0x40030005, "I004", coords[2]),  # CFG-TMODE-ECEF_Z
                    (0x20030006, "I001", coords_hp[0]),  # CFG-TMODE-ECEF_X_HP
                    (0x20030007, "I001", coords_hp[1]),  # CFG-TMODE-ECEF_Y_HP
                    (0x20030008, "I001", coords_hp[2]),  # CFG-TMODE-ECEF_Z_HP
                    (
                        0x4003000F,  # CFG-TMODE-FIXED_POS_ACC
                        "U004",
                        max(int(round(settings.accuracy * 10000)), 1),
                    ),
                    (0x20030001, "E001", 2),  # CFG-TMODE-MODE, fixed
                ]
            )
        else:
            items.extend(
                [
                    (
                        0x40030010,  # CFG-TMODE-SVIN_MIN_DUR
                        "U004",
                        max(int(round(settings.duration)), 1),
                    ),
                    (
                        0x40030011,  # CFG-TMODE-SVIN_ACC_LIMIT
                        "U004",
                        max(int(round(settings.accuracy * 10000)), 1),
                    ),
                    (0x20030001, "E001", 1),  # CFG-TMODE-MODE, survey-in
                ]
            )

        return items

    @staticmethod
    def _create_valset_message(items: Sequence[tuple[int, str, int]]) -> bytes:
        payload = bytearray([0, 1, 0, 0])  # version 0, RAM layer only

        for key, type_, value in items:
            payload.extend(pack("<I", key))
            if type_ in ("E001", "L001", "U001"):
                payload.extend(pack("<B", value))
            elif type_ == "I001":
                payload.extend(pack("<b", value))
            elif type_ == "U002":
                payload.extend(pack("<H", value))
            elif type_ == "I004":
                payload.extend(pack("<i", value))
            elif type_ == "U004":
                payload.extend(pack("<I", value))
            else:
                raise ValueError(f"Unsupported UBX configuration type: {type_!r}")

        header = bytearray([0xB5, 0x62, 0x06, 0x8A])
        header.extend(pack("<H", len(payload)))
        packet = header + payload
        ck_a = ck_b = 0
        for byte in packet[2:]:
            ck_a = (ck_a + byte) & 0xFF
            ck_b = (ck_b + ck_a) & 0xFF
        packet.extend([ck_a, ck_b])
        return bytes(packet)
