from functools import partial
from types import SimpleNamespace
from typing import cast

import pytest
from trio import sleep

from flockwave.server.ext.mavlink.ftp import (
    MAVFTP,
    MAVFTPMessage,
    MAVFTPOpCode,
    MAVFTPSession,
    _MAVFTP_CHUNK_SIZE,
)
from flockwave.server.ext.mavlink.types import MAVLinkMessage, UAVBoundPacketSenderFn


class RecordingSession:
    def __init__(self) -> None:
        self.active_write_count = 0
        self.maximum_active_write_count = 0
        self.writes: list[tuple[int, bytes]] = []

    async def write(self, data: bytes, offset: int = 0) -> int:
        self.active_write_count += 1
        self.maximum_active_write_count = max(
            self.maximum_active_write_count, self.active_write_count
        )
        try:
            await sleep(0)
            self.writes.append((offset, data))
            return len(data)
        finally:
            self.active_write_count -= 1


class FailingSession(RecordingSession):
    async def write(self, data: bytes, offset: int = 0) -> int:
        if offset == 0:
            raise RuntimeError("simulated write failure")
        return await super().write(data, offset)


class AcknowledgingPacketSender:
    def __init__(self) -> None:
        self.active_request_count = 0
        self.maximum_active_request_count = 0
        self.request_sequence_numbers: list[int] = []

    async def __call__(self, message, *, wait_for_response):
        payload = message[1]["payload"]
        request_sequence_number = payload[0] + (payload[1] << 8)
        request = MAVFTPMessage.decode(payload)
        self.request_sequence_numbers.append(request_sequence_number)
        request_index = len(self.request_sequence_numbers) - 1

        self.active_request_count += 1
        self.maximum_active_request_count = max(
            self.maximum_active_request_count, self.active_request_count
        )
        try:
            # Later requests complete first to exercise out-of-order ACK routing.
            await sleep((4 - request_index) / 1000)
            reply = SimpleNamespace(
                payload=MAVFTPMessage(
                    MAVFTPOpCode.ACK,
                    session_id=request.session_id,
                    offset=request.offset,
                )
                .encode((request_sequence_number + 1) % 65536)
                .ljust(251, b"\x00")
            )
            assert wait_for_response[1](cast(MAVLinkMessage, reply))
            return reply
        finally:
            self.active_request_count -= 1


async def test_mavftp_upload_pipelines_write_chunks() -> None:
    session = RecordingSession()
    data = bytes(range(256)) * 7

    acknowledged = [
        offset
        async for offset in MAVFTP._write_chunks_with_window(
            cast(MAVFTPSession, session), data, window_size=5
        )
    ]

    assert session.maximum_active_write_count == 5
    assert acknowledged == [5 * _MAVFTP_CHUNK_SIZE, len(data)]
    assert b"".join(chunk for _, chunk in sorted(session.writes)) == data
    assert [offset for offset, _ in sorted(session.writes)] == list(
        range(0, len(data), _MAVFTP_CHUNK_SIZE)
    )


async def test_mavftp_upload_rejects_empty_write_window() -> None:
    session = RecordingSession()

    with pytest.raises(ValueError, match="window_size must be positive"):
        async for _ in MAVFTP._write_chunks_with_window(
            cast(MAVFTPSession, session), b"firmware", window_size=0
        ):
            pass


async def test_mavftp_upload_propagates_write_error_without_exception_group() -> None:
    session = FailingSession()

    with pytest.raises(RuntimeError, match="simulated write failure"):
        async for _ in MAVFTP._write_chunks_with_window(
            cast(MAVFTPSession, session), bytes(2 * _MAVFTP_CHUNK_SIZE)
        ):
            pass


async def test_mavftp_pipeline_routes_out_of_order_acknowledgements() -> None:
    sender = AcknowledgingPacketSender()
    ftp = MAVFTP(cast(UAVBoundPacketSenderFn, sender))
    session = MAVFTPSession(
        23, partial(ftp._send_and_wait, use_shared_retry_policy=False)
    )
    data = bytes(5 * _MAVFTP_CHUNK_SIZE)

    acknowledged = [
        offset
        async for offset in ftp._write_chunks_with_window(session, data)
    ]

    assert acknowledged == [len(data)]
    assert sender.maximum_active_request_count == 5
    assert len(set(sender.request_sequence_numbers)) == 5
    assert all(
        (second - first) % 65536 == 2
        for first, second in zip(
            sender.request_sequence_numbers, sender.request_sequence_numbers[1:]
        )
    )
