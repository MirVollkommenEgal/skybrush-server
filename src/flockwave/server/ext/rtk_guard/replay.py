"""Deterministic replay of timestamped primary and backup RTCM recordings."""

from __future__ import annotations

from heapq import merge
from pathlib import Path

from .decoder import RTCMObservationDecoder
from .recorder import read_recording
from .state_machine import RTKGuardEngine


def replay(directory: str | Path, engine: RTKGuardEngine):
    path = Path(directory)
    primary_decoder = RTCMObservationDecoder()
    backup_decoder = RTCMObservationDecoder()
    with (path / "primary.rtcm").open("rb") as primary, (
        path / "backup.rtcm"
    ).open("rb") as backup:
        events = merge(
            ((timestamp, 0, data) for timestamp, data in read_recording(primary)),
            ((timestamp, 1, data) for timestamp, data in read_recording(backup)),
        )
        for timestamp, source, data in events:
            decoder = primary_decoder if source == 0 else backup_decoder
            for packet in decoder.feed(data):
                if source == 0:
                    engine.feed_primary(packet, timestamp)
                else:
                    engine.feed_backup(packet, timestamp)
            yield engine.tick(timestamp, engine.config.primary.preset_id)
