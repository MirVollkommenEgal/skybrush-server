"""Raw RTCM and decision recording plus deterministic replay helpers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from struct import Struct
from typing import BinaryIO, Iterator

from .model import GuardStatus


_HEADER = Struct(">dI")


class GuardRecorder:
    def __init__(self, directory: str | Path) -> None:
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        self._primary = (path / "primary.rtcm").open("ab")
        self._backup = (path / "backup.rtcm").open("ab")
        self._decisions = (path / "decisions.jsonl").open("a", encoding="utf-8")

    def close(self) -> None:
        self._primary.close()
        self._backup.close()
        self._decisions.close()

    def record_packet(self, source: str, data: bytes, timestamp: float) -> None:
        stream = self._primary if source == "primary" else self._backup
        stream.write(_HEADER.pack(timestamp, len(data)))
        stream.write(data)
        stream.flush()

    def record_status(self, status: GuardStatus) -> None:
        value = status.json
        value["recordedAt"] = datetime.now(timezone.utc).isoformat()
        self._decisions.write(json.dumps(value, separators=(",", ":")) + "\n")
        self._decisions.flush()


def read_recording(stream: BinaryIO) -> Iterator[tuple[float, bytes]]:
    while True:
        header = stream.read(_HEADER.size)
        if not header:
            return
        if len(header) != _HEADER.size:
            raise ValueError("truncated RTK recording header")
        timestamp, length = _HEADER.unpack(header)
        data = stream.read(length)
        if len(data) != length:
            raise ValueError("truncated RTK recording packet")
        yield timestamp, data
