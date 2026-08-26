from io import BytesIO

from flockwave.server.ext.rtk_guard.recorder import _HEADER, read_recording


def test_recording_reader_preserves_timestamp_and_bytes():
    data = b"rtcm"
    stream = BytesIO(_HEADER.pack(12.5, len(data)) + data)
    assert list(read_recording(stream)) == [(12.5, data)]


def test_recording_reader_rejects_truncated_data():
    stream = BytesIO(_HEADER.pack(12.5, 5) + b"bad")
    try:
        list(read_recording(stream))
    except ValueError as ex:
        assert "truncated" in str(ex)
    else:
        raise AssertionError("truncated recording accepted")
