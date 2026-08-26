from types import SimpleNamespace

from flockwave.server.ext.rtk_guard.config import SourceConfig
from flockwave.server.ext.rtk_guard.extension import RTKGuardExtension
from flockwave.server.model import FlockwaveMessage


def test_resolves_source_through_extension_api_proxy_attributes():
    extension = RTKGuardExtension()
    extension._rtk_api = SimpleNamespace(
        get_preset=lambda preset_id: SimpleNamespace(
            id=preset_id, sources=["tcp://127.0.0.1:2101"]
        )
    )

    source = extension._resolve_source(SourceConfig(preset_id="backup"))

    assert source == "tcp://127.0.0.1:2101"


def test_status_message_type_is_experimental():
    message = {
        "$fw.version": "1.0",
        "id": "test",
        "body": {"type": "X-RTK-GUARD-STAT"},
    }

    assert FlockwaveMessage.is_experimental(message)
