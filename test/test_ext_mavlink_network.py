from unittest.mock import Mock

from flockwave.server.ext.mavlink.comm import Channel
from flockwave.server.ext.mavlink.network import MAVLinkNetwork


CHANNELS = [1500] * 18


def test_targeted_rc_override_is_broadcast_with_target_system() -> None:
    network = MAVLinkNetwork("test")
    network.manager = Mock()
    uav = Mock(id="45")
    network._uavs[45] = uav

    assert network.enqueue_rc_override_packet_for_uav("45", CHANNELS)

    packet = network.manager.enqueue_broadcast_packet.call_args.args[0]
    assert packet[0] == "RC_CHANNELS_OVERRIDE"
    assert packet[1]["target_system"] == 45
    network.manager.enqueue_broadcast_packet.assert_called_once_with(
        packet, destination=Channel.RC, allow_failure=True
    )
    network.manager.enqueue_packet.assert_not_called()


def test_targeted_rc_override_is_not_sent_when_uav_is_unknown() -> None:
    network = MAVLinkNetwork("test")
    network.manager = Mock()

    assert not network.enqueue_rc_override_packet_for_uav("45", CHANNELS)

    network.manager.enqueue_packet.assert_not_called()
    network.manager.enqueue_broadcast_packet.assert_not_called()


def test_untargeted_rc_override_remains_a_broadcast() -> None:
    network = MAVLinkNetwork("test")
    network.manager = Mock()

    network.enqueue_rc_override_packet(CHANNELS)

    packet = network.manager.enqueue_broadcast_packet.call_args.args[0]
    assert packet[0] == "RC_CHANNELS_OVERRIDE"
    assert packet[1]["target_system"] == 0
    network.manager.enqueue_broadcast_packet.assert_called_once_with(
        packet, destination=Channel.RC, allow_failure=True
    )
    network.manager.enqueue_packet.assert_not_called()
