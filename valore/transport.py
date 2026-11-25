"""Adapters for the installed Tuya P2P SDK 0.1.3."""

import asyncio
import hmac
import ssl
from collections import Counter
from importlib.metadata import version
from ipaddress import ip_address, ip_network

from tuya_ipc_p2p_sdk.control import parse_control
from tuya_ipc_p2p_sdk.crypto import decrypt_record
from tuya_ipc_p2p_sdk.exceptions import TuyaIpcP2pError, TuyaIpcP2pSessionError
from tuya_ipc_p2p_sdk.signaling import moto_client
from tuya_ipc_p2p_sdk.stream_session import StreamSession
from tuya_ipc_p2p_sdk.transport import IceResponder
from tuya_ipc_p2p_sdk.transport.kcp_segment import KCP_HEADER_LENGTH, parse_segment
from tuya_ipc_p2p_sdk.transport.relay_framing import MEDIA_TAG_LENGTH, media_tag

from talkback import Speaker

LAN_NETWORKS = tuple(ip_network(net) for net in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"))


class DeviceAnswerTimeout(TimeoutError):
    """The signaling broker connected but the doorbell never answered."""


def require_verified_mqtt():
    if version("tuya-ipc-p2p-sdk") != "0.1.3":
        raise RuntimeError("Tato verze serveru vyžaduje tuya-ipc-p2p-sdk 0.1.3.")
    # The SDK reads this context on every connect. Change it only in this process.
    moto_client._UNVERIFIED_TLS = ssl.create_default_context()


class UdpProbe(IceResponder):
    """Count direct KCP packets and test a trailing HMAC without using them as media."""

    def __init__(self, password, candidate, keys):
        super().__init__(password, candidate)
        self.keys = keys
        self.candidates = Counter()
        self.authenticated = Counter()
        self.stun_datagrams = 0
        self.lan_datagrams = 0

    def datagram_received(self, data, addr):
        super().datagram_received(data, addr)
        peer = ip_address(addr[0])
        if not any(peer in network for network in LAN_NETWORKS):
            return
        self.lan_datagrams += 1
        if data[4:8] == b"\x21\x12\xa4\x42":
            self.stun_datagrams += 1
            return
        if len(data) < KCP_HEADER_LENGTH:
            return
        segment = parse_segment(data)
        if segment is None or segment.conversation not in (0, 1, 2) or segment.command not in range(0x51, 0x55):
            return
        end = KCP_HEADER_LENGTH + len(segment.data)
        if len(data) not in (end, end + MEDIA_TAG_LENGTH):
            return
        self.candidates[segment.conversation] += 1
        if len(data) == end + MEDIA_TAG_LENGTH:
            body, tag = data[:end], data[end:]
            for key in self.keys():
                if key and hmac.compare_digest(media_tag(key, body), tag):
                    self.authenticated[segment.conversation] += 1
                    break

    def summary(self):
        return {
            "lan_datagrams": self.lan_datagrams,
            "stun_datagrams": self.stun_datagrams,
            "kcp_candidates": dict(self.candidates),
            "hmac_verified": dict(self.authenticated),
        }


class ValoreSession(StreamSession):
    probe = None

    def __init__(self, config, identity, uid, on_frame, on_audio=None):
        super().__init__(config, identity, uid, on_frame)
        self.on_audio = on_audio
        self.speaker = None

    def _send_channel_zero(self, send_key, receive_key):
        relay = self._require_relay()
        self.speaker = Speaker(relay, send_key)
        if self.on_audio is not None:
            audio = relay._conversation(2)
            audio.set_message_handler(lambda record: self._on_audio_record(record, receive_key))
        super()._send_channel_zero(send_key, receive_key)

    def _on_control_record(self, record, receive_key):
        if self.speaker is not None:
            try:
                self.speaker.on_control(parse_control(decrypt_record(receive_key, record)))
            except TuyaIpcP2pError:
                pass
        super()._on_control_record(record, receive_key)

    async def async_close(self):
        owner = self.speaker.owner if self.speaker is not None else None
        if owner is not None:
            self.speaker.end(owner)
        await super().async_close()
        if owner is not None:
            await owner.close(drain=False)

    def _on_audio_record(self, record, receive_key):
        try:
            plaintext = decrypt_record(receive_key, record)
        except TuyaIpcP2pError:
            return
        self.on_audio(plaintext)

    async def _async_gather_candidates(self, ice_password):
        self.probe = UdpProbe(ice_password, self._on_local_candidate, self._udp_keys)
        self._ice = self.probe
        await self.probe.async_gather()

    def _udp_keys(self):
        return self._config.p2p_session.aes_key, self._answer_key

    async def _async_await_answer(self):
        try:
            return await super()._async_await_answer()
        except TuyaIpcP2pSessionError as exc:
            if isinstance(exc.__cause__, TimeoutError):
                raise DeviceAnswerTimeout from exc
            raise

    async def async_start(self):
        try:
            await super().async_start()
        except asyncio.CancelledError:
            # SDK cancels its video-ready future when the relay closes during setup.
            if asyncio.current_task().cancelling() or not self._ended.done():
                raise
