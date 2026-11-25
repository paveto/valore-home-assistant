"""The authenticated Ingress boundary and the Home Assistant API client."""

import asyncio
import hmac
import re

from aiohttp import web

SUPERVISOR_IP = "172.30.32.2"
INGRESS_BIND = "172.30.32.1"
INGRESS_BASE = re.compile(r"/api/hassio_ingress/[A-Za-z0-9_-]+/?")
NOTIFY_SERVICE = re.compile(r"(?:notify\.)?(mobile_app_[a-z0-9_]+)")
WS_TOKEN = web.AppKey("valore_ws_token", str)


class AppConfigurationError(ValueError):
    """A validation failure whose message is safe to show."""


@web.middleware
async def only_supervisor(request, handler):
    # Use the TCP peer. The forwarded client IP is not the trusted proxy.
    peer = request.transport.get_extra_info("peername") if request.transport else None
    if not peer or peer[0] != SUPERVISOR_IP:
        raise web.HTTPForbidden()
    return await handler(request)


def check_token(request, token):
    if not token or not token.isascii() or not hmac.compare_digest(token, request.app[WS_TOKEN]):
        raise web.HTTPForbidden()


def check_socket(request):
    check_token(request, request.query.get("token"))


def ingress_base(request):
    prefix = request.headers.get("X-Ingress-Path", "")
    if INGRESS_BASE.fullmatch(prefix):
        return prefix.rstrip("/") + "/"
    return "./"


class HomeAssistant:
    def __init__(self, http, token):
        self.http = http
        self.headers = {"Authorization": f"Bearer {token}"}
        self.notify_service = None
        self.event_id = None
        self.event_published = False

    async def app_info(self):
        async with self.http.get(
            "http://supervisor/addons/self/info",
            headers=self.headers,
            allow_redirects=False,
        ) as response:
            if response.status != 200:
                raise RuntimeError("Supervisor options unavailable")
            return (await response.json())["data"]

    def set_notify_service(self, value):
        if not value:
            return
        match = NOTIFY_SERVICE.fullmatch(value)
        if not match:
            raise AppConfigurationError("notify_service musí být akce notify.mobile_app_…")
        self.notify_service = match[1]

    async def _post(self, path, data):
        async with asyncio.timeout(8):
            async with self.http.post(
                f"http://supervisor/core/api/{path}",
                headers=self.headers,
                json=data,
                allow_redirects=False,
            ) as response:
                if not 200 <= response.status < 300:
                    raise RuntimeError(f"Home Assistant API returned HTTP {response.status}")

    async def notify(self, message):
        if not self.notify_service:
            raise RuntimeError("Home Assistant mobile notification is not configured")
        await self._post(
            f"services/notify/{self.notify_service}",
            {"title": "Valore", "message": message},
        )

    async def deliver_ring(self, event):
        identity = event["event_id"]
        if self.event_id != identity:
            self.event_id = identity
            self.event_published = False
        if not self.event_published:
            await self._post("events/valore_ring", event)
            self.event_published = True
        if self.notify_service:
            await self.notify("Někdo zazvonil u dveří.")
