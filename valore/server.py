"""On-demand, loopback-only MJPEG server for the Valore doorbell."""

import asyncio
import io
import json
import logging
import math
import os
import re
from secrets import token_urlsafe
import signal
import time
from collections import deque
from pathlib import Path

import aiohttp
from aiohttp import web
from PIL import Image
from tuya_ipc_p2p_sdk.models import StreamConfig
from tuya_mobile import TuyaMobileApp, get_mobile_app_profile

from doorbell_codec import DoorbellAudioDecoder
from ha_bridge import (
    AppConfigurationError, HomeAssistant, INGRESS_BIND, WS_TOKEN, check_token,
    ingress_base, only_supervisor,
)
from ha_notify import watch_rings
from live_audio import LiveAudio
from talkback import talk_websocket
from transport import DeviceAnswerTimeout, ValoreSession, require_verified_mqtt
from tuya_login import MobileClient, discover_device, get_mqtt_identity

COOLDOWN = 20
IDLE_TIMEOUT = 10
START_TIMEOUT = 75
FRAME_TIMEOUT = 15
CLOSE_TIMEOUT = 15
MAX_RECONNECTS = 2
RETRYABLE_ENDS = {
    "relay_eof", "relay_closed", "relay_error", "first_frame_timeout", "frame_stall",
}
NO_CACHE = {"Cache-Control": "no-store"}
RELAY_EOF = re.compile(r"(\d+) bytes read on a total of (\d+) expected bytes")
PAGE = Path(__file__).with_name("index.html")
AUDIO_SCRIPT = PAGE.with_name("audio.js")


def session_result(reason):
    if reason.startswith("relay closed"):
        match = RELAY_EOF.search(reason)
        if match:
            partial, expected = map(int, match.groups())
            return {"end": "relay_eof", "partial_bytes": partial, "expected_bytes": expected}
        return {"end": "relay_closed" if reason == "relay closed" else "relay_error"}
    if reason.startswith("device disconnect"):
        match = re.search(r"close_reason=(\d+)$", reason)
        return {"end": "device_disconnect", "device_close_reason": int(match[1]) if match else None}
    return {"end": "session_closed"}


class ValoreCamera:
    def __init__(self, open_session, battery=None):
        self.open_session = open_session
        self.audio = LiveAudio()
        self.active_session = None
        self.battery = battery
        self.task = None
        self.idle = None
        self.viewers = 0
        self.state = "idle"
        self.changed = asyncio.Event()
        self.frame = None
        self.sequence = 0
        self.resolution = None
        self.cooldown_until = 0
        self.error = None
        self.reconnects = 0
        self.session_count = 0
        self.history = deque(maxlen=10)
        self.stopping = None
        self.shutdown = False
        self.reset_metrics()

    def reset_metrics(self):
        self.session_started = None
        self.first_frame_at = None
        self.last_frame_at = None
        self.session_frames = 0
        self.frame_times = deque(maxlen=240)

    def notify(self):
        self.changed.set()
        self.changed = asyncio.Event()

    def set_state(self, state):
        self.state = state
        self.notify()

    def on_frame(self, frame):
        if self.state not in ("connecting", "streaming"):
            return
        if not (frame.startswith(b"\xff\xd8\xff") and frame.endswith(b"\xff\xd9")):
            return
        now = time.monotonic()
        if self.first_frame_at is None:
            try:
                with Image.open(io.BytesIO(frame)) as image:
                    image.load()
                    self.resolution = image.size
            except (OSError, ValueError):
                return
            self.first_frame_at = now
        self.last_frame_at = now
        self.frame_times.append(now)
        self.session_frames += 1
        self.sequence += 1
        self.frame = frame
        self.set_state("streaming")

    def status(self):
        now = time.monotonic()
        recent = [stamp for stamp in self.frame_times if now - stamp <= 5]
        speaker = getattr(self.active_session, "speaker", None)
        fps = (len(recent) - 1) / max(recent[-1] - recent[0], 0.001) if len(recent) > 1 else 0
        return {
            "state": self.state,
            "viewers": self.viewers,
            "fps": round(fps, 2) if self.state == "streaming" else 0,
            "resolution": self.resolution,
            "frames": self.sequence,
            "battery": self.battery,
            "codec": "MJPEG",
            "transport": "Tuya P2P v4 / KCP / TCP Relay",
            "error": self.error,
            "reconnect_attempts": self.reconnects,
            "retry_in_seconds": self.retry_in,
            "audio": {"chunks": self.audio.chunks, "sample_rate": self.audio.sample_rate},
            "talk_frames": speaker.frames if speaker is not None else 0,
            "talk": speaker.status() if speaker is not None else None,
            "current_session": self.session_metrics(now) if self.session_started is not None else None,
            "recent_sessions": list(self.history),
        }

    def session_metrics(self, ended):
        first, last = self.first_frame_at, self.last_frame_at
        media_seconds = last - first if first is not None else 0
        return {
            "session": self.session_count,
            "duration_seconds": round(ended - self.session_started, 2),
            "first_frame_seconds": round(first - self.session_started, 2) if first is not None else None,
            "media_seconds": round(media_seconds, 2),
            "frames": self.session_frames,
            "average_fps": round((self.session_frames - 1) / media_seconds, 2) if media_seconds else 0,
        }

    async def wait_for_end(self, stream):
        closed = asyncio.create_task(stream.async_wait_closed())
        try:
            deadline = time.monotonic() + FRAME_TIMEOUT
            while True:
                if closed.done():
                    return session_result(closed.result())
                done, _ = await asyncio.wait({closed}, timeout=max(0, deadline - time.monotonic()))
                if done:
                    return session_result(closed.result())
                if self.last_frame_at is None:
                    return {"end": "first_frame_timeout"}
                deadline = self.last_frame_at + FRAME_TIMEOUT
                if deadline <= time.monotonic():
                    return {"end": "frame_stall"}
        finally:
            closed.cancel()
            await asyncio.gather(closed, return_exceptions=True)

    async def run_attempt(self):
        self.reset_metrics()
        self.session_started = time.monotonic()
        self.session_count += 1
        self.set_state("connecting")
        stream = None
        result = {"end": "setup_error"}
        try:
            async with asyncio.timeout(START_TIMEOUT):
                stream = await self.open_session(self.on_frame)
                self.active_session = stream
                await stream.async_start()
            result = await self.wait_for_end(stream)
        except asyncio.CancelledError:
            result = {"end": self.stopping or "cancelled"}
            raise
        except DeviceAnswerTimeout:
            result = {"end": "no_device_answer"}
        except TimeoutError:
            result = {"end": "setup_timeout"}
        except Exception as exc:
            result = {"end": "setup_error", "error_type": type(exc).__name__}
        finally:
            metrics = self.session_metrics(time.monotonic())
            self.active_session = None
            self.frame = None
            self.audio.clear()
            self.set_state("closing")
            if stream is not None:
                try:
                    async with asyncio.timeout(CLOSE_TIMEOUT):
                        await stream.async_close()
                except Exception as exc:
                    result["cleanup_error"] = type(exc).__name__
                probe = getattr(stream, "probe", None)
                if probe is not None:
                    result["direct_udp"] = probe.summary()
                speaker = getattr(stream, "speaker", None)
                if speaker is not None:
                    result["talk"] = speaker.status()
            self.history.appendleft(metrics | result)
            logging.info("%s", json.dumps(self.history[0]))
            self.session_started = None
            self.cooldown_until = time.monotonic() + COOLDOWN
        return result

    async def run(self):
        try:
            for attempt in range(MAX_RECONNECTS + 1):
                if self.shutdown or not self.viewers:
                    break
                self.reconnects = attempt
                result = await self.run_attempt()
                self.error = result["end"]
                if self.shutdown or result.get("cleanup_error") or not self.viewers or attempt == MAX_RECONNECTS:
                    break
                if self.error not in RETRYABLE_ENDS:
                    break
                self.set_state("backoff")
                await asyncio.sleep(max(0, self.cooldown_until - time.monotonic()))
        finally:
            self.audio.close()
            self.set_state("idle")

    @property
    def retry_in(self):
        return max(0, math.ceil(self.cooldown_until - time.monotonic()))

    def acquire(self):
        closing = self.state == "closing" or (self.task is not None and self.task.cancelling())
        if self.shutdown or closing or (self.state == "idle" and self.retry_in):
            raise web.HTTPServiceUnavailable(
                text="Camera unavailable. Retry shortly.",
                headers={"Retry-After": str(self.retry_in or COOLDOWN)},
            )
        if self.idle is not None:
            self.idle.cancel()
            self.idle = None
        if self.task is None or self.task.done():
            self.error = None
            self.set_state("connecting")
            self.task = asyncio.create_task(self.run())
        self.viewers += 1

    def release(self):
        self.viewers -= 1
        if self.viewers == 0 and self.task is not None and not self.task.done() and not self.shutdown:
            if self.state == "backoff":
                self.task.cancel()
            elif self.state != "closing":
                self.idle = asyncio.create_task(self.stop_when_idle())

    async def stop_when_idle(self):
        await asyncio.sleep(IDLE_TIMEOUT)
        self.idle = None
        if not self.viewers:
            await self.stop("idle_timeout")

    async def stop(self, reason):
        if reason == "shutdown":
            self.shutdown = True
            if self.idle is not None:
                self.idle.cancel()
                self.idle = None
        if self.task is not None and not self.task.done():
            if self.state != "closing" and not self.task.cancelling():
                self.stopping = reason
                self.task.cancel()
            await asyncio.shield(asyncio.gather(self.task, return_exceptions=True))
            self.stopping = None

    async def wait_frame(self, after=-1):
        while True:
            if self.frame is not None and self.sequence > after:
                return self.sequence, self.frame
            if self.state == "idle" or self.shutdown:
                raise web.HTTPServiceUnavailable(text="No live video available")
            await self.changed.wait()


def create_app(camera, ha):
    async def stream(request):
        camera.acquire()
        response = None
        try:
            sequence, frame = await camera.wait_frame()
            response = web.StreamResponse(headers={
                **NO_CACHE,
                "Content-Type": "multipart/x-mixed-replace; boundary=frame",
            })
            await response.prepare(request)
            try:
                while True:
                    header = f"--frame\r\nContent-Type: image/jpeg\r\nContent-Length: {len(frame)}\r\n\r\n"
                    await response.write(header.encode() + frame + b"\r\n")
                    sequence, frame = await camera.wait_frame(sequence)
            except web.HTTPServiceUnavailable:
                await response.write(b"--frame--\r\n")
            return response
        except ConnectionResetError:
            return response if response is not None else web.Response(status=204)
        finally:
            camera.release()

    async def snapshot(request):
        camera.acquire()
        try:
            async with asyncio.timeout(START_TIMEOUT + FRAME_TIMEOUT):
                _, frame = await camera.wait_frame(after=camera.sequence)
        except TimeoutError:
            raise web.HTTPServiceUnavailable(text="No fresh snapshot available") from None
        finally:
            camera.release()
        return web.Response(body=frame, content_type="image/jpeg", headers=NO_CACHE)

    async def status(request):
        return web.json_response(camera.status(), headers=NO_CACHE)

    async def index(request):
        page = PAGE.read_text(encoding="utf-8")
        page = page.replace("__VALORE_BASE__", ingress_base(request))
        page = page.replace("__VALORE_TOKEN__", request.app[WS_TOKEN])
        page = page.replace("__VALORE_NOTIFY_HIDDEN__", "" if ha.notify_service else "hidden")
        return web.Response(
            text=page, content_type="text/html", charset="utf-8",
            headers={**NO_CACHE, "Referrer-Policy": "no-referrer"},
        )

    async def notification_test(request):
        check_token(request, request.headers.get("X-Valore-Token"))
        try:
            await ha.notify("Zkušební oznámení z aplikace Valore.")
        except (aiohttp.ClientError, RuntimeError, TimeoutError) as exc:
            logging.warning("Zkušební oznámení selhalo (%s).", type(exc).__name__)
            raise web.HTTPServiceUnavailable() from None
        return web.Response(status=204)

    async def audio(request):
        return await camera.audio.websocket(request, camera)

    async def audio_script(request):
        return web.FileResponse(AUDIO_SCRIPT)

    async def talk(request):
        return await talk_websocket(request, camera)

    async def talk_script(request):
        return web.FileResponse(
            PAGE.with_name(request.match_info["name"]),
            headers={"Content-Type": "text/javascript; charset=utf-8"},
        )

    async def cleanup(app):
        await camera.stop("shutdown")

    app = web.Application(middlewares=[only_supervisor])
    app[WS_TOKEN] = token_urlsafe(32)
    app.router.add_get("/", index)
    app.router.add_get("/stream.mjpg", stream, allow_head=False)
    app.router.add_get("/snapshot.jpg", snapshot, allow_head=False)
    app.router.add_post("/notification-test", notification_test)
    app.router.add_get("/status", status)
    app.router.add_get("/audio", audio, allow_head=False)
    app.router.add_get("/audio.js", audio_script)
    app.router.add_get("/talk", talk, allow_head=False)
    app.router.add_get("/{name:talk\\.js|microphone\\.js}", talk_script)
    app.on_shutdown.append(cleanup)
    return app


async def serve():
    require_verified_mqtt()
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        raise AppConfigurationError("Tento balíček spouštěj jako aplikaci v Home Assistant OS.")

    timeout = aiohttp.ClientTimeout(total=35)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        ha = HomeAssistant(http, token)
        info = await ha.app_info()
        options = info["options"]
        port = info["ingress_port"]
        if type(port) is not int or not 62000 <= port <= 65535:
            raise AppConfigurationError("Supervisor nepřiřadil port pro panel Valore.")
        email = options.get("tuya_email", "").strip()
        password = options.get("tuya_password", "")
        if not email or not password:
            raise AppConfigurationError("V nastavení aplikace Valore vyplň tuya_email a tuya_password.")
        country = options.get("country_code", "420")
        device_id = (options.get("device_id") or "").strip()
        ha.set_notify_service((options.get("notify_service") or "").strip())

        profile = get_mobile_app_profile(TuyaMobileApp.TUYA_SMART)
        mobile = MobileClient(profile, http, username=email, request_timeout=30, max_login_attempts=1)
        login = await mobile.login_with_password(password, country)
        del password, options, info
        identity = get_mqtt_identity(mobile, login)
        device_id, local_key = await discover_device(mobile, device_id)

        battery = None
        try:
            status = await mobile.get_device_status(device_id=device_id, gateway_id=device_id)
            battery = status.get("result", status).get("dps", {}).get("145")
        except Exception:
            logging.info("Stav baterie není dostupný.")

        async def open_session(on_frame):
            response = await mobile._call("m.ipc.v4.rtc.config.get", {"devId": device_id}, version="1.0")
            if not response.get("success"):
                raise RuntimeError("RTC configuration unavailable")
            config = StreamConfig.from_json(response["result"], device_id, local_key)
            decoder = DoorbellAudioDecoder()

            def on_audio(record):
                if (pcm := decoder.decode(record)) is not None:
                    camera.audio.publish(*pcm)

            return ValoreSession(config, identity, login.uid, on_frame, on_audio)

        camera = ValoreCamera(open_session, battery)
        runner = web.AppRunner(create_app(camera, ha), access_log=None, handler_cancellation=True)
        await runner.setup()
        stopped = asyncio.Event()
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stopped.set)
        watcher = None
        try:
            await web.TCPSite(runner, INGRESS_BIND, port).start()
            watcher = asyncio.create_task(watch_rings(mobile, device_id, ha.deliver_ring))
            logging.info("Valore je připraven v postranním panelu Home Assistantu.")
            await stopped.wait()
        finally:
            if watcher is not None:
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)
            await runner.cleanup()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    logging.getLogger("tuya_ipc_p2p_sdk").setLevel(logging.CRITICAL)
    logging.getLogger("tuya_mobile").setLevel(logging.CRITICAL)
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        return
    except AppConfigurationError as exc:
        raise SystemExit(str(exc)) from None
    except Exception as exc:
        raise SystemExit(f"Valore se nepodařilo spustit ({type(exc).__name__}).") from None


if __name__ == "__main__":
    main()
