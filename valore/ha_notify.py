"""Send verified Valore ring events to Home Assistant."""

import asyncio
from collections import deque
from hashlib import sha256
import json
import logging
import time

POLL_INTERVAL = 20
MAX_BACKOFF = 120
EVENT_TTL = 180
LOG = logging.getLogger(__name__)


def ring_events(rows, device_id, earliest):
    events = {}
    for row in rows:
        if not isinstance(row, dict) or row.get("msgSrcId") != device_id:
            continue
        if row.get("msgCode") != "ipc_doorbell":
            continue
        raw_id = row.get("idStr") or row.get("id")
        raw_time = row.get("time")
        if type(raw_time) not in (int, str) or not str(raw_time).isdigit():
            continue
        if type(raw_id) not in (int, str) or not str(raw_id).strip() or len(str(raw_id)) > 128:
            continue
        if len(str(raw_time)) not in (10, 13):
            continue
        stamp = int(raw_time)
        if stamp >= 10**12:
            stamp //= 1000
        if stamp < earliest or stamp > int(time.time()) + 15:
            continue
        identity = sha256(f"{device_id}:{raw_id}".encode()).hexdigest()
        events[identity] = {"event": "ring", "event_id": identity, "timestamp": stamp}
    return sorted(events.values(), key=lambda event: (event["timestamp"], event["event_id"]))


async def fetch_rings(mobile, device_id, earliest):
    query = {"limit": 20, "offset": 0, "bigMsgType": 1, "msgSrcId": device_id}
    response = await mobile._call(
        "tuya.m.msg.list.by.json",
        {"json": json.dumps(query, separators=(",", ":"))},
        version="1.0",
    )
    result = response.get("result")
    if not response.get("success") or not isinstance(result, dict) or not isinstance(result.get("datas"), list):
        raise RuntimeError("Tuya did not return an alarm page")
    return ring_events(result["datas"], device_id, earliest)


async def watch_rings(mobile, device_id, deliver):
    started = int(time.time())
    pending = deque()
    known = {}
    delay = POLL_INTERVAL
    LOG.info("Home Assistant: sledování stisku zvonku každých %s s.", POLL_INTERVAL)
    while True:
        cutoff = max(started, int(time.time()) - EVENT_TTL)
        known = {key: stamp for key, stamp in known.items() if stamp >= cutoff}
        try:
            for event in await fetch_rings(mobile, device_id, cutoff):
                identity = event["event_id"]
                if identity not in known:
                    known[identity] = event["timestamp"]
                    pending.append(event)
            delay = POLL_INTERVAL
        except Exception as exc:
            LOG.warning("Home Assistant: čtení zpráv Tuya selhalo (%s).", type(exc).__name__)
            delay = min(delay * 2, MAX_BACKOFF)

        while pending:
            event = pending[0]
            if event["timestamp"] >= int(time.time()) - EVENT_TTL:
                try:
                    await deliver(event)
                except Exception as exc:
                    LOG.warning("Home Assistant: doručení zatím selhalo (%s).", type(exc).__name__)
                    break
                LOG.info("Home Assistant: událost zazvonění odeslána.")
            pending.popleft()
        await asyncio.sleep(delay)
