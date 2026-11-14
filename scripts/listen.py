#!/usr/bin/env python3
"""Listen to aisstream.io for the watchlist vessels and record what is heard.

How it works
- Connection A subscribes to ShipStaticData for the whole world. Those messages
  carry the IMO number, which survives renaming and reflagging, so a watchlist
  vessel is recognised by IMO whatever MMSI it is using today.
- Connection B subscribes to position reports for the MMSIs learned so far,
  which fills in the track between static messages.
- Everything heard is merged into data/positions.json.

The API key is read from the AISSTREAM_API_KEY environment variable and is
never written to disk or printed.
"""
import asyncio
import datetime as dt
import json
import math
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VESSELS = ROOT / "data" / "vessels.json"
POSITIONS = ROOT / "data" / "positions.json"
URL = "wss://stream.aisstream.io/v0/stream"
WORLD = [[[-90, -180], [90, 180]]]
MIN_MOVE_KM = 2.0        # skip points closer than this to the last one...
MIN_GAP_MIN = 60         # ...unless this much time has passed
MAX_TRACK = 3000         # points kept per vessel
MAX_RUNS = 120           # run-log entries kept
MAX_MMSI_FILTER = 50     # aisstream's per-subscription MMSI filter limit


def now():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def km(a_lat, a_lon, b_lat, b_lon):
    r = 6371.0
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    dp, dl = p2 - p1, math.radians(b_lon - a_lon)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


def pick(d, *names):
    """Read a key whatever its capitalisation (the feed is not consistent)."""
    low = {str(k).lower(): v for k, v in (d or {}).items()}
    for n in names:
        if n.lower() in low:
            return low[n.lower()]
    return None


def clean(s):
    return str(s or "").replace("@", " ").strip()[:60]


def valid_pos(lat, lon):
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
        return None
    return round(lat, 4), round(lon, 4)


def load_watch():
    rows = json.loads(VESSELS.read_text())
    return {int(r["imo"]): r for r in rows if r.get("status") == "Designated"}


def load_state():
    if POSITIONS.exists():
        s = json.loads(POSITIONS.read_text())
    else:
        s = {}
    s.setdefault("runs", [])
    s.setdefault("vessels", {})
    return s


def add_point(rec, t, pos, sog, src):
    """Append a track point unless it barely differs from the last one."""
    track = rec.setdefault("track", [])
    if track:
        last = track[-1]
        age = (t - dt.datetime.strptime(last["t"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)).total_seconds() / 60
        if km(last["lat"], last["lon"], pos[0], pos[1]) < MIN_MOVE_KM and age < MIN_GAP_MIN:
            rec["last_heard"] = iso(t)
            return False
    point = {"t": iso(t), "lat": pos[0], "lon": pos[1], "src": src}
    if sog is not None:
        try:
            point["sog"] = round(float(sog), 1)
        except (TypeError, ValueError):
            pass
    track.append(point)
    del track[:-MAX_TRACK]
    rec["last_heard"] = iso(t)
    return True


def handle(msg, state, watch, mmsi_to_imo, t=None):
    """Merge one feed message into state. Returns the IMO matched, or None."""
    t = t or now()
    kind = msg.get("MessageType")
    meta = msg.get("MetaData") or {}
    mmsi = pick(meta, "MMSI")
    pos = valid_pos(pick(meta, "latitude"), pick(meta, "longitude"))
    body = (msg.get("Message") or {}).get(kind) or {}
    if kind == "ShipStaticData":
        try:
            imo = int(pick(body, "ImoNumber") or 0)
        except (TypeError, ValueError):
            return None
        if imo > 0:
            # Diagnostic: proves the feed's IMO field is being read at all.
            state["_imo_seen"] = state.get("_imo_seen", 0) + 1
        if imo not in watch:
            return None
        rec = state["vessels"].setdefault(str(imo), {})
        if mmsi is not None:
            seen = rec.setdefault("mmsi_seen", [])
            if mmsi not in seen:
                seen.append(mmsi)
            rec["mmsi"] = mmsi
            mmsi_to_imo[int(mmsi)] = imo
        name = clean(pick(body, "Name") or pick(meta, "ShipName"))
        if name:
            names = rec.setdefault("names_seen", [])
            if name not in names:
                names.append(name)
            rec["name_broadcast"] = name
        dest = clean(pick(body, "Destination"))
        if dest:
            rec["destination"] = dest
        draught = pick(body, "MaximumStaticDraught")
        if isinstance(draught, (int, float)) and draught > 0:
            rec["draught_m"] = round(float(draught), 1)
        rec["static_heard"] = iso(t)
        if pos:
            add_point(rec, t, pos, None, "static")
        return imo
    if mmsi is not None and int(mmsi) in mmsi_to_imo and pos:
        imo = mmsi_to_imo[int(mmsi)]
        rec = state["vessels"].setdefault(str(imo), {})
        add_point(rec, t, pos, pick(body, "Sog"), "position")
        return imo
    return None


async def stream(sub, on_msg, deadline, counter, stop=None):
    import websockets
    delay = 2
    while now() < deadline and not (stop and stop.is_set()):
        try:
            async with websockets.connect(URL, max_size=2 ** 22, open_timeout=30) as ws:
                await ws.send(json.dumps(sub))
                delay = 2
                while now() < deadline and not (stop and stop.is_set()):
                    left = (deadline - now()).total_seconds()
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(1, min(30, left)))
                    except asyncio.TimeoutError:
                        continue
                    data = json.loads(raw)
                    if "error" in data or "Error" in data:
                        print("feed error:", str(data)[:200], file=sys.stderr)
                        counter["errors"] += 1
                        break
                    counter["messages"] += 1
                    on_msg(data)
        except Exception as e:  # network drops are routine: reconnect
            counter["errors"] += 1
            print("connection problem:", type(e).__name__, str(e)[:120], file=sys.stderr)
        await asyncio.sleep(min(delay, 30))
        delay *= 2


async def main():
    key = os.environ.get("AISSTREAM_API_KEY", "").strip()
    if not key:
        print("AISSTREAM_API_KEY is not set. Add it as a repository secret.", file=sys.stderr)
        return 1
    seconds = int(os.environ.get("LISTEN_SECONDS", "1500"))
    watch = load_watch()
    state = load_state()
    mmsi_to_imo = {}
    for imo, rec in state["vessels"].items():
        if int(imo) in watch:
            for m in rec.get("mmsi_seen", []):
                mmsi_to_imo[int(m)] = int(imo)
    started = now()
    deadline = started + dt.timedelta(seconds=seconds)
    heard = set()
    c_static = {"messages": 0, "errors": 0}
    c_pos = {"messages": 0, "errors": 0}

    def on_msg(m):
        imo = handle(m, state, watch, mmsi_to_imo)
        if imo:
            heard.add(imo)

    async def positions():
        # Re-subscribe whenever a new MMSI is learned from the static feed.
        while now() < deadline:
            known = sorted(mmsi_to_imo)[:MAX_MMSI_FILTER]
            if not known:
                await asyncio.sleep(20)
                continue
            stop = asyncio.Event()

            async def watch_for_new(n=len(mmsi_to_imo)):
                while now() < deadline and len(mmsi_to_imo) == n:
                    await asyncio.sleep(15)
                stop.set()

            sub = {"APIKey": key, "BoundingBoxes": WORLD,
                   "FiltersShipMMSI": [str(m) for m in known],
                   "FilterMessageTypes": ["PositionReport", "StandardClassBPositionReport"]}
            await asyncio.gather(stream(sub, on_msg, deadline, c_pos, stop), watch_for_new())

    static_sub = {"APIKey": key, "BoundingBoxes": WORLD, "FilterMessageTypes": ["ShipStaticData"]}
    await asyncio.gather(stream(static_sub, on_msg, deadline, c_static), positions())

    state["generated"] = iso(now())
    state["runs"].append({"started": iso(started), "seconds": seconds,
                          "static_messages": c_static["messages"],
                          "static_with_imo": state.pop("_imo_seen", 0),
                          "position_messages": c_pos["messages"],
                          "vessels_heard": len(heard),
                          "errors": c_static["errors"] + c_pos["errors"]})
    del state["runs"][:-MAX_RUNS]
    POSITIONS.write_text(json.dumps(state, indent=1) + "\n")
    print(f"listened {seconds}s: {c_static['messages']} static, {c_pos['messages']} position messages, "
          f"{len(heard)} watchlist vessels heard")
    # A run that received nothing at all usually means a bad key or a feed outage.
    return 2 if c_static["messages"] == 0 else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
