import datetime as dt, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import listen

T = dt.datetime(2026, 11, 14, 6, 0, tzinfo=dt.timezone.utc)
watch = {9276561: {"name": "SHENZHEN"}}

def static(imo, mmsi, lat, lon, name="NEW NAME", dest="FOR ORDERS"):
    return {"MessageType": "ShipStaticData", "MetaData": {"MMSI": mmsi, "ShipName": name, "latitude": lat, "longitude": lon},
            "Message": {"ShipStaticData": {"ImoNumber": imo, "Name": name + "@@@", "Destination": dest, "MaximumStaticDraught": 20.5}}}

def position(mmsi, lat, lon, sog=11.2):
    return {"MessageType": "PositionReport", "MetaData": {"MMSI": mmsi, "Latitude": lat, "Longitude": lon},
            "Message": {"PositionReport": {"Sog": sog}}}

def test_flow():
    state, m2i = {"vessels": {}, "runs": []}, {}
    assert listen.handle(static(1234567, 111, 10, 10), state, watch, m2i, T) is None      # not on watchlist
    assert listen.handle(position(613000001, 25, 56), state, watch, m2i, T) is None       # MMSI not yet known
    assert listen.handle(static(9276561, 613000001, 25.0, 56.0), state, watch, m2i, T) == 9276561
    rec = state["vessels"]["9276561"]
    assert rec["mmsi"] == 613000001 and rec["name_broadcast"] == "NEW NAME" and rec["draught_m"] == 20.5
    assert len(rec["track"]) == 1
    # same spot a minute later: no new point, but last_heard moves
    listen.handle(position(613000001, 25.0001, 56.0001), state, watch, m2i, T + dt.timedelta(minutes=1))
    assert len(rec["track"]) == 1 and rec["last_heard"].endswith("06:01:00Z")
    # moved 50+ km: new point
    listen.handle(position(613000001, 25.5, 56.0), state, watch, m2i, T + dt.timedelta(minutes=30))
    assert len(rec["track"]) == 2 and rec["track"][1]["sog"] == 11.2
    # "not available" coordinates are ignored
    listen.handle(position(613000001, 91, 181), state, watch, m2i, T + dt.timedelta(hours=2))
    assert len(rec["track"]) == 2
    # new MMSI for the same IMO (reflagged) is still matched
    listen.handle(static(9276561, 620999999, 26, 57), state, watch, m2i, T + dt.timedelta(hours=3))
    assert rec["mmsi_seen"] == [613000001, 620999999] and len(rec["track"]) == 3

if __name__ == "__main__":
    test_flow(); print("ok")
