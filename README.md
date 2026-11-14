# Iran Shadow Fleet Tracker

An open-source intelligence project that follows tankers the U.S. government has
sanctioned for moving Iranian oil, and maps where they are heard.

## What is here

| Path | What it is |
|---|---|
| `index.html` | The public page: live map, watchlist, breakdowns, method |
| `data/vessels.json` | The watchlist: 30 designated tankers and 2 delisted, transcribed from Treasury and State Department releases |
| `data/sources.json` | The government documents the watchlist comes from |
| `data/positions.json` | Positions heard so far, written by the listener |
| `scripts/listen.py` | Listens to the aisstream.io AIS feed for watchlist vessels |
| `.github/workflows/listen.yml` | Runs the listener every two hours and commits what it heard |

## How the map works

Ships broadcast their identity and position over AIS. The listener connects to
[aisstream.io](https://aisstream.io), a free feed, for about 25 minutes every
two hours. It recognises a watchlist vessel by the IMO number in its broadcast,
because the IMO number stays the same when a ship is renamed or reflagged. Once
it learns which radio identity (MMSI) a vessel is using, it also follows that
vessel's position reports.

## Reading it honestly

- **Silence is not proof.** The free feed relies on shore stations, so a ship in
  open ocean is not heard even with its transponder on. A vessel "not heard" may
  be dark, or may simply be out of range.
- **Positions can be faked.** Shadow fleet vessels are known to broadcast false
  positions and false identities. A plotted point is a claim the ship made.
- **Sampling.** The listener runs about a fifth of the time, so short gaps in a
  track are the listener's, not the ship's.
- **Allegations.** Every watchlist entry reflects a U.S. government designation,
  not an independent finding. Flags and owners are as stated on the designation
  date.

## Setup

1. Create a free API key at aisstream.io.
2. Add it to this repository as an Actions secret named `AISSTREAM_API_KEY`.
3. The listener then runs on its own. To run it at once, open the Actions tab,
   choose "Listen for watchlist vessels", and press "Run workflow".

The key is read from the secret at run time and is never stored in the repository.

## Test

```
python tests/test_listen.py
```
