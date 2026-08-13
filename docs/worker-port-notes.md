# Port notes: `usss-lympic` → `usss-lympik-worker`

Handoff for applying this branch's changes to the worker version of the
Lympik → Teamworks AMS pipeline.

**Source branch**: `claude/lympik-alpine-skiing-404-3vngw0` in
`USSS-High-Performance/usss-lympic`
**Branched from**: `cef53c5` (*"Add Fastest Athlete and Fastest Time event-wide
fields to row 0"*) — so that commit and everything before it is already shared
history, not part of this port.
**12 commits**, 14 files, +1388/−196.

Read section 7 (*Confirmed API behavior*) before writing code. Most of what
follows was established by probing the live AMS instance, not from published
docs — several published behaviors turned out to be wrong or incomplete, and
re-deriving them costs live writes into a production athlete database.

Everything in sections 1–6 is production-verified: the pipeline ran live against
Teamworks and updated 5 real athlete-session entries in place, then a read-back
confirmed no duplicates.

---

## 1. Dispatch run fetching on the event's `module`

**Commit** `318ee93`. **The bug this fixed was fatal, not cosmetic.**

The pipeline asked *every* event it discovered for the alpine-skiing event
detail record. But `/profile/{pId}/activity/search` finds events by `dataType`,
**not** by module — so a plain `event:timing` event comes back too, and asking
for its alpine-skiing record **404s and kills the whole event before its runs
are ever read**.

Fix: read the event's own `module` off `GET /event/{eId}` and pick the group
endpoint from it.

```python
# Which group endpoint a run list comes from, keyed by the `module` value on
# the event detail payload (GET /event/{eId}). Both modules return the same
# record shape, so one parser covers both -- only the path segment differs.
GROUP_PATH_BY_MODULE = {
    "event:timing": "timing",
    "event:alpine-skiing": "alpine-skiing",
}
```

- `event:timing` → `GET /event/{eId}/timing/group`
- `event:alpine-skiing` → `GET /event/{eId}/alpine-skiing/group`
- any other module → log an error and skip the event, don't guess

Both modules return the same `{totalCount, records}` wrapper and the same record
shape (`id` / `startedAt` / `profile` / `status` / `totalDuration` / `invalid`,
plus an inline `edges` list), so **one parser handles both** — only the path
segment differs.

Two ordering requirements that matter:

1. The alpine detail call (`GET /profile/{pId}/event/{eId}/alpine-skiing`) is
   made **only** for `event:alpine-skiing`. On a timing event the
   alpine-specific row-0 fields are left blank.
2. It's made **after** the runs table is built, so an event with nothing to
   upload can't fail on metadata it never needed.

## 2. DNF fires on any `invalid` value

Was `group.get("invalid") == "user_dnf"`, now:

```python
"DNF": group.get("invalid") is not None,
```

Alpine events use `user_dnf`; **timing events use `duration_limit_max`**. Under
the old rule a 2-section 22.53s run in a field of 3-section ~35s runs uploaded
as a clean run. Any reason code either module adds later should count the same
way, hence the `is not None` rather than a list of known values.

## 3. Fastest-run computation excludes `invalid` runs

An invalid run can still carry a **partial** `totalDuration` covering only part
of the course — which then beats every completed run outright. Observed
directly: a `duration_limit_max` run recorded a single 12.3s section against a
field of ~35s completed runs and won, so row 0 read `Anon / 12.3`.

```python
completed = [
    g for g in raw_groups
    if g.get("totalDuration") is not None and g.get("invalid") is None
]
```

Note this scans the **unfiltered** run list (including runs with no Teamworks
match), because it's a fact about the event, not about who it's uploaded for.
Ties go to the later `startedAt` — a later run means a rougher course and so a
comparatively faster time.

## 4. Upsert: update existing entries instead of skipping them

**Commit** `5af07c5`. The largest change, and the reason for the whole branch.

**Why**: a Lympik session can be read while it is still in progress, so an
event already uploaded keeps gaining runs. The old logic asked Teamworks which
`(event, athlete)` pairs already existed and **skipped** them, which froze each
entry at whatever the session looked like the first time it was seen.

### 4a. The lookup must return the Teamworks event id

`find_existing_event_ids()` → **`find_existing_events()`**. It previously
returned a bare `set` of `(event_id, user_id)` pairs — a yes/no answer — and
threw away the Teamworks event id. That id *is* `existingEventId`, so it now
has to survive.

```python
# by_pair: {(str(lympik_event_id), str(teamworks_user_id)): [teamworks event id, ...]}
#   a list because the same pair can legitimately come back more than once
#   (duplicates already in AMS from earlier manual imports)
# events_seen / events_parsed: how many events came back, and how many yielded
#   both a user id and a Teamworks event id. events_seen > 0 with
#   events_parsed == 0 means the parser is broken, which is very different from
#   "these users have no matching entries".
ExistingEvents = namedtuple("ExistingEvents", "by_pair events_seen events_parsed raw_responses")
```

Helpers:

```python
def _event_teamworks_id(event):
    """Confirmed to be `id` on this instance; the aliases cost nothing."""
    for key in ("id", "eventId", "existingEventId", "event_id"):
        value = event.get(key)
        if value is not None:
            return value
    return None


def _sorted_ids(ids):
    """Lowest first, so a pair with duplicate entries resolves to the oldest
    one deterministically. Falls back to string ordering if a response ever
    mixes id types, which would otherwise raise."""
    try:
        return sorted(ids)
    except TypeError:
        return sorted(ids, key=str)
```

### 4b. `run()` partitions into updates and creates

Nothing is skipped. For each payload, look up its `(event_id, user_id)` pair:
a hit sets `ams_event["existingEventId"] = existing_ids[0]` and counts as an
update; a miss leaves the key **absent entirely** and counts as a create.

**Re-sending athletes whose own runs didn't change is deliberate, not waste.**
Row 0's `Fastest Athlete` / `Fastest Time` are event-wide, so when any athlete
posts a new best, every *other* athlete's entry for that session is stale.

### 4c. Three guards, all serving "never duplicate an entry"

These are the load-bearing part of the change. Port all three.

**Guard 1 — abort when the lookup is broken, don't create.**

```python
if existing.events_seen and not existing.events_parsed:
    logger.error("aborting before upload: synchronise returned %d event(s) but none "
                 "could be matched to an athlete and event id ...", existing.events_seen)
    return
```

If events came back but not one could be addressed, the lookup is broken rather
than empty. Creating there would duplicate every entry in the window — and do it
again on every scheduled run. A genuinely new session just gets created on the
next run once the lookup works.

Critical distinction: this is **not** the same as "no pair matched a candidate".
An athlete legitimately has many older entries that match no current event id;
that is the normal case for a new session and must still create. The signal is
specifically *structural* — events that yield no `(userId, id)` at all.

**Guard 2 — duplicates resolve to the lowest id, and the rest are named.**

Where a pair already has several entries (from earlier manual imports), update
the lowest id and log the others by id for manual deletion. Do not auto-delete
another system's records.

**Guard 3 — verify every update's returned id.**

```python
sent_id = payload["ams_event"].get("existingEventId")
if action == "update" and str(teamworks_event_id) != str(sent_id):
    logger.error("... was sent with existingEventId=%s but Teamworks returned event %s "
                 "-- it created a duplicate instead of updating ...", sent_id, teamworks_event_id)
```

`eventsimport` reports `SUCCESSFULLY_IMPORTED` **whether or not** it honored
`existingEventId`. A returned id that differs from what was sent is the only
signal that it created a duplicate. Without this check that failure is
completely silent.

## 5. Anonymous fastest run → `Guest` / `Guest - {label}`

**Commit** `ddcc96e`. Replaces the old `"Anon"`.

```python
profile = fastest.get("profile") or {}
name_parts = [p for p in (profile.get("firstName"), profile.get("lastName")) if p]
if name_parts:
    return " ".join(name_parts), fastest["totalDuration"]

# str() before strip(): `label` is untyped in Lympik's schema, so a numeric bib
# would otherwise blow up on .strip(). Whitespace-only is treated as blank,
# since "Guest - " reads as a bug.
label = str(fastest.get("label") or "").strip()

return (f"Guest - {label}" if label else "Guest"), fastest["totalDuration"]
```

Rules:
- label present → `Guest - {label}`
- label blank, whitespace-only, or absent → `Guest`
- `label` lives on the **run/group** record, not the profile
- "anonymous" covers both no `profile` at all *and* a profile carrying no name

This matters more than it looks. On a live event, 21 of 31 completed runs were
anonymous, and the labels included real first names (`Carissa`, `Storm`,
`Caiyu`) alongside device tags (`G1`–`G4`).

## 6. `PIPELINE_DRY_RUN=1`

Does everything except the upload: reads Lympik, builds every payload, resolves
create-vs-update against live Teamworks, writes the debug dumps, logs the plan,
sends nothing. `run()` takes `dry_run=False` as a parameter; `main()` reads the
env var.

The plan also carries `Fastest Athlete` / `Fastest Time` per entry, so a dry run
shows the **values** it would write, not just which events it would touch. A dry
run that only names events can't verify a change to a field's value.

---

## 7. Confirmed API behavior — read before coding

Everything here was verified against the live `usopc.smartabase.com/athlete360-usss`
instance by a one-off probe, which wrote fake data for one athlete, read it
back, updated it, and read it back again. Where this contradicts the published
reference, **this is right and the reference is wrong**.

The probe has since been removed from `usss-lympic` (it is not pipeline code, and
a button that writes test events into a production athlete database is not worth
leaving in place). Recover it from git history if the API shifts and this table
needs re-checking: `git show 5af07c5:probe_upsert.py`.

### `POST /api/v1/synchronise` (reading events back)

Undocumented endpoint. Request:

```json
{"formName": "Lympik Event", "startDate": "13/08/2026", "userIds": [12791],
 "pagination": {"paginate": true, "cursor": "..."}}
```

- **`userIds` is mandatory.** Omitting it returns **no events for anyone**, not
  "all events". Short-circuit on an empty list rather than making a call that
  quietly means something else.
- `startDate` is `dd/MM/yyyy`, inclusive, no upper bound.
- Omit `pagination` on the first request; send it on every page after.
- Events live under `body["export"]["events"]`.
- **The AMS event id is each event's `id`** — this is what `existingEventId` needs.
- **`userId` comes back as a bare int** (`"userId": 12791`), *not* the
  `{"userId": N}` wrapper the import endpoints require. Same field name,
  different shape by direction.
- **Response rows are not request rows.** An import sends row 0 as event-level
  fields only with table rows after it; the response returns row 0 holding the
  event-level fields **merged with the first table row**, then one row per
  remaining row. A 3-run event imported as rows 0–3 reads back as rows 0–2.
  Reading `Event ID` from row 0 still works.
- Also returns `lastSynchronisationTimeOnServer` and `idsOfDeletedEvents`.
- Number fields may read back in scientific notation (`"1.78432026E9"`).

### `POST /api/v1/eventsimport` (writing)

- **`existingEventId` works on the plural endpoint**, not just the singular
  `eventimport` that documents it. Set it per event object inside `events`. An
  int works; `""` is equivalent to omitting it.
- **An update returns the same event id it replaced** — see Guard 3.
- Returns HTTP 200 on failure. Check `result.state == "SUCCESSFULLY_IMPORTED"`
  as an allowlist.
- All-or-nothing per batch, with no indication which event failed — retry a
  failed batch one event at a time to isolate it.
- Can report success while returning **fewer ids than events submitted**; treat
  an id-count mismatch as a batch failure.

### Whole-event replacement is safe — calculated fields survive

`existingEventId` replaces an event's contents rather than merging, and the
`Lympik Event` form carries a dozen fields the pipeline never sends. All of them
are **AMS-side derivations that recompute after an update**. Verified: nothing
present after a create was missing after an update.

| Field | Derived from |
|---|---|
| `Discipline` | `api_discipline` — so the pipeline already writes the right field |
| `Lympik Activity URL` | `Event ID` |
| `name_stripped` | `Session Name` |
| `Run Time` | `run_time`, formatted to 2dp |
| `Total Runs`, `Session Total Time (s)`, `# DNF`, `Session % DNF` | the run rows |
| `Date Year`, `Period`, `Period Calc Number`, `7 Day Total Time in Course` | AMS-side calcs |

So "resend full state" means *your* fields, not the form's. Also: fields sent as
`""` are **dropped from the response entirely** (e.g. unused `Section 4`/`5`).

### Fields the pipeline actually writes

Row 0: `Event ID`, `Session Name`, `Location`, `startedAt unix`,
`api_discipline`, `Gate Count`, `Vertical Drop`, `Air Temp`, `Wind Speed`,
`Humidity`, `Snow Temp`, `Fastest Athlete`, `Fastest Time`.

Rows 1..N (one per run): `Run #`, `Run ID`, `Section 1`–`Section 5`, `run_time`,
`DNF`.

Every value must be a string. Field names are case-sensitive and must match the
AMS form builder exactly. Never repeat a single-value (event-level) field across
more than one row — even blank, AMS rejects it with *"This form does not support
multiple rows for key: `<field>`"*.

---

## 8. Do NOT port

Branch scaffolding, existing only because a branch-only workflow can't be
dispatched (`workflow_dispatch` registers only for workflows already on the
default branch, so dispatching one returns 404 regardless of the ref passed —
hence push triggers gated on a touch-file):

- `.github/workflows/probe-upsert.yml`, `pipeline-dry-run.yml`,
  `pipeline-live-run.yml`
- `.probe-trigger`, `.dryrun-trigger`, `.liverun-trigger`
- `inspect_anonymous_runs.py` — one-off read-only diagnostic
  (`git show 8ce9e40:inspect_anonymous_runs.py` if the label question resurfaces)
- `probe_upsert.py` — the tool that re-verifies AMS behavior if the API shifts,
  but not pipeline code (`git show 5af07c5:probe_upsert.py`)

All of the above have been deleted from `usss-lympic` too, for the same reason —
so there is nothing here the worker should carry across.

Port the pipeline changes (sections 1–6) and the docs (section 7).

## 9. Open items

- **Lookback window is 1 day** (`time.time() - 86400`) and the upsert re-sends
  everything in it on every run. Deliberate; sessions never span more than a day.
- **A live event's fastest run looks like bad data**: 15.42s against a field of
  45–50s, with the **same 3 sections** as every other run, and *not* flagged
  `invalid` — so the section-count and `invalid` filters both miss it. It wins
  `Fastest Time` and, having no label, yields plain `Guest`. No outlier filter
  was implemented; picking a threshold is a judgment about the data. The obvious
  rule would drop a completed run below some fraction of the field median.
- **Anonymous runs are consistently faster than named ones** on the observed
  event (45.6–46.5 vs 48.2–50.3), so `Fastest Athlete` will usually name a
  guest rather than a rostered athlete. That's the data, not a bug.
- Two duplicate entries exist in AMS under Event ID `testtest` from earlier
  manual testing (`2612009`, `2677651`). Known and accepted; Guard 2 handles the
  shape.

## 10. Verification path used here

Worth repeating in the worker rather than trusting the port:

1. Unit-level: 20 assertions over the synchronise parser against the real
   response shape, pagination, duplicate resolution, the abort guard, dry run,
   and oldest-first ordering; plus 15 over the `Guest`/`Guest - {label}` rules
   (blank, whitespace-only, absent, numeric label, nameless-but-present profile).
2. Live probe with fake data on one athlete under an obvious test session name
   and a generated UUID Event ID — confirms shape and `existingEventId` without
   touching real sessions.
3. Live dry run — confirms the create/update split and the values.
4. Live run.
5. **Read back after the live run** — the definitive no-duplicates check. Same
   entry count, same ids.

Batch submissions are sorted oldest-first, per Teamworks' own guidance, to
minimize re-triggering historical calculations.
