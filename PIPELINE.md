# Lympik → Teamworks AMS pipeline

The concrete, current pipeline lives in `run_pipeline.py` (steps 1-6 below). It
supersedes the earlier, more abstract 6-step plan this doc used to describe.

## Status

| Step | Status |
|---|---|
| 1. Get recent event IDs from Lympik | Done — `lympik_activity.py: get_recent_event_ids()` |
| 2. Pull existing "Lympik Event" entries from Teamworks | Done — `teamworks_client.py: TeamworksClient.find_existing_event_ids()` |
| 3. Get unique Event IDs already in Teamworks | Done — same call, see below |
| 4. Get all Teamworks athletes | Done — `teamworks_client.py: TeamworksClient.list_athletes()` |
| 5. Filter to (event, athlete) pairs not yet uploaded | Done — `run_pipeline.py: run()` |
| 6. Per event: pull data, build per-athlete runs, match, upload | Done — `run_pipeline.py` |

Everything under "Done" was exercised against mock HTTP servers seeded with the
real sample payloads you provided (`event_detail.json`, `event_results.json`,
`profile_activity_timing.json`) — not against live credentials. Testing this way
caught several real bugs before they shipped: see "Bugs found during testing"
below.

## Step 1: recent event IDs — solved

`/profile/{pId}/activity/search?dateFrom=...&dataType=timing` was the missing
discovery endpoint from earlier investigation — not in the published OpenAPI spec,
reachable via the `activity.search` key scope. `lympik_client.py`'s
`search_activity()` calls it; `lympik_activity.py`'s `get_recent_event_ids()`
returns the deduplicated event ids from the response. Only `dateFrom` is used
(no upper bound) — confirmed this always looks forward from that point to now.

## Steps 2-3: which events are already in Teamworks — solved

This used to be a stopgap: a local JSON ledger file tracking what *this script*
had uploaded, since no query endpoint was known. That's gone now. Teamworks
itself is the source of truth, queried fresh every run via
`TeamworksClient.find_existing_event_ids()` (`POST /api/v1/synchronise`):
confirmed working shape from a sibling Teamworks AMS integration (different
org, same v1 API family) — request `{"formName", "startDate", "userIds"}`,
paginated via `{"pagination": {"paginate": True, "cursor": ...}}` on every page
after the first, response events under `body["export"]["events"]`.

Each returned event's row-0 pairs are checked for our own `"Event ID"` field
(the same field every upload writes into row 0) to recover the Lympik event id,
and `event["userId"]` for which athlete it belongs to — giving back a set of
`(event_id, teamworks_user_id)` pairs already present, which `run()` filters
`all_payloads` against before uploading.

**Not yet confirmed against this specific AMS instance** — only against a
different org's. Every call dumps its raw response to
`debug_payloads/synchronise_response.json`, and `find_existing_event_ids()`
logs a warning if events come back but none parse into a recognized shape, so
a mismatch is visible immediately instead of silently uploading duplicates.
Confirm this against a real run before trusting it fully; if the shape turns
out to differ, the fix is isolated to `_extract_field_value()` /
`_event_user_id()` in `teamworks_client.py`.

This also removes the GitHub Actions ephemeral-disk risk the ledger used to
carry (see README) — there's no local state to lose between runs anymore.

## Step 4: all Teamworks athletes — solved

`TeamworksClient.list_athletes()` — cursor-paginated `/api/v1/usersynchronise`.

## Steps 5-6: per-event processing and upload — solved

1. `GET /event/{eId}` → `Event ID` / `Session Name` / `Location` / `startedAt unix`
   fields, shared by every athlete's entry for this event, plus the `module`
   value that drives step 2.
2. The event's `module` picks the group endpoint, via `GROUP_PATH_BY_MODULE` in
   `run_pipeline.py`: `event:timing` → `GET /event/{eId}/timing/group`,
   `event:alpine-skiing` → `GET /event/{eId}/alpine-skiing/group`. Both return
   the same `{totalCount, records}` wrapper and the same record shape
   (`id` / `startedAt` / `profile` / `status` / `totalDuration` / `invalid` plus an
   inline `edges` list), so `build_runs_dataframe()` parses either one unchanged —
   only the path segment differs. An event on any other module is logged as an
   error and skipped; there's no runs endpoint here that knows how to read it.

   Sections are read from each run's `edges` list by `sequence` (0/1/2/3/4 →
   Section 1/2/3/4/5); a run with no `sequence` match is left blank, not zero.
   The section count varies by event (timing events observed so far record 3),
   and the Teamworks form tolerates the trailing blanks. A run is marked `DNF`
   whenever `invalid` is present at all, whatever reason it names — alpine events
   use `user_dnf`, timing events `duration_limit_max`. Runs with no assigned
   athlete (`profile` null/missing) are dropped and logged, since there's no one
   to upload them against; on a timing event that's the *common* case, where most
   runs are recorded against a device `label` ("G5 AND") rather than a profile.

   `_compute_event_fastest()` separately scans the *unfiltered* run list
   (including unassigned runs) for two more row-0 fields: `Fastest Athlete` (the
   name off the fastest completed run's profile, or `"Anon"` if that run has no
   profile/name; ties go to the later `startedAt`, since a later run implies a
   rougher course and so a comparatively faster time) and `Fastest Time` (that
   run's `totalDuration`). Runs flagged `invalid` are excluded here as well as
   runs with no `totalDuration` at all: an invalid run can still carry a
   *partial* `totalDuration` covering only part of the course, which then beats
   every completed run. A timing event showed this directly — a
   `duration_limit_max` run recorded a single 12.3s section against a field of
   ~35s completed runs, and won.
3. Alpine-skiing events only: `GET /profile/{pId}/event/{eId}/alpine-skiing` →
   alpine-specific event detail, wrapped as `{"status": ..., "event": {...}}`,
   mapped into further row-0 fields: `discipline` → `api_discipline`,
   `gateCount` → `Gate Count`, `verticalDrop` → `Vertical Drop`,
   `airTemperature` → `Air Temp`, `windSpeed` → `Wind Speed`, `humidity` →
   `Humidity`, `snowTemperature` → `Snow Temp`. A timing event has no such
   record — requesting it there 404s — so those fields stay blank. The call is
   made *after* the runs table is built, so an event with nothing to upload
   can't fail on metadata it never needed.
4. Athletes within the event are grouped **by name** (`firstName`+`lastName`),
   matched against Teamworks via `athlete_matching.match_athletes()`'s
   full-name → last-name → first-initial → full-first-name cascade (an exact
   first+last match is taken immediately when it's unique; only an
   unresolved/absent full-name match falls through to the last-name-first
   cascade). Unmatched athletes are logged as errors (with event id) and
   skipped, never guessed.
5. Each matched athlete's runs are sorted earliest→latest, numbered into
   `Run #` (1-indexed), and built into one event payload — event-level fields
   in `row: 0`, one table row per run after that.
6. **All** athlete payloads across **every** event in the run are collected
   first, filtered against Teamworks' own existing entries (see Steps 2-3
   above), sorted oldest-first, and submitted together via
   `TeamworksClient.bulk_import_events()` (`POST /api/v1/eventsimport`),
   batched (default 25/batch, per Teamworks' own sample). This endpoint is
   all-or-nothing per batch — one malformed event fails the whole batch with
   no indication which one — so a batch failure automatically retries every
   event in it individually to isolate the cause; confirmed by test (see
   below).
7. Every matched athlete's built payload is also written to
   `debug_payloads/{event_id}__{teamworks_user_id}.json` (gitignored),
   alongside the raw Lympik data it was built from and a note on where each
   field came from. Written on every run, win or lose — a debugging aid for
   tracing a wrong value or column back to its source, not part of the
   upload path itself.

## Bugs found during testing

Caught by testing against mock servers seeded with your real sample data,
before any of these shipped:

1. **Grouping runs by Lympik profile id instead of name.** Safer in theory
   against two *different* athletes sharing a name — but your sample data
   showed the same athlete under two slightly different profile-id strings
   across runs within one event, which split that athlete into two separate
   uploads for the same session (exactly the duplicate problem this pipeline
   exists to prevent). Reverted to grouping by name (`firstName`+`lastName`),
   per the original spec. Tradeoff, for awareness: two genuinely different
   athletes sharing an exact name within one event would now merge — flag this
   if that's a real risk for your rosters.
2. **(Historical -- the ledger this describes no longer exists, see Steps
   2-3 above.)  Ledgering by whole event instead of per athlete.** A
   batch-submission test simulated one athlete's upload failing while
   another in the *same* event succeeded. With a whole-event ledger, the
   failed athlete blocked the entire event from being marked done — so the
   next run would have retried the whole event, **re-uploading the athlete
   who had already succeeded** as a second, duplicate entry (there's no
   `existingEventId` tracking to turn that retry into an update instead).
   Fixed at the time by ledgering per `(event_id, teamworks_user_id)` pair;
   moot now that duplicate detection is a live Teamworks query keyed the
   same way, per athlete, on every run.
3. **A lone last-name candidate skipped first-name verification entirely.**
   `athlete_matching.match_athletes()`'s narrowing loop only ran when the
   last-name pool had *more than one* candidate — if only one Teamworks
   athlete shared a last name, it was accepted immediately, without ever
   comparing first names. In production this matched a nonexistent Lympik
   athlete ("RTS2 USSS", no Teamworks record) straight onto the one
   Teamworks athlete who happened to share a last name ("RTS1 USSS"),
   uploading RTS2's runs as a second, wrongly-attributed entry under RTS1's
   profile. Fixed: a lone last-name candidate is now always checked against
   the Lympik first name over however many letters both names have, and
   rejected (left unmatched) if they disagree — confirmed by test.
4. **(Historical -- moot now that the ledger is gone.) A batch reporting
   success while missing a lost upload could vanish from the ledger on a
   crash.** The ledger used to be written once, after every result in a run
   was logged; an exception partway through that logging (or the process
   dying) meant already-successful uploads were never recorded, so the next
   run would re-upload them as duplicates. Fixed at the time by writing each
   success immediately, one at a time; no longer relevant since there's no
   ledger to write to.

## Open questions before running this for real

1. **Confirm `/api/v1/synchronise`'s response shape against this specific AMS
   instance** (see Steps 2-3 above) — it's only confirmed against a
   different org so far. Check `debug_payloads/synchronise_response.json`
   after a real run and watch for the "none matched a known event id/user
   id shape" warning; if the shape differs, the fix is isolated to
   `_extract_field_value()` / `_event_user_id()` in `teamworks_client.py`.
2. ~~Timezone~~ — resolved: always Mountain Time. `PIPELINE_TIMEZONE` env var
   defaults to `America/Denver` (handles MST/MDT automatically).
