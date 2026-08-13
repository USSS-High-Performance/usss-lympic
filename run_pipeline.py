"""One pass through the Lympik -> Teamworks AMS pipeline (steps 1-7).

Scheduling/interval is left to whatever runs this script (cron, a scheduled
task, etc.) -- this module is a single run.

Duplicate protection: before uploading, run() asks Teamworks itself which
(Lympik event, Teamworks athlete) pairs already have a "Lympik Event" entry,
via TeamworksClient.find_existing_event_ids() (POST /api/v1/synchronise,
matching our own "Event ID" row-0 field against the event ids this run is
about to process). This replaced an earlier local JSON ledger stopgap --
querying Teamworks directly means no local state file to lose or fall out
of sync, and it also sees entries created any other way (a manual entry, a
different script). See PIPELINE.md for why the ledger existed and how this
replaced it, and docs/teamworks-api-reference.md for the endpoint itself.
Since this endpoint's response shape isn't yet confirmed against this
specific AMS instance, every call dumps its raw response to
debug_payloads/synchronise_response.json for verification.
"""

import json
import logging
import math
import os
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from athlete_matching import match_athletes, teamworks_user_id
from lympik_activity import get_recent_event_ids
from lympik_client import LympikClient
from teamworks_client import TeamworksClient

FORM_NAME = "Lympik Event"
EVENT_ID_FIELD = "Event ID"
DEBUG_DUMP_DIR = Path("debug_payloads")

# Which group endpoint a run list comes from, keyed by the `module` value on
# the event detail payload (GET /event/{eId}). Both modules return the same
# record shape, so one parser covers both -- only the path segment differs.
# An event on any other module is skipped: there's no runs endpoint here that
# knows how to read it.
GROUP_PATH_BY_MODULE = {
    "event:timing": "timing",
    "event:alpine-skiing": "alpine-skiing",
}

RUNS_DF_COLUMNS = [
    "firstName",
    "lastName",
    "Run ID",
    "Run Start unix Time",
    "Section 1",
    "Section 2",
    "Section 3",
    "Section 4",
    "Section 5",
    "run_time",
    "DNF",
]

logger = logging.getLogger("lympik_pipeline")


def _stringify(value):
    """Every eventsimport value must be a string regardless of its real type
    (docs/teamworks-api-reference.md) -- and a missing split/run-time should
    become "" rather than the literal text "None"/"nan"."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return str(value)


def _unix_to_ams_date_time(unix_ts, tz):
    dt = datetime.fromtimestamp(unix_ts, tz=tz)
    return dt.strftime("%d/%m/%Y"), dt.strftime("%I:%M %p").lstrip("0")


def build_runs_dataframe(lympik_client, event_id, module_path):
    """/event/{eId}/{module_path}/group -> one row per run, where module_path
    is "timing" or "alpine-skiing" per the event's own `module` (see
    GROUP_PATH_BY_MODULE). Both modules return the same record shape --
    id/startedAt/profile/status/totalDuration/invalid plus an inline `edges`
    list -- so one parser covers both.

    Runs with no assigned athlete (profile null/missing) are dropped and
    logged, since there's no athlete to upload them against. On a timing
    event that's the common case rather than the exception: most runs there
    are recorded against a device `label` ("G5 AND") instead of a profile.

    A run counts as DNF whenever the API set `invalid` at all, whatever the
    reason it gave -- alpine events use `user_dnf`, timing events
    `duration_limit_max`, and any reason code either module adds later should
    count the same way.

    The number of sections varies by event (timing events seen so far record
    3), so Section columns past the last `sequence` present are left blank
    rather than zero; the Teamworks form tolerates the blanks.

    Returns (dataframe, raw_groups) -- raw_groups is the unprocessed API
    response for every run (including dropped ones), kept around purely so
    build_athlete_payloads() can include it in a debug dump; nothing in the
    upload path itself uses it."""
    groups = list(lympik_client.get_all_pages(f"/event/{event_id}/{module_path}/group"))

    rows = []
    for group in groups:
        profile = group.get("profile")
        if not profile:
            logger.warning("event %s: run %s has no assigned athlete, skipping", event_id, group.get("id"))
            continue

        splits = {edge.get("sequence"): edge.get("duration") for edge in (group.get("edges") or [])}

        rows.append(
            {
                "firstName": profile.get("firstName"),
                "lastName": profile.get("lastName"),
                "Run ID": group.get("id"),
                "Run Start unix Time": group.get("startedAt"),
                "Section 1": splits.get(0),
                "Section 2": splits.get(1),
                "Section 3": splits.get(2),
                "Section 4": splits.get(3),
                "Section 5": splits.get(4),
                "run_time": group.get("totalDuration"),
                "DNF": group.get("invalid") is not None,
            }
        )

    return pd.DataFrame(rows, columns=RUNS_DF_COLUMNS), groups


def _compute_event_fastest(raw_groups):
    """Fastest completed run across the whole event -- every run returned by
    the module's group endpoint, not just the ones with a Teamworks match,
    since this is a fact about the event, not about who it's uploaded for.

    A run is excluded if it has no totalDuration *or* if the API flagged it
    `invalid` at all: an invalid run can still carry a partial totalDuration,
    and since that partial time covers only part of the course it beats every
    completed run outright. A timing event showed this directly -- a
    `duration_limit_max` run recorded a single 12.3s section against a field
    of ~35s completed runs, and won.

    Ties go to the later startedAt: a later run means a rougher course and so
    a comparatively faster time. Returns ("Anon", time) if the fastest run's
    profile has no name (or no profile at all -- a device-labelled run), or
    (None, None) if the event has no completed runs."""
    completed = [g for g in raw_groups if g.get("totalDuration") is not None and g.get("invalid") is None]
    if not completed:
        return None, None

    fastest = min(completed, key=lambda g: (g["totalDuration"], -(g.get("startedAt") or 0)))

    profile = fastest.get("profile") or {}
    name_parts = [p for p in (profile.get("firstName"), profile.get("lastName")) if p]
    name = " ".join(name_parts) if name_parts else "Anon"

    return name, fastest["totalDuration"]


def _write_debug_payload(event_id, module, teamworks_user_id_value, lympik_profile, event, alpine_event, event_fields, raw_groups, raw_athlete_groups, athlete_runs_df, ams_event):
    """Dumps exactly what would be (or was) sent to Teamworks for this
    athlete+event, alongside the raw Lympik data it was built from and a
    note on where every field came from -- so a mismatch (wrong value,
    wrong column) can be tracked back to its source without guessing.
    Written for every matched athlete on every run, regardless of upload
    outcome; each file is overwritten by the next run that touches the same
    (event, athlete) pair, since this is a debugging aid, not a record."""
    DEBUG_DUMP_DIR.mkdir(exist_ok=True)
    group_path = GROUP_PATH_BY_MODULE.get(module, module)

    dump = {
        "_pipeline_note": (
            "Debug dump only -- not uploaded from here. Shows the exact "
            "eventsimport payload for this athlete+event plus the raw "
            "Lympik data it was built from, so a wrong value or column can "
            "be traced back to its source."
        ),
        "event_id": event_id,
        "lympik_event_module": module,
        "teamworks_user_id": teamworks_user_id_value,
        "lympik_athlete_name": f"{lympik_profile['firstName']} {lympik_profile['lastName']}",
        "raw_lympik_event": {
            "_source": f"GET /event/{event_id}",
            "data": event,
        },
        "raw_lympik_alpine_skiing_event": {
            "_source": (
                f"GET /profile/{{pId}}/event/{event_id}/alpine-skiing"
                if module == "event:alpine-skiing"
                else f"not fetched -- only event:alpine-skiing has this record, this event is {module}"
            ),
            "data": alpine_event,
        },
        "raw_lympik_runs_for_this_athlete": {
            "_source": f"GET /event/{event_id}/{group_path}/group, filtered to this athlete's name",
            "data": raw_athlete_groups,
        },
        "raw_lympik_all_runs_in_event": {
            "_source": f"GET /event/{event_id}/{group_path}/group, unfiltered -- source for Fastest Athlete/Fastest Time",
            "data": raw_groups,
        },
        "extracted_event_fields": {
            "_source": "event_fields dict in build_athlete_payloads() -- becomes row 0 of the ams_event payload",
            "field_sources": {
                "Event ID": "event['id']",
                "Session Name": "event['name']",
                "Location": "event['locationName']",
                "startedAt unix": "event['startedAt']",
                "api_discipline": "alpine_event['discipline'] -- blank unless module is event:alpine-skiing",
                "Gate Count": "alpine_event['gateCount'] -- blank unless module is event:alpine-skiing",
                "Vertical Drop": "alpine_event['verticalDrop'] -- blank unless module is event:alpine-skiing",
                "Air Temp": "alpine_event['airTemperature'] -- blank unless module is event:alpine-skiing",
                "Wind Speed": "alpine_event['windSpeed'] -- blank unless module is event:alpine-skiing",
                "Humidity": "alpine_event['humidity'] -- blank unless module is event:alpine-skiing",
                "Snow Temp": "alpine_event['snowTemperature'] -- blank unless module is event:alpine-skiing",
                "Fastest Athlete": "_compute_event_fastest(raw_groups) -- min totalDuration across all runs in the event that aren't flagged `invalid`, ties broken by later startedAt, 'Anon' if no profile name",
                "Fastest Time": "_compute_event_fastest(raw_groups) -- the winning run's totalDuration",
            },
            "data": event_fields,
        },
        "extracted_runs_table": {
            "_source": "build_runs_dataframe() -- one row per run, becomes rows 1..N of the ams_event payload",
            "field_sources": {
                "Run ID": "group['id']",
                "Run Start unix Time": "group['startedAt']",
                "Section 1-5": "group['edges'], matched by edge['sequence'] == 0/1/2/3/4, value is edge['duration'] -- blank past the last sequence the event actually recorded",
                "run_time": "group['totalDuration']",
                "DNF": "group['invalid'] is present at all, whatever reason it gives ('user_dnf', 'duration_limit_max', ...)",
            },
            "data": athlete_runs_df.to_dict("records"),
        },
        "ams_event_payload": {
            "_source": "the exact dict passed to TeamworksClient.bulk_import_events() for this athlete",
            "data": ams_event,
        },
    }

    path = DEBUG_DUMP_DIR / f"{event_id}__{teamworks_user_id_value}.json"
    path.write_text(json.dumps(dump, indent=2, default=str))


def _write_debug_synchronise_response(raw_responses, existing_pairs):
    """Dumps the raw /api/v1/synchronise response(s) used for duplicate
    detection this run, plus what we parsed out of them -- this endpoint's
    response shape is confirmed against a different AMS org, not this one,
    so this is the way to confirm/fix event_id/user_id extraction if
    find_existing_event_ids() isn't finding what it should."""
    DEBUG_DUMP_DIR.mkdir(exist_ok=True)
    dump = {
        "_pipeline_note": (
            "Debug dump only. Raw POST /api/v1/synchronise response(s) used to find "
            "which (event, athlete) pairs already exist in Teamworks this run, plus "
            "what find_existing_event_ids() parsed out of them."
        ),
        "raw_responses": raw_responses,
        "parsed_existing_pairs": sorted(existing_pairs),
    }
    (DEBUG_DUMP_DIR / "synchronise_response.json").write_text(json.dumps(dump, indent=2, default=str))


def _build_rows_payload(event_fields, athlete_runs_df):
    rows = [{"row": 0, "pairs": [{"key": k, "value": _stringify(v)} for k, v in event_fields.items()]}]

    for i, run in enumerate(athlete_runs_df.to_dict("records"), start=1):
        rows.append(
            {
                "row": i,
                "pairs": [
                    {"key": "Run #", "value": str(i)},
                    {"key": "Run ID", "value": _stringify(run["Run ID"])},
                    {"key": "Section 1", "value": _stringify(run["Section 1"])},
                    {"key": "Section 2", "value": _stringify(run["Section 2"])},
                    {"key": "Section 3", "value": _stringify(run["Section 3"])},
                    {"key": "Section 4", "value": _stringify(run["Section 4"])},
                    {"key": "Section 5", "value": _stringify(run["Section 5"])},
                    {"key": "run_time", "value": _stringify(run["run_time"])},
                    {"key": "DNF", "value": _stringify(run["DNF"])},
                ],
            }
        )
    return rows


def build_athlete_payloads(lympik_client, teamworks_athletes, event_id, tz):
    """Returns a list of {"event_id", "teamworks_user_id", "lympik_profile",
    "ams_event"} dicts, one per matched athlete in this Lympik event -- not
    filtered against the ledger or uploaded yet, since run() collects these
    across every event in the run, filters, and submits them together in as
    few eventsimport batches as possible. Unmatched athletes are logged as
    errors (with the event id) and skipped, never guessed.

    The event's own `module` (off GET /event/{eId}) decides which group
    endpoint the runs come from -- see GROUP_PATH_BY_MODULE. An event on any
    other module is logged and skipped rather than guessed at. Only
    `event:alpine-skiing` has the extra alpine-skiing event detail record, so
    the alpine-specific row-0 fields are left blank on a timing event; asking
    for that record on a timing event is what used to 404 the whole event."""
    event = lympik_client.get(f"/event/{event_id}")
    module = event.get("module")
    module_path = GROUP_PATH_BY_MODULE.get(module)
    if module_path is None:
        logger.error("event %s: unsupported module %r, skipping", event_id, module)
        return []

    event_fields = {
        "Event ID": event["id"],
        "Session Name": event.get("name"),
        "Location": event.get("locationName"),
        "startedAt unix": event.get("startedAt"),
    }
    start_date, start_time = _unix_to_ams_date_time(event["startedAt"], tz)

    runs_df, raw_groups = build_runs_dataframe(lympik_client, event_id, module_path)
    if runs_df.empty:
        logger.info("event %s: no assigned runs, nothing to upload", event_id)
        return []

    # Fetched after the runs check, so an event with nothing to upload can't
    # fail on metadata it never needed.
    alpine_event = {}
    if module == "event:alpine-skiing":
        alpine_event = lympik_client.get_alpine_skiing_event(event_id).get("event") or {}
    event_fields.update(
        {
            "api_discipline": alpine_event.get("discipline"),
            "Gate Count": alpine_event.get("gateCount"),
            "Vertical Drop": alpine_event.get("verticalDrop"),
            "Air Temp": alpine_event.get("airTemperature"),
            "Wind Speed": alpine_event.get("windSpeed"),
            "Humidity": alpine_event.get("humidity"),
            "Snow Temp": alpine_event.get("snowTemperature"),
        }
    )

    fastest_athlete, fastest_time = _compute_event_fastest(raw_groups)
    event_fields["Fastest Athlete"] = fastest_athlete
    event_fields["Fastest Time"] = fastest_time

    # Grouped by name, not Lympik profile id: sample data showed the same
    # athlete can appear under two slightly different profile-id strings
    # across runs within one event, which would otherwise split one athlete
    # into multiple Teamworks uploads for the same session.
    lympik_profiles = [
        {"firstName": fn, "lastName": ln}
        for fn, ln in runs_df[["firstName", "lastName"]].drop_duplicates().itertuples(index=False)
    ]

    matched, unmatched, _ = match_athletes(
        lympik_profiles,
        teamworks_athletes,
        lympik_first_name_fn=lambda p: p["firstName"],
        lympik_last_name_fn=lambda p: p["lastName"],
    )

    for profile in unmatched:
        logger.error("event %s: no Teamworks match for %s %s", event_id, profile["firstName"], profile["lastName"])

    payloads = []
    for lympik_profile, teamworks_athlete in matched:
        athlete_runs_df = (
            runs_df[
                (runs_df["firstName"] == lympik_profile["firstName"])
                & (runs_df["lastName"] == lympik_profile["lastName"])
            ]
            .sort_values("Run Start unix Time")
            .reset_index(drop=True)
        )
        ams_event = {
            "formName": FORM_NAME,
            "startDate": start_date,
            "finishDate": start_date,
            "startTime": start_time,
            "userId": {"userId": teamworks_user_id(teamworks_athlete)},
            "rows": _build_rows_payload(event_fields, athlete_runs_df),
        }

        raw_athlete_groups = [
            g
            for g in raw_groups
            if (g.get("profile") or {}).get("firstName") == lympik_profile["firstName"]
            and (g.get("profile") or {}).get("lastName") == lympik_profile["lastName"]
        ]
        _write_debug_payload(
            event_id,
            module,
            teamworks_user_id(teamworks_athlete),
            lympik_profile,
            event,
            alpine_event,
            event_fields,
            raw_groups,
            raw_athlete_groups,
            athlete_runs_df,
            ams_event,
        )

        payloads.append(
            {
                "event_id": event_id,
                "teamworks_user_id": teamworks_user_id(teamworks_athlete),
                "sort_key": event.get("startedAt"),
                "lympik_profile": lympik_profile,
                "ams_event": ams_event,
            }
        )
    return payloads


def run(lympik_client, teamworks_client, since_unix, tz):
    event_ids = get_recent_event_ids(lympik_client, since_unix)
    logger.info("%d recent event(s) in window", len(event_ids))

    teamworks_athletes = teamworks_client.list_athletes()

    all_payloads = []
    for event_id in event_ids:
        try:
            all_payloads.extend(build_athlete_payloads(lympik_client, teamworks_athletes, event_id, tz))
        except Exception:
            logger.exception("event %s: failed to prepare, will retry next run", event_id)

    if not all_payloads:
        logger.info("nothing to upload")
        return

    # Ask Teamworks itself which of these (event, athlete) pairs already
    # have a "Lympik Event" entry -- see module docstring. start_date is
    # the earliest date any of this run's events could fall on, per the
    # same lookback window used to find them.
    start_date, _ = _unix_to_ams_date_time(since_unix, tz)
    existing_pairs, raw_synchronise_responses = teamworks_client.find_existing_event_ids(
        form_name=FORM_NAME,
        start_date=start_date,
        user_ids=sorted({p["teamworks_user_id"] for p in all_payloads}),
        event_id_field=EVENT_ID_FIELD,
        candidate_event_ids={p["event_id"] for p in all_payloads},
    )
    _write_debug_synchronise_response(raw_synchronise_responses, existing_pairs)

    pending = [p for p in all_payloads if (str(p["event_id"]), str(p["teamworks_user_id"])) not in existing_pairs]
    logger.info(
        "%d athlete-session(s) to upload (%d already in Teamworks)", len(pending), len(all_payloads) - len(pending)
    )

    # Oldest-first, per Teamworks' own eventsimport sample ("minimise
    # re-running historical calcs").
    pending.sort(key=lambda p: p["sort_key"])

    results = teamworks_client.bulk_import_events([p["ams_event"] for p in pending])

    for payload, (_, teamworks_event_id, error) in zip(pending, results):
        profile = payload["lympik_profile"]
        athlete_label = f"{profile['firstName']} {profile['lastName']}"
        if error is not None:
            logger.error("event %s: upload failed for %s: %s", payload["event_id"], athlete_label, error)
        else:
            logger.info("event %s: uploaded %s -> Teamworks event %s", payload["event_id"], athlete_label, teamworks_event_id)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    since_unix = int(time.time() - 86400)
    tz = ZoneInfo(os.environ.get("PIPELINE_TIMEZONE", "America/Denver"))

    run(
        lympik_client=LympikClient(),
        teamworks_client=TeamworksClient(),
        since_unix=since_unix,
        tz=tz,
    )


if __name__ == "__main__":
    main()
