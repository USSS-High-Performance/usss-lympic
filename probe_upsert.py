"""One-off probe against live Teamworks AMS, to answer the two questions the
upsert change depends on and that no amount of local testing can settle:

1. What does `POST /api/v1/synchronise` actually return on *this* AMS
   instance, and specifically which key holds the **Teamworks** event id?
   The current shape in `teamworks_client.py` is confirmed only against a
   different org, and `find_existing_event_ids()` never needed the Teamworks
   id before -- the upsert does, since that id is the `existingEventId` an
   update has to send.
2. Does `/api/v1/eventsimport` (plural, what this pipeline batches through)
   honor `existingEventId`, or does it ignore the field and create a
   duplicate? The published reference only says each item has "the same
   shape" as a singular `eventimport` call, which is an inference, not a
   confirmation -- and the failure mode is silent: an ignored field creates
   rather than errors.

Uses fake data against one athlete (default: Katie Hensien) under a
deliberately obvious Session Name, with a freshly generated UUID as the
Lympik "Event ID" so it can never collide with a real session. Explicitly
authorized as a test; the entries it creates are real AMS records and are
reported at the end for deletion.

Deliberately standalone: nothing in the pipeline imports this, and it is
never run on a schedule. It does reuse `run_pipeline._build_rows_payload()`
rather than hand-rolling a lookalike payload, so what it proves applies to
the real upload path.

Read the output top to bottom -- each phase prints its own verdict, and the
final summary states what the upsert implementation should do.
"""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from athlete_matching import match_athletes, teamworks_user_id
from run_pipeline import EVENT_ID_FIELD, FORM_NAME, RUNS_DF_COLUMNS, _build_rows_payload
from teamworks_client import TeamworksAmsError, TeamworksClient

# `or` rather than a get() default: on a push-triggered run the workflow sets
# these from workflow_dispatch inputs that don't exist, so they arrive set but
# empty -- which a get() default would happily pass through as an empty name.
PROBE_FIRST_NAME = os.environ.get("PROBE_FIRST_NAME") or "Katie"
PROBE_LAST_NAME = os.environ.get("PROBE_LAST_NAME") or "Hensien"
PROBE_SESSION_NAME = "ZZ TEST - Lympik upsert probe (safe to delete)"

# Redacted before anything is printed: this probe dumps whole raw API
# responses whose shape is by definition unknown in advance, and the job log
# it prints into is not the right home for personal data. Names and AMS user
# ids are kept -- the pipeline's own logs already carry those, and the probe
# is unreadable without them.
_PII_KEY_MARKERS = (
    "dob",
    "birth",
    "email",
    "phone",
    "mobile",
    "address",
    "street",
    "postcode",
    "zip",
    "ssn",
    "social",
    "passport",
    "medical",
    "health",
    "injury",
    "gender",
    "sex",
    "ethnic",
    "race",
    "nationality",
    "emergency",
    "guardian",
    "parent",
    "password",
)

logger = logging.getLogger("probe_upsert")


def scrub(node):
    """Recursively redact values whose key looks personal. Applied to every
    raw response before printing -- see _PII_KEY_MARKERS."""
    if isinstance(node, dict):
        return {
            k: ("<redacted>" if any(m in k.lower() for m in _PII_KEY_MARKERS) else scrub(v)) for k, v in node.items()
        }
    if isinstance(node, list):
        return [scrub(item) for item in node]
    return node


def dump(label, payload):
    print(f"\n----- {label} -----")
    print(json.dumps(scrub(payload), indent=2, default=str))


def walk_paths(node, path="$"):
    """Yield (json_path, leaf_value) for every leaf, so a known value can be
    located by path instead of guessing at key names."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield from walk_paths(value, f"{path}.{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from walk_paths(value, f"{path}[{index}]")
    else:
        yield path, node


def find_paths(node, target):
    return [path for path, value in walk_paths(node) if value is not None and str(value) == str(target)]


def resolve_athlete(client):
    """Same roster call and same matching cascade the pipeline uses, so a
    failure here is a real pipeline failure and not a probe artifact."""
    athletes = client.list_athletes()
    print(f"roster: {len(athletes)} athlete(s) visible to this API account")

    profile = {"firstName": PROBE_FIRST_NAME, "lastName": PROBE_LAST_NAME}
    matched, unmatched, _ = match_athletes(
        [profile],
        athletes,
        lympik_first_name_fn=lambda p: p["firstName"],
        lympik_last_name_fn=lambda p: p["lastName"],
    )
    if not matched:
        raise SystemExit(f"ABORT: no Teamworks match for {PROBE_FIRST_NAME} {PROBE_LAST_NAME} -- nothing was written")

    user_id = teamworks_user_id(matched[0][1])
    print(f"matched {PROBE_FIRST_NAME} {PROBE_LAST_NAME} -> Teamworks userId {user_id}")
    return user_id


def build_event_fields(fake_event_id, started_at, fastest_time):
    """Exactly the row-0 field set the pipeline sends (run_pipeline.py's
    build_athlete_payloads), so a rejected field name here is a rejected
    field name there."""
    return {
        EVENT_ID_FIELD: fake_event_id,
        "Session Name": PROBE_SESSION_NAME,
        "Location": "PROBE - not a real venue",
        "startedAt unix": started_at,
        "api_discipline": "slalom",
        "Gate Count": 42,
        "Vertical Drop": 180,
        "Air Temp": -4,
        "Wind Speed": 3,
        "Humidity": 61,
        "Snow Temp": -7,
        "Fastest Athlete": f"{PROBE_FIRST_NAME} {PROBE_LAST_NAME}",
        "Fastest Time": fastest_time,
    }


def build_runs_df(runs):
    """runs: list of (run_id, started_at, section_times, total, dnf)."""
    rows = []
    for run_id, started_at, sections, total, dnf in runs:
        row = {
            "firstName": PROBE_FIRST_NAME,
            "lastName": PROBE_LAST_NAME,
            "Run ID": run_id,
            "Run Start unix Time": started_at,
            "run_time": total,
            "DNF": dnf,
        }
        for i in range(5):
            row[f"Section {i + 1}"] = sections[i] if i < len(sections) else None
        rows.append(row)
    return pd.DataFrame(rows, columns=RUNS_DF_COLUMNS)


def build_payload(user_id, event_fields, runs_df, tz, started_at, existing_event_id=None):
    dt = datetime.fromtimestamp(started_at, tz=tz)
    payload = {
        "formName": FORM_NAME,
        "startDate": dt.strftime("%d/%m/%Y"),
        "finishDate": dt.strftime("%d/%m/%Y"),
        "startTime": dt.strftime("%I:%M %p").lstrip("0"),
        "userId": {"userId": user_id},
        "rows": _build_rows_payload(event_fields, runs_df),
    }
    if existing_event_id is not None:
        payload["existingEventId"] = existing_event_id
    return payload


def synchronise(client, user_id, start_date):
    """Raw paginated /api/v1/synchronise, without any of the parsing
    find_existing_event_ids() layers on top -- the point is to see the
    unfiltered truth."""
    pages = []
    cursor = None
    while True:
        body = {"formName": FORM_NAME, "startDate": start_date, "userIds": [user_id]}
        if cursor:
            body["pagination"] = {"paginate": True, "cursor": cursor}

        response = client.session.post(
            f"{client.base_url}/api/v1/synchronise",
            params={"informat": "json", "format": "json"},
            json=body,
            timeout=30,
        )
        response.raise_for_status()
        page = response.json()
        pages.append(page)

        export = page.get("export") or {}
        cursor = page.get("cursor") or export.get("cursor")
        if not cursor:
            return pages


def events_from_pages(pages):
    """Where find_existing_event_ids() expects events to live. Reported
    separately from the raw dump so a shape mismatch is obvious rather than
    inferred."""
    events = []
    for page in pages:
        events.extend((page.get("export") or {}).get("events", []))
    return events


def describe_event(event):
    """Compact structural summary: which keys each row carries, in order.
    Printed instead of dumping whole unrelated events -- the field *names*
    and row layout are what the upsert has to get right, and a full dump of
    every athlete's history buries them."""
    summary = {}
    for row in event.get("rows", []):
        summary[row.get("row")] = [pair.get("key") for pair in row.get("pairs", [])]
    return summary


def print_event_structure(label, event):
    print(f"\n{label}: Teamworks id {event.get('id')}, userId {event.get('userId')}")
    structure = describe_event(event)
    print(f"  rows returned: {sorted(k for k in structure if k is not None)}")
    for row_index in sorted(structure, key=lambda k: (k is None, k)):
        keys = structure[row_index]
        print(f"  row {row_index} ({len(keys)} field(s)): {keys}")
    return structure


def all_keys(event):
    return {pair.get("key") for row in event.get("rows", []) for pair in row.get("pairs", [])}


def pairs_map(event):
    """key -> list of values across all rows, for spotting which fields AMS
    populated on its own versus which came from our payload."""
    values = {}
    for row in event.get("rows", []):
        for pair in row.get("pairs", []):
            values.setdefault(pair.get("key"), []).append(pair.get("value"))
    return values


def report_shape(pages, fake_event_id, teamworks_event_id):
    print("\n=== SHAPE ANALYSIS ===")
    for i, page in enumerate(pages):
        print(f"page[{i}] top-level keys: {sorted(page.keys())}")
        export = page.get("export")
        if isinstance(export, dict):
            print(f"page[{i}].export keys: {sorted(export.keys())}")

    events = events_from_pages(pages)
    print(f"\nevents found under $.export.events: {len(events)}")
    if not events:
        print("!! find_existing_event_ids() looks for $.export.events -- nothing there.")
        print("!! Paths where the probe's own values DO appear (this is where the parser should look):")
        for label, target in (("Lympik Event ID", fake_event_id), ("Teamworks event id", teamworks_event_id)):
            for page_index, page in enumerate(pages):
                for path in find_paths(page, target):
                    print(f"   {label}: page[{page_index}]{path[1:]}")
        return events

    print(f"first event object keys: {sorted(events[0].keys())}")

    print(f"\nwhere the Lympik Event ID ({fake_event_id}) appears:")
    for path in find_paths(events, fake_event_id) or ["   (not found)"]:
        print(f"   $.export.events{path[1:]}" if path.startswith("$") else path)

    print(f"\nwhere the Teamworks event id ({teamworks_event_id}) appears:")
    hits = find_paths(events, teamworks_event_id)
    for path in hits or ["   (not found -- an update cannot be addressed without this)"]:
        print(f"   $.export.events{path[1:]}" if path.startswith("$") else path)
    if hits:
        # The whole point of the probe: name the key the upsert should read.
        for path in hits:
            key = path.split(".")[-1]
            if "[" not in key:
                print(f"   -> candidate key for existingEventId lookup: {key!r}")

    return events


def matching_events(events, fake_event_id):
    """Every returned event carrying this probe's Event ID anywhere in it,
    with its Teamworks id if one can be located -- the duplicate check."""
    found = []
    for event in events:
        if find_paths(event, fake_event_id):
            teamworks_id = None
            for key in ("id", "eventId", "existingEventId", "event_id"):
                if event.get(key) is not None:
                    teamworks_id = event[key]
                    break
            found.append((teamworks_id, event))
    return found


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    tz = ZoneInfo(os.environ.get("PIPELINE_TIMEZONE", "America/Denver"))
    client = TeamworksClient()
    fake_event_id = str(uuid.uuid4())
    started_at = int(time.time()) - 3600
    start_date = datetime.fromtimestamp(started_at, tz=tz).strftime("%d/%m/%Y")

    print("=== PROBE CONFIG ===")
    print(f"AMS base URL:      {client.base_url}")
    print(f"form:              {FORM_NAME}")
    print(f"athlete:           {PROBE_FIRST_NAME} {PROBE_LAST_NAME}")
    print(f"fake Event ID:     {fake_event_id}")
    print(f"session name:      {PROBE_SESSION_NAME}")
    print(f"event date:        {start_date}")

    print("\n=== PHASE 0: resolve athlete ===")
    user_id = resolve_athlete(client)

    print("\n=== PHASE 1: create (2 runs, no existingEventId) ===")
    runs = [
        ("probe-run-1", started_at, [11.10, 12.20, 13.30], 36.60, False),
        ("probe-run-2", started_at + 300, [10.90, 12.00, 13.10], 36.00, False),
    ]
    create_payload = build_payload(user_id, build_event_fields(fake_event_id, started_at, 36.00), build_runs_df(runs), tz, started_at)
    dump("create payload", create_payload)

    created = client.bulk_import_events([create_payload])
    _, created_id, create_error = created[0]
    if create_error is not None:
        raise SystemExit(f"ABORT: create failed, nothing to test against: {create_error}")
    print(f"\nCREATED Teamworks event id: {created_id}")

    print("\n=== PHASE 2: read back via /api/v1/synchronise ===")
    pages = synchronise(client, user_id, start_date)
    events = report_shape(pages, fake_event_id, created_id)

    after_create = matching_events(events, fake_event_id)
    print(f"\nentries carrying this Event ID after create: {len(after_create)} (expected 1)")

    # Only this probe's own event gets dumped in full. Dumping every page
    # means dumping this athlete's whole Lympik Event history, which buries
    # the field names the upsert actually has to get right.
    created_structure = None
    created_keys = set()
    for _, event in after_create:
        created_structure = print_event_structure("AFTER CREATE", event)
        created_keys = all_keys(event)
        dump("this probe's event as returned by synchronise (after create)", event)

    print("\n=== PHASE 3: update (3 runs + changed Fastest Time, existingEventId set) ===")
    runs.append(("probe-run-3", started_at + 600, [10.50, 11.80, 12.90], 35.20, False))
    update_payload = build_payload(
        user_id,
        build_event_fields(fake_event_id, started_at, 35.20),
        build_runs_df(runs),
        tz,
        started_at,
        existing_event_id=created_id,
    )
    print(f"sending existingEventId={created_id!r} (type {type(created_id).__name__})")
    dump("update payload", update_payload)

    updated = client.bulk_import_events([update_payload])
    _, updated_id, update_error = updated[0]
    if update_error is not None:
        print(f"!! UPDATE FAILED: {update_error}")
        print("!! Interpretation: eventsimport rejected existingEventId. Fall back to")
        print("!! the singular /api/v1/eventimport for updates (the documented path).")
    else:
        print(f"\nUPDATE returned event id: {updated_id}")
        if str(updated_id) == str(created_id):
            print("VERDICT: same id returned -> eventsimport HONORED existingEventId. Update in place works.")
        else:
            print(f"VERDICT: DIFFERENT id ({created_id} -> {updated_id}) -> eventsimport IGNORED")
            print("existingEventId and created a DUPLICATE. Updates must use singular /api/v1/eventimport.")

    print("\n=== PHASE 4: re-read to confirm no duplicate ===")
    pages_after = synchronise(client, user_id, start_date)
    events_after = events_from_pages(pages_after)
    after_update = matching_events(events_after, fake_event_id)

    print(f"\nentries carrying this Event ID after update: {len(after_update)}")
    for teamworks_id, _ in after_update:
        print(f"   Teamworks event id: {teamworks_id}")

    if len(after_update) == 1:
        print("VERDICT: exactly one entry -> update replaced in place, no duplicate. Upsert is safe.")
    elif len(after_update) > 1:
        print("VERDICT: MORE THAN ONE ENTRY -> the update duplicated instead of replacing.")
        print("Do NOT ship the upsert on this path.")
    else:
        print("VERDICT: no entries found. Either synchronise can't see them (shape/permissions)")
        print("or the Event ID isn't where the parser looks. Upsert cannot rely on this lookup yet.")

    # Verifying the row content actually changed matters as much as the id
    # count: an in-place update that silently kept the old rows would look
    # identical to a good one by id alone.
    print("\n=== PHASE 5: did the content actually change? ===")
    for teamworks_id, event in after_update:
        run_ids = find_paths(event, "probe-run-3")
        print(f"event {teamworks_id}: 'probe-run-3' present: {bool(run_ids)}")
        fastest = find_paths(event, "35.2") or find_paths(event, "35.20")
        print(f"event {teamworks_id}: updated Fastest Time (35.20) present: {bool(fastest)}")

    # The upsert sends full state and `existingEventId` replaces the event
    # wholesale, so what matters is whether a field the payload never
    # mentions survives that replace. Existing entries on this form carry
    # fields the pipeline doesn't send (Discipline, Run Time, Total Runs,
    # SL Prep Z-Score, ...); if those are AMS-side calculations they should
    # reappear after an update, and if they don't, the upsert is silently
    # destroying them.
    print("\n=== PHASE 6: which fields survive a full-replace update? ===")
    sent_keys = {pair["key"] for row in update_payload["rows"] for pair in row["pairs"]}
    print(f"keys this probe actually sent ({len(sent_keys)}): {sorted(sent_keys)}")

    for teamworks_id, event in after_update:
        updated_structure = print_event_structure("AFTER UPDATE", event)
        updated_keys = all_keys(event)

        print(f"\n  fields present but NOT sent by us (AMS-side, calculated or defaulted):")
        for key in sorted(updated_keys - sent_keys):
            print(f"    {key} = {pairs_map(event).get(key)}")

        print(f"\n  fields we sent but absent from the response:")
        missing = sorted(sent_keys - updated_keys)
        print(f"    {missing if missing else '(none -- everything we sent came back)'}")

        if created_keys:
            lost = sorted(created_keys - updated_keys)
            gained = sorted(updated_keys - created_keys)
            print(f"\n  present after create but GONE after update: {lost if lost else '(none)'}")
            print(f"  new after update: {gained if gained else '(none)'}")
            if created_structure != updated_structure:
                print("  NOTE: row layout changed between create and update (see structures above)")

        # Called out specifically: existing entries show `Discipline`, while
        # this pipeline writes `api_discipline`. If both exist on the form,
        # the pipeline may be filling a raw field while the one reports read
        # stays empty.
        values = pairs_map(event)
        print(f"\n  'api_discipline' (what the pipeline sends) = {values.get('api_discipline', '<absent>')}")
        print(f"  'Discipline' (what existing entries carry)  = {values.get('Discipline', '<absent>')}")

        dump("this probe's event as returned by synchronise (after update)", event)

    print("\n=== CLEANUP ===")
    print("Fake AMS entries created by this probe -- delete these in AMS when done:")
    print(f"  form '{FORM_NAME}', athlete {PROBE_FIRST_NAME} {PROBE_LAST_NAME}, date {start_date}")
    print(f"  session name '{PROBE_SESSION_NAME}'")
    for teamworks_id in {str(created_id)} | {str(t) for t, _ in after_update if t is not None}:
        print(f"  Teamworks event id: {teamworks_id}")


if __name__ == "__main__":
    try:
        main()
    except TeamworksAmsError as exc:
        print(f"\nABORT: Teamworks rejected a request: {exc}", file=sys.stderr)
        raise
