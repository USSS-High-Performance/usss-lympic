"""Thin client for the Teamworks AMS v1 API (HTTP Basic Auth).

Behavior here follows docs/teamworks-api-reference.md plus the confirmed
gotchas in docs/teamworks-ams-notes.md from a prior AMS integration -- AMS
forms are no-code/user-configurable, so the real behavior of the import
endpoints has gaps versus their published schema.
"""

import logging
import os
from collections import namedtuple

import requests
from dotenv import load_dotenv

load_dotenv()

# What find_existing_events() recovered from /api/v1/synchronise.
#
# by_pair: {(str(lympik_event_id), str(teamworks_user_id)): [teamworks event
#   id, ...]} -- a list because the same pair can legitimately come back more
#   than once (duplicates already in AMS from earlier manual imports), and the
#   caller needs to see that rather than have one silently picked for it.
# events_seen / events_parsed: how many events came back, and how many yielded
#   both a user id and a Teamworks event id. The gap between them is the
#   signal that this endpoint's shape has drifted: events_seen > 0 with
#   events_parsed == 0 means the parser is broken, which is very different
#   from "these users have no matching entries" and must not be treated as
#   "nothing exists, create everything".
ExistingEvents = namedtuple("ExistingEvents", "by_pair events_seen events_parsed raw_responses")

DEFAULT_BATCH_SIZE = 25  # per Teamworks' own sample: start small, raise only after measuring.
DEFAULT_BASE_URL = "https://usopc.smartabase.com/athlete360-usss"

logger = logging.getLogger("teamworks_client")


class TeamworksAmsError(Exception):
    """Raised when an import endpoint returns HTTP 200 with a non-success body.
    These endpoints return 200 even on failure -- raise_for_status() alone
    will not catch it, so the response body must always be checked."""


class TeamworksClient:
    def __init__(self, base_url=None, username=None, password=None, app_id=None):
        self.base_url = (base_url or os.environ.get("TEAMWORKS_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.username = username or os.environ["TEAMWORKS_USERNAME"]
        self.password = password or os.environ["TEAMWORKS_PASSWORD"]
        self.app_id = app_id or os.environ.get("TEAMWORKS_APP_ID", "usss.lympik-integration")

        self.session = requests.Session()
        self.session.auth = (self.username, self.password)
        self.session.headers["X-APP-ID"] = self.app_id

    def list_athletes(self):
        """Walks /api/v1/usersynchronise to completion and returns every user
        the API account can see (group membership doesn't matter, unlike
        /api/v1/groupmembers). Always starts a full sync from
        lastSynchronisationTimeOnServer=0 -- a caller wanting Teamworks'
        incremental delta-sync should track and pass that value itself."""
        users = []
        cursor = ""
        while True:
            body = {
                "lastSynchronisationTimeOnServer": 0,
                "userIds": [],
                "paginate": "True",
                "cursor": cursor,
            }
            response = self.session.post(
                f"{self.base_url}/api/v1/usersynchronise",
                params={"informat": "json", "format": "json"},
                json=body,
                timeout=30,
            )
            response.raise_for_status()
            page = response.json()

            users.extend(_find_user_list(page))

            cursor = page.get("cursor")
            if not cursor:
                return users

    def find_existing_events(self, form_name, start_date, user_ids, event_id_field, candidate_event_ids):
        """Which (Lympik event id, Teamworks user id) pairs already have a
        `form_name` entry in Teamworks on/after start_date, and the Teamworks
        event id of each -- queried fresh from Teamworks every run rather than
        from a ledger file, so there's no local state to lose and entries
        created any other way (a manual entry, a different script) are seen too.

        The Teamworks event id is the point: it's what `existingEventId` needs
        in order to update an entry in place instead of adding a second one.

        Response shape, confirmed live against this AMS instance by
        probe_upsert.py (see docs/teamworks-api-reference.md): request
        {"formName", "startDate", "userIds"}, paginated via
        {"pagination": {"paginate": True, "cursor": ...}} on pages after the
        first; events under body["export"]["events"]; each event's Teamworks id
        under its own "id"; "userId" a bare int (not the {"userId": N} wrapper
        the import endpoints take); and our own "Event ID" among row 0's pairs.

        userIds is mandatory here: omitting it returns no events for anyone
        rather than "all events" -- so empty user_ids or candidate_event_ids
        short-circuits instead of making a call that would quietly mean
        something else.

        event_id_field is the row-0 field holding our Lympik event id, checked
        first. If it doesn't land on one of candidate_event_ids, every string
        leaf in the event is checked against that set as a fallback -- these
        are distinctive UUIDs, so a stray match is implausible. Note that AMS
        also derives a "Lympik Activity URL" field containing the same id, so
        the fallback can match on that; harmless, since it identifies the same
        event.

        Returns an ExistingEvents. Ids in by_pair are stringified on the key
        side (this endpoint's id types aren't guaranteed to match
        usersynchronise's) but left as-returned on the value side, since
        that value goes straight back out as existingEventId.
        """
        candidate_ids = {str(cid) for cid in candidate_event_ids}
        if not candidate_ids or not user_ids:
            return ExistingEvents({}, 0, 0, [])

        by_pair = {}
        raw_responses = []
        events_seen = 0
        events_parsed = 0
        cursor = None
        base_body = {
            "formName": form_name,
            "startDate": start_date,
            "userIds": sorted(user_ids),
        }
        while True:
            body = dict(base_body)
            if cursor:
                body["pagination"] = {"paginate": True, "cursor": cursor}

            response = self.session.post(
                f"{self.base_url}/api/v1/synchronise",
                params={"informat": "json", "format": "json"},
                json=body,
                timeout=30,
            )
            response.raise_for_status()
            page = response.json()
            raw_responses.append(page)

            export = page.get("export") or {}
            for event in export.get("events", []):
                events_seen += 1

                user_id = _event_user_id(event)
                teamworks_event_id = _event_teamworks_id(event)
                if user_id is None or teamworks_event_id is None:
                    # Counted as seen but not parsed: an event we can't
                    # address is exactly the case that must not be mistaken
                    # for "this event doesn't exist yet".
                    continue
                events_parsed += 1

                event_id = _extract_field_value(event, event_id_field)
                if str(event_id) not in candidate_ids:
                    event_id = next((v for v in _walk_strings(event) if v in candidate_ids), None)
                if event_id is None:
                    continue

                by_pair.setdefault((str(event_id), str(user_id)), []).append(teamworks_event_id)

            cursor = page.get("cursor") or export.get("cursor")
            if not cursor:
                break

        for ids in by_pair.values():
            ids[:] = _sorted_ids(ids)

        if events_seen and not events_parsed:
            logger.error(
                "synchronise returned %d event(s) but none yielded both a user id and an event id -- "
                "this endpoint's response shape has changed; check "
                "debug_payloads/synchronise_response.json",
                events_seen,
            )

        return ExistingEvents(by_pair, events_seen, events_parsed, raw_responses)

    def bulk_import_events(self, events, batch_size=DEFAULT_BATCH_SIZE):
        """POSTs /api/v1/eventsimport in batches of `batch_size`.

        Each item in `events` is a single event dict (formName/startDate/
        startTime/userId/rows/... -- same shape as a single-event import).
        All events in one call must target the same form -- this only reads
        eventImportResultForForm[0], the correct index only when every event
        in the batch uses the same form name (true for this pipeline, which
        only ever submits "Lympik Event").

        This endpoint is all-or-nothing per batch: a single malformed event
        fails the *entire* batch, and the API does not say which one is at
        fault. When a batch fails, this method automatically retries every
        event in that batch individually (batch size 1) so one bad payload
        doesn't block its batch-mates and the caller learns exactly which
        event failed and why.

        Teamworks can also report a batch as SUCCESSFULLY_IMPORTED while
        silently returning fewer ids than events submitted (seen in practice
        for a userId Teamworks can't actually deliver a "Lympik Event" entry
        to) -- there's no per-event error for this, just a shorter list. That
        id-count mismatch is treated the same as a batch failure: retried
        individually so the caller learns exactly which event didn't
        actually go through, instead of silently misaligning results by
        position.

        Returns a list of (event, event_id, error) tuples, one per input
        event, in the same order as `events` -- event_id is the resulting
        Teamworks event id on success and None on failure, error is the
        exception on failure and None on success.
        """
        results = [None] * len(events)

        for start in range(0, len(events), batch_size):
            batch = events[start : start + batch_size]
            indices = range(start, start + len(batch))

            batch_ids = None
            try:
                batch_ids = self._post_eventsimport(batch)
            except TeamworksAmsError:
                logger.warning(
                    "batch of %d event(s) failed as a whole, retrying individually to isolate the cause", len(batch)
                )

            if batch_ids is not None and len(batch_ids) != len(batch):
                logger.warning(
                    "batch of %d event(s) reported success but returned %d id(s) -- "
                    "at least one event was silently dropped, retrying individually to isolate which",
                    len(batch),
                    len(batch_ids),
                )
                batch_ids = None

            if batch_ids is not None:
                for idx, event_id in zip(indices, batch_ids):
                    results[idx] = (events[idx], event_id, None)
                continue

            for idx in indices:
                try:
                    single_result_ids = self._post_eventsimport([events[idx]])
                    if len(single_result_ids) != 1:
                        raise TeamworksAmsError(
                            f"expected exactly 1 id for a single-event import, got {single_result_ids!r}"
                        )
                    results[idx] = (events[idx], single_result_ids[0], None)
                except TeamworksAmsError as exc:
                    results[idx] = (events[idx], None, exc)

        return results

    def _post_eventsimport(self, events):
        response = self.session.post(
            f"{self.base_url}/api/v1/eventsimport",
            params={"informat": "json", "format": "json"},
            json={"events": events},
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()

        state = (body.get("result") or {}).get("state")
        if state != "SUCCESSFULLY_IMPORTED":
            raise TeamworksAmsError(f"{state}: {(body.get('result') or {}).get('message')} (raw body: {body})")

        return body["eventImportResultForForm"][0]["eventImportResults"]["ids"]


def _find_user_list(response_json):
    """The user list is wrapped under an implementation-specific key that
    varies by AMS instance -- find it by shape (the first list-of-dicts
    value in the response) rather than hardcoding a key name."""
    for value in response_json.values():
        if isinstance(value, list) and (not value or isinstance(value[0], dict)):
            return value
    return []


def _extract_field_value(event, field_name):
    """Row 0 holds event-level fields (Event ID, Session Name, ...) --
    confirmed shape for eventimport/eventsimport requests; assumed
    symmetric for synchronise responses until confirmed otherwise."""
    for row in event.get("rows", []):
        if row.get("row") == 0:
            for pair in row.get("pairs", []):
                if pair.get("key") == field_name:
                    return pair.get("value")
    return None


def _walk_strings(node):
    """Yield every string leaf value in an arbitrarily nested dict/list --
    fallback for locating a known id when the row-0 shape doesn't apply."""
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for value in node.values():
            yield from _walk_strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_strings(item)


def _event_user_id(event):
    """The import endpoints take {"userId": N}, but synchronise responses
    return a bare int (confirmed live) -- handle both, since the same helper
    reads both directions."""
    raw = event.get("userId")
    if isinstance(raw, dict):
        return raw.get("userId")
    return raw


def _event_teamworks_id(event):
    """The AMS-side event id, which an update sends back as
    `existingEventId`. Confirmed to be `id` on this instance; the aliases
    are cheap insurance and cost nothing when `id` is present."""
    for key in ("id", "eventId", "existingEventId", "event_id"):
        value = event.get(key)
        if value is not None:
            return value
    return None


def _sorted_ids(ids):
    """Lowest first, so a pair with duplicate entries resolves to the oldest
    (lowest-numbered) one deterministically. Falls back to string ordering if
    a response ever mixes id types, which would otherwise raise."""
    try:
        return sorted(ids)
    except TypeError:
        return sorted(ids, key=str)
