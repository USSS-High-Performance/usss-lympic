# Teamworks AMS v1 API — integration notes (from the usss-mocap upload workflow)

Context: workflow is *source file with an athlete's name attached → resolve to an AMS
user → build a payload with single-value "event" fields plus a repeating table →
POST to `/api/v1/eventimport`*. These notes capture what actually works, confirmed by
live testing, so a future integration doesn't have to re-discover it the hard way.

## Auth

- v1 endpoints use HTTP Basic Auth (base64 `username:password` in the `Authorization`
  header). No session/token needed.
- Send an `X-APP-ID` header (any string identifying your integration) — helps Teamworks
  support find your requests if you ever need to open a ticket.
- v2/v3 endpoints use a different, session-token-based auth. Not needed unless a v2/v3
  endpoint is specifically required.

## Resolving a user/athlete

- **Don't** use `/api/v1/groupmembers`. It's scoped to one named group, isn't reliably
  paginated, and any mismatch between the group's real membership and what you expect
  (wrong spelling of the group name, an athlete simply not added to that group) produces
  a generic "not found" with no indication that the group itself is the problem.
- **Do** use `/api/v1/usersynchronise` instead — it returns every user the API account
  can see (group membership doesn't matter) and has real cursor pagination:
  ```json
  {"lastSynchronisationTimeOnServer": 0, "paginate": "True", "cursor": ""}
  ```
  Follow the `cursor` field in each response until it comes back empty/null. The user
  list is wrapped under an implementation-specific key that isn't precisely documented —
  find it by shape (the first list-of-dicts value in the response) instead of hardcoding
  a key name.
- Field names on user objects vary by instance (`userId`/`user_id`/`id`,
  `firstName`/`first_name`/`forename`, etc.) — write a small helper that tries several
  candidate keys rather than assuming one.
- A simple, workable name-matching cascade: exact last name (case-insensitive) → narrow
  by first-initial → narrow by full first name. Know its limits going in: no fuzzy
  matching, no accent/unicode normalization, no handling of hyphens/middle
  names/suffixes, and a genuine duplicate name in the org still produces an ambiguous
  "no unique match." If that's a real risk, prefer matching on a stable ID (athlete ID,
  DOB) over name when the source data can supply one.

## Submitting data — `/api/v1/eventimport`

This endpoint creates/updates "Events" against a form. AMS forms are no-code/user
configurable, so the endpoint's real behavior has gaps versus its published schema —
the following is confirmed by live testing, not just docs.

**Basic payload shape:**
```json
{
  "formName": "Exact Form Name",
  "startDate": "dd/mm/yyyy",
  "finishDate": "dd/mm/yyyy",
  "startTime": "h:mm AM/PM",
  "userId": { "userId": 12345 },
  "rows": [
    { "row": 0, "pairs": [ { "key": "Field Name", "value": "stringified value" } ] }
  ]
}
```
- Every `value` must be a string regardless of the field's real type.
- `key` must exactly match the field name configured in the AMS form builder
  (case-sensitive) — get it from the form itself, don't guess or infer from the UI label.
- **The API returns HTTP 200 even on failure.** Always parse and check the body.
  Confirmed success shape:
  ```json
  { "state": "SUCCESSFULLY_IMPORTED", "message": "...", "ids": [123456] }
  ```
  Treat this as an **allowlist**, not a blocklist — check for
  `state == "SUCCESSFULLY_IMPORTED"` and treat everything else as failure, rather than
  checking for known failure strings (you won't know all of them in advance).
- `existingEventId` (from `ids[0]` of a prior create) **replaces the entire event's
  contents** — it does not append or merge. If you're updating an event, resend the
  full desired state, not a delta; anything you omit is not guaranteed to survive.
- **`existingEventId` works on the plural `/api/v1/eventsimport` too**, not just the
  singular endpoint that documents it — confirmed live. Set it per event object
  inside `events`. An int works; the doc example's `""` for "create" is equivalent to
  omitting the key.
- **An update returns the same event id it replaced.** This is the only way to tell
  an update from an accidental create: `eventsimport` reports
  `SUCCESSFULLY_IMPORTED` either way, so if the returned id differs from the
  `existingEventId` you sent, it created a duplicate. Check every update's returned
  id against what you sent.
- **Calculated fields survive a full replace.** A form's derived fields are
  recomputed from the values you send rather than being wiped by the replace, so
  "resend full state" means *your* fields, not the form's. Confirmed live on a form
  carrying twelve such fields — copies of a sent field under a different name, a URL
  built from an id, a stripped/normalized version of a text field, reformatted
  numbers, aggregates over the table rows (count, sum, count and % of a boolean
  column), and rolling multi-event calcs. All recomputed correctly after an update;
  none were lost. Note that a derived field whose source is empty may simply not
  appear, and **fields you send as `""` are dropped from the response entirely**.

**Forms with single-value ("event") fields + a repeating table — the shape that works:**

If a form has fields that apply once per event (e.g. Movement, Variable, Plane) plus a
genuinely repeating table (e.g. a Percent-indexed table of trial values):

- `row: 0`'s `pairs` = **only** the single-value/event-level fields. No table columns
  mixed in.
- `row: 1` through `row: N` = one table row each; `pairs` = **only** that row's table
  columns.
- Never duplicate a single-value field's key across more than one row — even supplying
  it blank on other rows gets rejected: *"This form does not support multiple rows for
  key: `<field>`"*.
- Build the whole event (event fields + entire table) in **one call**. Don't try to
  create-then-append across multiple calls — since `existingEventId` replaces rather
  than merges, a second call just clobbers the first.

**What doesn't work, confirmed by testing (so you don't have to re-learn it):**
- Single-value field repeated on every row, even with a blank value on all but one row
  → rejected ("does not support multiple rows for key").
- Single-value fields *mixed into* row 0 alongside that row's own table data, with
  later rows carrying only table data → AMS may silently split this into **two**
  separate events instead of one combined event.
- Single-value fields sent as extra top-level payload keys (siblings of
  `formName`/`startDate`/etc.) → hard `422`, the schema rejects unrecognized properties.
- Submitting each logical record as its own separate `eventimport` call "to be safe" —
  works, but explodes your event count. One test run on this project generated 3,000+
  stray events from a file that should have produced ~105. Batch into the fewest events
  that make semantic sense, and smoke-test on a single record/combo before running a
  full file through it.

## Debugging practices that saved real time

- Always log the raw response body (`resp.text`), not just `resp.raise_for_status()`.
  A `4xx`/`5xx` body from this API usually names the exact violated field/rule; a `200`
  can still be a silent failure.
- Keep a small, standalone, read-only diagnostic script (no GUI, no side effects) that
  exercises just the read endpoints (e.g. user lookup) so assumptions can be checked
  independently of the full app.
- Smoke-test a single record before a full batch upload. A wrong assumption at scale
  pollutes the destination system, and cleaning up thousands of junk records in a live
  AMS instance is no fun.
- When official docs are thin (true for this API, since forms are dynamic/no-code),
  don't guess blindly for many rounds. Teamworks publishes an `llms.txt` documentation
  index plus per-endpoint OpenAPI pages at `docs.ams.teamworksapp.com` — pull the
  specific endpoint's page before guessing, and if that's still insufficient, ask their
  support for a worked "recipe" rather than spending many test cycles reverse-engineering
  undocumented behavior. (Note: some sandboxed environments block outbound access to
  this docs site — if so, fetch it from a normal browser/session instead.)

## Reusing this for the next AMS integration

Rather than re-deriving all of the above, treat `mocap_report_gui/smartabase_client.py`
in `usss-mocap` as the working reference implementation — it already has the confirmed
patterns for auth, paginated user lookup, and event submission
(`_headers`, `_fetch_all_athletes`, `get_athlete_id`, `_post_event`). Two reasonable
paths for a new project:

1. **Copy/adapt the file directly** — swap in the new form's name and field lists.
   Lowest effort, but two independent copies can drift apart over time as either one
   picks up fixes.
2. **Extract the generic parts into a small shared internal module** that multiple
   repos import — better if more than one or two AMS integrations are expected, but
   it's an infrastructure decision (private package index, git submodule, or a shared
   internal utilities repo) rather than a code change, so it's worth deciding
   deliberately rather than defaulting into it.

Either way, keep these notes (or a link to this file) next to whichever project starts
the next integration — the specific failure modes above (existingEventId semantics,
single-value-field row placement, HTTP-200-on-failure) are exactly the ones that cost
the most back-and-forth here.
