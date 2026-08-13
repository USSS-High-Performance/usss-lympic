"""Read-only diagnostic: what do the anonymous runs in the current window
actually look like?

Written to answer two questions the "Guest - {label}" change raises but a dry
run can't:

1. Is `label` ever populated? A dry run on a live event produced plain
   "Guest", which means that run had no label -- so the label branch may be
   dead code against this data, and the change would silently do nothing.
2. What is the fastest run, really? That same event's fastest completed run
   came out at 15.42 against a field where matched athletes run far slower,
   which is the signature of a partial run that Lympik did *not* flag
   `invalid` -- the exact case `_compute_event_fastest()` excludes when the
   flag is set, and can't when it isn't.

Touches Lympik only, reads only, writes nothing anywhere. Delete along with
the rest of the branch scaffolding once these questions are settled.
"""

import logging
import os
import time
from zoneinfo import ZoneInfo

from lympik_activity import get_recent_event_ids
from lympik_client import LympikClient
from run_pipeline import GROUP_PATH_BY_MODULE, _compute_event_fastest

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")


def describe_run(group):
    profile = group.get("profile") or {}
    name_parts = [p for p in (profile.get("firstName"), profile.get("lastName")) if p]
    return {
        "named": bool(name_parts),
        "label": group.get("label"),
        "duration": group.get("totalDuration"),
        "invalid": group.get("invalid"),
        "sections": len(group.get("edges") or []),
    }


def main():
    since_unix = int(time.time() - 86400)
    tz = ZoneInfo(os.environ.get("PIPELINE_TIMEZONE", "America/Denver"))
    client = LympikClient()

    event_ids = get_recent_event_ids(client, since_unix)
    print(f"{len(event_ids)} event(s) in the 24h window (tz {tz})\n")

    for event_id in event_ids:
        event = client.get(f"/event/{event_id}")
        module = event.get("module")
        module_path = GROUP_PATH_BY_MODULE.get(module)
        print(f"=== event {event_id} ===")
        print(f"  name={event.get('name')!r} module={module!r}")
        if module_path is None:
            print("  unsupported module, the pipeline skips this event\n")
            continue

        groups = list(client.get_all_pages(f"/event/{event_id}/{module_path}/group"))
        described = [describe_run(g) for g in groups]

        named = [d for d in described if d["named"]]
        anon = [d for d in described if not d["named"]]
        labelled = [d for d in anon if str(d["label"] or "").strip()]

        print(f"  {len(groups)} run(s): {len(named)} named, {len(anon)} anonymous")
        print(f"  anonymous runs carrying a non-blank label: {len(labelled)}")
        if labelled:
            print(f"  distinct labels seen: {sorted({str(d['label']).strip() for d in labelled})}")

        # The question behind the 15.42: how do completed anonymous times compare
        # to completed named ones? A partial run shows up as an outlier low.
        def completed(rows):
            return sorted(d["duration"] for d in rows if d["duration"] is not None and d["invalid"] is None)

        named_times, anon_times = completed(named), completed(anon)
        print(f"  completed named times    (n={len(named_times)}): {named_times[:8]}")
        print(f"  completed anonymous times(n={len(anon_times)}): {anon_times[:8]}")

        # Every completed anonymous run, with its section count -- a run with
        # fewer sections than the rest of the field only timed part of the course.
        section_counts = sorted({d["sections"] for d in described})
        print(f"  section counts across all runs: {section_counts}")
        for d in sorted(
            (d for d in anon if d["duration"] is not None and d["invalid"] is None),
            key=lambda d: d["duration"],
        )[:5]:
            print(f"    anon completed: duration={d['duration']} sections={d['sections']} label={d['label']!r}")

        print(f"  _compute_event_fastest() -> {_compute_event_fastest(groups)}\n")


if __name__ == "__main__":
    main()
