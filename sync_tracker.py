"""
Sourcing Outreach Tracker — Sync Script
Fetches latest vendor list from Google Sheets CSV export,
merges with existing tracker data (preserving status/notes),
and regenerates the output report.

Gmail syncing is performed by the Claude agent using MCP tools.
This script handles: sheet sync, report generation, query generation,
and applying Gmail updates passed in by the agent.
"""

import json
import csv
import urllib.request
import ssl
import os
from datetime import datetime, date

# macOS Python 3 often lacks system CA certs — use unverified context for Google's public export URL
_SSL_CTX = ssl._create_unverified_context()

# ── Config ──────────────────────────────────────────────────────────────────
SHEET_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1BHg0HxMa31Xpd6gb1bLMDlDmzmjw3QMMz5B8pjfC4Ac"
    "/export?format=csv&gid=367867264"
)
TRACKER_PATH = os.path.join(os.path.dirname(__file__), "sourcing_tracker.json")
REPORT_PATH  = os.path.join(os.path.dirname(__file__), "sourcing_report.md")

FOLLOW_UP_DAYS  = 2    # flag vendor if no reply after this many days
GMAIL_BATCH_SIZE = 15  # vendor emails per Gmail OR query (keeps queries short)

# ── Helpers ──────────────────────────────────────────────────────────────────
def load_tracker() -> dict:
    with open(TRACKER_PATH, "r") as f:
        return json.load(f)

def save_tracker(data: dict):
    data["meta"]["last_synced"] = date.today().isoformat()
    with open(TRACKER_PATH, "w") as f:
        json.dump(data, f, indent=2)

def slug(name: str) -> str:
    return name.lower().replace(" ", "_").replace("-", "_")

def days_since(date_str: str):
    if not date_str:
        return None
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    return (date.today() - d).days

# ── Gmail Query Builder ───────────────────────────────────────────────────────
def get_gmail_search_queries(tracker: dict) -> list[str]:
    """
    Build batched Gmail search queries covering all tracked vendor emails.
    Uses a date filter (since last_gmail_sync) to minimise results processed.

    Returns a list of query strings — run each as a separate Gmail search and
    union the results. Batch size is capped at GMAIL_BATCH_SIZE to stay well
    under Gmail's query-length limit.

    At 100 vendors with batch size 15: ~7 queries per sync (vs 100 previously).
    Token cost stays nearly flat as vendor count grows.
    """
    since = tracker["meta"].get("last_gmail_sync")
    if since:
        # Gmail date filter uses YYYY/MM/DD format
        date_filter = "after:" + since.replace("-", "/") + " "
    else:
        date_filter = ""

    emails = [v["contact_email"] for v in tracker["vendors"] if v.get("contact_email")]

    queries = []
    for i in range(0, len(emails), GMAIL_BATCH_SIZE):
        batch = emails[i : i + GMAIL_BATCH_SIZE]
        addr_clause = " OR ".join(
            f"(to:{e} OR from:{e})" for e in batch
        )
        queries.append(f"{date_filter}({addr_clause})")

    return queries

def update_gmail_sync_timestamp(tracker: dict) -> dict:
    """Record when the last Gmail sync ran (today's date)."""
    tracker["meta"]["last_gmail_sync"] = date.today().isoformat()
    return tracker

# ── Sheet Sync ───────────────────────────────────────────────────────────────
def fetch_sheet_vendors() -> list[dict]:
    """
    Fetch current vendor list from Google Sheets CSV export.
    Handles sheets that have blank rows above the header row.
    """
    req = urllib.request.Request(
        SHEET_CSV_URL,
        headers={"User-Agent": "Mozilla/5.0"}
    )
    with urllib.request.urlopen(req, context=_SSL_CTX) as resp:
        lines = resp.read().decode("utf-8").splitlines()

    # Strip blank rows to find the true header row
    clean_lines = [l for l in lines if l.strip().replace(",", "")]
    reader = csv.DictReader(clean_lines)

    vendors = []
    for row in reader:
        source = row.get("Source", "").strip()
        if not source:
            continue
        vendors.append({
            "id":            slug(source),
            "source":        source,
            "category":      row.get("Category", "").strip(),
            "relevant_skus": row.get("Relevant SKUs", "").strip(),
            "contact_email": row.get("Contact Email", "").strip(),
        })
    return vendors

def merge_vendors(tracker: dict, sheet_vendors: list[dict]) -> dict:
    """
    Add new vendors from the sheet; preserve existing tracking data.
    Does NOT remove vendors dropped from the sheet (they may still be active).
    """
    existing = {v["id"]: v for v in tracker["vendors"]}
    merged = []

    for sv in sheet_vendors:
        if sv["id"] in existing:
            ev = existing[sv["id"]]
            ev["source"]        = sv["source"]
            ev["category"]      = sv["category"]
            ev["relevant_skus"] = sv["relevant_skus"]
            ev["contact_email"] = sv["contact_email"]
            merged.append(ev)
        else:
            merged.append({
                **sv,
                "contact_name":        None,
                "outreach_date":       None,
                "status":              "Not Started",
                "last_email_date":     None,
                "last_email_subject":  None,
                "last_email_snippet":  None,
                "days_since_outreach": None,
                "next_action":         "Send initial outreach email requesting catalog and pricing",
                "next_action_due":     None,
                "notes":               None,
            })

    # Preserve vendors removed from sheet — flag them but keep tracking data
    sheet_ids = {sv["id"] for sv in sheet_vendors}
    for vid, ev in existing.items():
        if vid not in sheet_ids:
            ev["_removed_from_sheet"] = True
            merged.append(ev)

    tracker["vendors"] = merged
    return tracker

# ── Status Logic ─────────────────────────────────────────────────────────────
def compute_days_since_outreach(tracker: dict) -> dict:
    for v in tracker["vendors"]:
        v["days_since_outreach"] = days_since(v.get("outreach_date"))
    return tracker

def flag_follow_ups(tracker: dict) -> list[dict]:
    """Return vendors needing attention, with a human-readable flag."""
    flagged = []
    for v in tracker["vendors"]:
        status = v.get("status", "Not Started")
        days   = v.get("days_since_outreach")
        due    = v.get("next_action_due")

        if status == "Not Started":
            flagged.append({**v, "_flag": "Action Required — no outreach sent yet"})
        elif status in ("Outreach Sent", "Awaiting Response", "NDA Sent", "NDA Acknowledged") \
                and days is not None and days >= FOLLOW_UP_DAYS:
            flagged.append({**v, "_flag": f"Follow-Up Overdue — {days} day(s) since outreach, awaiting response"})
        elif status == "Follow-Up Needed":
            flagged.append({**v, "_flag": "Follow-Up Needed — manually flagged"})
        elif due and due <= date.today().isoformat() and status not in ("NDA Signed", "Closed", "On Hold"):
            flagged.append({**v, "_flag": f"Action Due — next action date reached ({due})"})

    return flagged

# ── Report Generation ────────────────────────────────────────────────────────
def generate_report(tracker: dict) -> str:
    today     = date.today().strftime("%B %d, %Y")
    vendors   = tracker["vendors"]
    flagged   = flag_follow_ups(tracker)
    last_sync = tracker["meta"].get("last_gmail_sync", "never")

    status_counts: dict[str, int] = {}
    for v in vendors:
        s = v.get("status", "Not Started")
        status_counts[s] = status_counts.get(s, 0) + 1

    lines = [
        "# Sourcing Outreach Tracker",
        f"**Report Date:** {today}  ",
        f"**Last Gmail Sync:** {last_sync}  ",
        f"**Sheet:** [Open Google Sheet]({tracker['meta']['sheet_link']})",
        "",
        "---",
        "",
        "## Summary",
        "",
        "| Status | Count |",
        "|--------|-------|",
    ]
    for status, count in sorted(status_counts.items()):
        lines.append(f"| {status} | {count} |")

    lines += [
        "",
        "---",
        "",
        f"## Priority Follow-Ups ({len(flagged)})",
        "",
    ]

    if flagged:
        for v in flagged:
            lines += [
                f"### {v['source']}",
                f"- **Status:** {v.get('status')}",
                f"- **Flag:** {v.get('_flag', '')}",
                f"- **Contact:** {v.get('contact_name') or ''} <{v.get('contact_email', 'N/A')}>",
                f"- **Last Email:** {v.get('last_email_date') or 'None'} — {v.get('last_email_subject') or ''}",
                f"- **Next Action:** {v.get('next_action') or 'N/A'} (due {v.get('next_action_due') or 'TBD'})",
                "",
            ]
    else:
        lines.append("_No urgent follow-ups at this time._\n")

    lines += [
        "---",
        "",
        "## All Vendors",
        "",
        "| Vendor | Category | SKUs | Status | Outreach Date | Days Out | Last Email | Next Action Due |",
        "|--------|----------|------|--------|---------------|----------|------------|-----------------|",
    ]

    for v in vendors:
        days     = v.get("days_since_outreach")
        days_str = str(days) if days is not None else "—"
        lines.append(
            f"| {v['source']} "
            f"| {v['category']} "
            f"| {v['relevant_skus']} "
            f"| {v.get('status', '—')} "
            f"| {v.get('outreach_date') or '—'} "
            f"| {days_str} "
            f"| {v.get('last_email_date') or '—'} "
            f"| {v.get('next_action_due') or '—'} |"
        )

    lines += [
        "",
        "---",
        "",
        "_Auto-generated. Ask Claude to run a Gmail sync to refresh statuses._",
    ]

    return "\n".join(lines)

# ── Main ─────────────────────────────────────────────────────────────────────
def run_sheet_sync():
    """Pull latest vendors from Google Sheet and regenerate report. No Gmail."""
    print("Loading tracker...")
    tracker = load_tracker()

    print("Fetching vendors from Google Sheet...")
    sheet_vendors = fetch_sheet_vendors()
    print(f"  Found {len(sheet_vendors)} vendor(s) in sheet.")

    tracker = merge_vendors(tracker, sheet_vendors)
    tracker = compute_days_since_outreach(tracker)

    save_tracker(tracker)

    report = generate_report(tracker)
    with open(REPORT_PATH, "w") as f:
        f.write(report)

    print(f"Sheet sync complete. Report: {REPORT_PATH}")
    return tracker

def print_gmail_queries():
    """Print the optimised Gmail search queries for the current vendor list."""
    tracker = load_tracker()
    queries = get_gmail_search_queries(tracker)
    print(f"Gmail search queries ({len(queries)} batch(es) for {len(tracker['vendors'])} vendor(s)):")
    for i, q in enumerate(queries, 1):
        print(f"  [{i}] {q}")
    return queries

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "queries":
        print_gmail_queries()
    else:
        run_sheet_sync()
