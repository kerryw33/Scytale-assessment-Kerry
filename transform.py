# Kerry-Lynn Whyte
# 30/08/2025
# Processes enriched data to CSV, and analyses whether reviews/checks passed.
# Usage:
#   python transform.py --input data/raw/enriched.json --output data/processed/PR_audit.csv

import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

# Constants used in evaluation 
APPROVED = "APPROVED"          # Review "state" value - signifies approval
SUCCESS_STATES = {"success"}   # Acceptable combined-status "state" values

def parse_iso(s: str | None):
    """
    Convert an ISO8601 string from GitHub (typically ends with 'Z') to a timezone-aware
    Python datetime in UTC. Returns None if input is falsy.
    """
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def safe_get(d, *keys, default=None):
    """
    Safely walk nested dict keys, if there's a value return it or `default=0` if any level is missing.
    """
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k, default)
    return cur


def review_approved_before_merge(reviews: list, merged_at_dt):
    """
    Determine CR_PASSED:
      Return True if there exists at least one review with state == "APPROVED"
      and (if timestamps are available) the review's submitted_at <= merged_at.
      If timestamps are missing, treat an APPROVED review as sufficient.
    """
    for r in reviews or []:
        if r.get("state") == APPROVED:
            sub = parse_iso(r.get("submitted_at"))
            if sub is None or merged_at_dt is None or sub <= merged_at_dt:
                return True
    return False

def required_contexts_from_protection(required: dict | None):
    """
    Extract the list of *required* status-check contexts from the branch protection payload.
    GitHub can return this in two ways:
      - {"contexts": ["ci/build", "ci/test", ...]}
      - {"checks": [{"context": "ci/build"}, {"context": "ci/test"}]}
    Returns a de-duplicated list of strings (can be empty).
    """
    ctxs = []
    if isinstance(required, dict):
        if isinstance(required.get("contexts"), list):
            ctxs.extend([c for c in required["contexts"] if c])
        if isinstance(required.get("checks"), list):
            ctxs.extend([c.get("context") for c in required["checks"] if c.get("context")])
    # Remove duplicates while preserving order
    return list(dict.fromkeys(ctxs))

def latest_context_state_before_merge(combined_status: dict, context: str, merged_at_dt):
    """
    For a given required context, look at combined_status.statuses[] entries with matching "context".
    Take the most recent one (by 'updated_at') and return True if: 'state' == 'success', AND (if timestamps exist) updated_at <= merged_at.
    """
    statuses = combined_status.get("statuses") or []
    candidates = [s for s in statuses if s.get("context") == context]
    if not candidates:
        return False
    # Sort newest-first by updated_at and inspect the latest
    candidates.sort(key=lambda s: s.get("updated_at", ""), reverse=True)
    latest = candidates[0]
    if latest.get("state") != "success":
        return False
    up = parse_iso(latest.get("updated_at"))
    return (up is None or merged_at_dt is None or up <= merged_at_dt)

def all_check_runs_passed_before_merge(check_runs_obj: dict, merged_at_dt):
    """
    Evaluate GitHub Actions / app check runs:
      - If there are no runs present, return True (nothing failed).
      - If runs exist, require each to have conclusion in {'success','neutral','skipped'} and (if timestamps are present) completed_at <= merged_at.
    (This is used as a fallback when cannot read required branch-protection contexts).
    """
    runs = (check_runs_obj or {}).get("check_runs")
    if runs is None:
        return True
    if len(runs) == 0:
        return True
    ok = True
    for r in runs:
        concl = r.get("conclusion")
        if concl not in {"success", "neutral", "skipped"}:
            ok = False
            break
        comp = parse_iso(r.get("completed_at"))
        if comp is not None and merged_at_dt is not None and comp > merged_at_dt:
            ok = False
            break
    return ok

def compute_row(item: dict) -> dict:
    """
    Convert one enriched PR record into a 'flat' row for the CSV.
    """
    pr = item["pr"]
    number = pr["number"]
    title = pr.get("title", "")
    author = pr.get("user", {}).get("login", "")
    merged_at = pr.get("merged_at", "")
    merged_at_dt = parse_iso(merged_at)
    base = pr.get("base", {}).get("ref", "")
    head = pr.get("head", {}).get("ref", "")
    merge_sha = pr.get("merge_commit_sha") or pr.get("head", {}).get("sha", "")

    # Check if code review passed
    reviews = item.get("reviews", [])
    cr_passed = review_approved_before_merge(
        reviews if isinstance(reviews, list) else [],
        merged_at_dt
    )

    # Check if checks passed before merge
    required_ctxs = required_contexts_from_protection(item.get("required_status_checks"))
    combined_status = item.get("combined_status", {})
    #If possible to read required contexts from branch protection, must ensure those contexts are success.
    combined_state = safe_get(item, "combined_status", "state", default="unknown")
    combined_ok = combined_state in SUCCESS_STATES
    runs_ok = all_check_runs_passed_before_merge(item.get("check_runs", {}), merged_at_dt)

    if required_ctxs:
        # Strict path: every required context must show success (and not after merge)
        checks_passed = all(
            latest_context_state_before_merge(combined_status, ctx, merged_at_dt)
            for ctx in required_ctxs
        )
    else:
        # Fallback path (no visibility into required contexts):
        #   combined status is success AND check runs look good,
        #   both evaluated against (if present) their timestamps.
        checks_passed = (combined_ok and runs_ok)

    # Return a single row of normalized fields for the CSV
    return {
        "number": number,
        "title": title,
        "author": author,
        "base": base,                 # branch merged into
        "head": head,                 # source branch
        "merge_commit": merge_sha,    
        "merged_at": merged_at,       # ISO string
        "CR_PASSED": bool(cr_passed), # passed code review or not
        "CHECKS_PASSED": bool(checks_passed), # passed checks or notå
    }

def main():
    # --- CLI args: input JSON (from extract.py) and output CSV path ---
    ap = argparse.ArgumentParser(description="Transform enriched PR JSON -> CSV audit")
    ap.add_argument("--input", default="data/raw/enriched.json")
    ap.add_argument("--output", default="data/processed/Pull_Records_audit.csv")
    args = ap.parse_args()

    # Read the enriched JSON produced by extract.py
    data = json.loads(Path(args.input).read_text(encoding="utf-8"))

    # Compute one CSV row per PR
    rows = [compute_row(item) for item in data]

    # Build a DataFrame and sort by merge date for nicer reading
    df = pd.DataFrame(rows).sort_values("merged_at")

    # Ensure output folder exists and write CSV
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"[OK] Wrote {len(df)} rows to {out}")

if __name__ == "__main__":
    main()