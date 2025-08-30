# Kerry-Lynn Whyte
# 30/08/2025
# A class that fetches pull requests (PRs), reviews, statuses, and checks from GitHub (raw PR data from GitHub.)
import os
import time
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests
try:
    # Load variables from a .env file if present (e.g., GITHUB_TOKEN)
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # it's ok if  python-dotenv isn't installed - as long as env vars are set another way
    pass

API = "https://api.github.com"  # Base URL for GitHub REST API

# obtains date and formats 
def _to_utc_iso(dt: datetime) -> str:
    """Format a datetime as an ISO8601 UTC string: YYYY-MM-DDTHH:MM:SSZ."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _parse_iso(s: str | None) -> datetime | None:
    """
    Parse ISO8601 strings (usually end with 'Z' for UTC) and convert into a standardized ISO8601 string in UTC.
    """
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)

def normalize_window(since: str | None, until: str | None) -> tuple[str, str]:
    """
    Conversion is optional: --since/--until (YYYY-MM-DD) into full ISO UTC strings.
    Default: since = 365 days ago, until = now.
    """
    now = datetime.now(timezone.utc)
    s_dt = datetime.fromisoformat(since).replace(tzinfo=timezone.utc) if since else (now - timedelta(days=365))
    u_dt = datetime.fromisoformat(until).replace(tzinfo=timezone.utc) if until else now
    return _to_utc_iso(s_dt), _to_utc_iso(u_dt)

# HTTP client 
class GitHubHTTP:
    """
    Minimal authenticated HTTP client for GitHub with gentle retry/backoff.
    - Uses a PAT for authentication.
    - Adds required headers to every request - expects (API version, Accept type, User-Agent).
    - Retries on rate-limit responses to handle temporary failures
      and GitHub's rate limiting.
    """
    def __init__(self, token: str | None = None, max_retries: int = 3, backoff: float = 1.2):
        t = token or os.getenv("GITHUB_TOKEN") 
        if not t:
            raise RuntimeError("GITHUB_TOKEN not set. Put it in .env or export it.") # fails if no token found
        self.sess = requests.Session() # Create a persistent HTTP session 
        self.sess.headers.update({
            "Accept": "application/vnd.github+json", # accept JSON responses
            "Authorization": f"Bearer {t}",         # Authenticate using the token
            "X-GitHub-Api-Version": "2022-11-28",   
            "User-Agent": "scytale-two-script"  # helps GitHub identify the client
        })
        # retry settings
        self.max_retries = max_retries 
        self.backoff = backoff

    def get(self, url: str, params: dict | None = None) -> requests.Response:
        """
        Perform a GET request with simple retry logic:
        - If rate limit is hit, wait until reset or back off (and try again).
        - Otherwise, if OK - return the response; else back off and retry a few times.
        """
        for attempt in range(self.max_retries):
            resp = self.sess.get(url, params=params)
            # Handle rate limiting 
            if resp.status_code == 403 and "rate limit" in (resp.text or "").lower():
                reset = resp.headers.get("X-RateLimit-Reset")
                # If GitHub states when limit resets -  sleep until then but  if not - exponential-ish backoff
                wait_s = max(0, int(reset) - int(time.time()) + 1) if (reset and reset.isdigit()) else int((attempt+1)*self.backoff*5)
                time.sleep(wait_s)
                continue
            if resp.ok:
                return resp
            # Non-OK and not rate-limited: short backoff then retry
            time.sleep(int((attempt+1)*self.backoff))
        # If all retries failed, raise the HTTP error
        resp.raise_for_status()

# pagination 
def parse_next_link(link_header: str | None) -> str | None:
    """
    Parse the HTTP 'Link' header returned by GitHub to find the URL for the next page.
    Example header contains segments such as: <...page=2>; rel="next"
    """
    if not link_header:
        return None
    for part in link_header.split(","):
        segs = [p.strip() for p in part.split(";")]
        if len(segs) >= 2 and segs[1] == 'rel="next"':
            return segs[0].lstrip("<").rstrip(">")
    return None

def list_closed_pr_pages(http: GitHubHTTP, owner: str, repo: str, per_page: int = 100):
    """
    Generator that yields (page_index, payload_list) for CLOSED PRs.
    Follows pagination via Link headers until there is no 'next' page.
    - Sort by 'updated' desc to get most recent PRs first.
    """
    url = f"{API}/repos/{owner}/{repo}/pulls"
    params = {"state": "closed", "per_page": per_page, "sort": "updated", "direction": "desc"}
    page_idx = 1
    while url:
        resp = http.get(url, params=params if page_idx == 1 else None)
        yield page_idx, resp.json()
        url = parse_next_link(resp.headers.get("Link"))  # if None, loop ends
        page_idx += 1

# per-PR enrichers - list reviews 
def list_reviews(http: GitHubHTTP, owner: str, repo: str, pr_number: int):
    """Return the list of reviews (APPROVED/COMMENTED/CHANGES_REQUESTED) for a PR."""
    return http.get(f"{API}/repos/{owner}/{repo}/pulls/{pr_number}/reviews").json()

def get_combined_status(http: GitHubHTTP, owner: str, repo: str, sha: str):
    """
    Get the 'combined status' for a commit
    Returns a top-level 'state' and a list of statuses with context and updated_at.
    """
    return http.get(f"{API}/repos/{owner}/{repo}/commits/{sha}/status").json()

def list_check_runs(http: GitHubHTTP, owner: str, repo: str, sha: str):
    """
    Get 'check runs' for a commit 
    Each run has conclusion/status and completed_at timestamps.
    """
    return http.get(f"{API}/repos/{owner}/{repo}/commits/{sha}/check-runs").json()

def get_required_status_contexts(http: GitHubHTTP, owner: str, repo: str, branch: str):
    """
    Read required status checks from branch protection (if visible/allowed).
    Not all tokens/repos permit this
    """
    return http.get(f"{API}/repos/{owner}/{repo}/branches/{branch}/protection/required_status_checks").json()

# main method
def main():
    # CLI definition: owner and repo are required but  date filters optional    
    ap = argparse.ArgumentParser(description="Extract merged PRs (paged) + reviews/checks into data/raw/")
    ap.add_argument("--owner", required=True, help="Organization/user (e.g. Scytale-exercise)")
    ap.add_argument("--repo", required=True, help="Repository name (e.g. scytale-repo3)")
    ap.add_argument("--since", default=None, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--until", default=None, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--per-page", type=int, default=100, help="Items per page (<=100)")
    args = ap.parse_args()

    # Convert date filters into consistent ISO strings and datetime objects
    since_iso, until_iso = normalize_window(args.since, args.until)
    since_dt, until_dt = _parse_iso(since_iso), _parse_iso(until_iso)

    # Ensure output folder exists
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Create the authenticated HTTP client
    http = GitHubHTTP()

    #  Pull CLOSED PRs, then filter: keep only those with merged_at in the date window 
    merged_prs: list[dict] = []
    total_closed = 0
    total_merged_anytime = 0
    total_merged_in_window = 0

    for page_idx, payload in list_closed_pr_pages(http, args.owner, args.repo, per_page=args.per_page):
        # Save the raw API page 
        (raw_dir / f"pulls_p{page_idx}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        total_closed += len(payload)

        for pr in payload:
            m_at = pr.get("merged_at")
            if m_at:
                total_merged_anytime += 1
                m_dt = _parse_iso(m_at)
                # Keep only PRs merged within the specified window
                if (since_dt is None or m_dt >= since_dt)