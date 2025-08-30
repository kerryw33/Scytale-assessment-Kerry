## Scytale PR Audit

**Author:** Kerry-Lynn Whyte 

This project fetches merged pull requests (PRs) from a single GitHub repository in the `Scytale-exercise` organisation, then enriches them with review and status check information, and produces a CSV audit report.

## Project structure
├── extract.py          # Fetches PRs, reviews, statuses, and checks from GitHub (raw PR data from GitHub.)
├── transform.py        # Processes enriched data to CSV, and analyses data - whether reviews/checks passed
├── requirements.txt    # Python dependencies 
├── data/
│   ├── raw/            # Raw JSON outputs (pages, merged_prs.json, enriched.json)
│   └── processed/      # Final CSV report
└── README.md


## Setup 
1. Clone the repo and enter the folder:
   ```bash
   git clone https://github.com/<your-fork>/SCYTALE_Assessment.git
   cd SCYTALE_Assessment

2. Create and Activate virtual environment:
    python3 -m venv .venv
    source .venv/bin/activate   # macOS/Linux
    .venv\Scripts\activate      # Windows (PowerShell)

3. Install dependencies
    pip install -r requirements.txt

4. Create .env file with Personal Access Token (PAT)
    GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxxxxx


## Usage

If you are running the scripts multiple times and want a clean slate, remove old output files first:

```bash
rm -rf data/raw/* data/processed/*

# 1. Extract raw PR data
Fetches closed PRs (handles pagination) and filters to merged PRs in the date range specifed (365 days by default if not specified). Enriches each merged PR with reviews and checks.

```bash
python3 extract.py --owner Scytale-exercise --repo scytale-repo3 --since 2000-01-01
```

Outputs:
- `data/raw/pulls_p*.json` — raw API pages of closed PRs  
- `data/raw/merged_prs.json` — merged PRs in the chosen window  
- `data/raw/enriched.json` — enriched PRs (reviews + checks) 

# 2. Transform into CSV report
Processes the enriched JSON into a CSV audit report.

```bash
python3 transform.py --input data/raw/enriched.json --output data/processed/PR_audit.csv
```

Output columns:
- `number` — PR number  
- `title` — PR title  
- `author` — PR author login  
- `merged_at` — merge date  
- `CR_PASSED` — `True` if ≥1 reviewer approved before merge  
- `CHECKS_PASSED` — `True` if all required checks passed before merge  


---

## Example Output

```csv
number,title,author,merged_at,CR_PASSED,CHECKS_PASSED
1,"Bug fixed",pedro,2025-05-10T14:21:00Z,True,True
2,"Code review",calcaraz,2025-05-12T09:02:00Z,True,False
```

---

## Notes

- If the repo has  **no merged PRs** in the given window - `merged_prs.json` and `enriched.json` will be empty.  
- Branch protection rules (required checks) are only visible if your token has rights; otherwise the script falls back to combined statuses and check runs.  
- Case and punctuation in `--repo` must match the repository name exactly (case sensitive).  
