# Daily Remote QA/SDET Job Digest

A zero-cost bot that emails you the latest remote QA / SDET / test-automation
jobs every morning at **8:00 AM IST**. Runs entirely on GitHub Actions — your
laptop does not need to be on.

## How it works
1. Pulls jobs from free public APIs/feeds: **RemoteOK, Remotive, Arbeitnow,
   Jobicy, We Work Remotely**. No API keys needed for any of these.
2. De-duplicates and scores every job against your QA/SDET profile in pure
   Python — title/skill matching, recency boost, seniority penalties.
3. Optionally re-ranks the top jobs with Gemini's **free** API for a one-line
   "why it fits" note. Skipped automatically if you don't add a key.
4. Skips jobs it already emailed on previous days (persisted in `seen.json`).
5. Emails an HTML digest via Gmail SMTP over SSL.

## Why not LinkedIn or Indeed?
Neither offers a free API. Scraping either violates their Terms of Service
and reliably gets GitHub Actions runner IPs blocked within hours. The five
sources above cover the bulk of legitimate remote QA/SDET postings without
any ToS concerns.

## Setup (~10 minutes)

### 1. Push to a private GitHub repo
```
job_hunter.py
requirements.txt
.github/workflows/daily-jobs.yml
```
Make the repo **private** so your secrets stay private.

### 2. Create a Gmail App Password
- Google Account → Security → turn on **2-Step Verification** (required first).
- Security → **App passwords** → generate one, select Mail / Other.
- Copy the 16-character password — that's `GMAIL_APP_PASSWORD`.
  (Your normal Gmail password will **not** work — Google blocks it.)

### 3. (Optional) Get a free Gemini API key
- **aistudio.google.com → Get API key** → create key. No credit card needed.
- This unlocks the "why it fits" one-liner on each job. Works fine without it.
- Free quota is plenty: the bot makes ONE Gemini call per day.

### 4. Add repo secrets
Repo → **Settings → Secrets and variables → Actions → New repository secret**:

| Secret | Value |
|---|---|
| `GMAIL_USER` | Gmail address that sends the digest |
| `GMAIL_APP_PASSWORD` | 16-char app password from step 2 |
| `MAIL_TO` | Delivery address (can equal `GMAIL_USER`; comma-separate for multiple) |
| `GEMINI_API_KEY` | Optional — your AI Studio key |

### 5. Test it now
Repo → **Actions → Daily QA/SDET Job Digest → Run workflow**.
Check your inbox in ~1 minute. After that it runs daily at 8 AM IST automatically.

## Local dry-run (no secrets needed)
```bash
pip install requests feedparser
python job_hunter.py --dry-run          # full pipeline, prints digest, writes digest.html
python job_hunter.py --dry-run --limit 10  # cap 10 jobs per source for a fast test
```

## Schedule & timezone
The cron `30 2 * * *` fires at 02:30 UTC, which is 08:00 IST (UTC+5:30).
To change the time, edit the `cron:` line in `.github/workflows/daily-jobs.yml`
using UTC. [crontab.guru](https://crontab.guru) is handy for verifying.

## Tuning the keyword lists
Open `job_hunter.py` and edit the constants near the top:

| Constant | Purpose |
|---|---|
| `ROLE_CORE` | Hard gate — a job needs one of these terms to appear at all |
| `SKILLS_HIGH` | +3 pts each — your primary strengths |
| `SKILLS_MED` | +1.5 pts each — secondary skills |
| `NICE_TO_HAVE` | +1 pt each — bonus signals |
| `NEGATIVE` | −8 pts each — penalises intern/junior/onsite roles |
| `TOP_N` | Max jobs per email (default 25) |
| `RECENT_DAYS` | Max age of jobs to include (default 4 days) |

## Location filter
Set `LOCATION_FILTER=1` in your `.env` (or as a repo secret) to drop jobs
that explicitly exclude India/APAC (e.g. "US only", "EU only").
Jobs with unspecified or worldwide locations are always kept. Off by default.
