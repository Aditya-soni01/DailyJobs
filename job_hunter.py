#!/usr/bin/env python3
"""
job_hunter.py — Daily remote QA/SDET job digest.

Pipeline:
  1. Pull jobs from several FREE public job APIs/RSS feeds (no keys needed).
  2. De-duplicate across sources.
  3. Score each job against a QA/SDET profile (pure Python, no LLM).
  4. (Optional) Re-rank the top jobs with Gemini free-tier for a one-line
     "why it fits" — only runs if GEMINI_API_KEY is set.
  5. Skip jobs already emailed on previous days (seen.json).
  6. Email an HTML digest via Gmail SMTP.

Run locally with --dry-run to test without sending email or needing secrets.
"""

import os
import re
import json
import html
import time
import smtplib
import argparse
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests

try:
    import feedparser  # only needed for the We Work Remotely RSS source
except ImportError:
    feedparser = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #

UA = {"User-Agent": "Mozilla/5.0 (job-digest-bot; personal use)"}
TIMEOUT = 25
TOP_N = 25                       # how many jobs to include in the email
RECENT_DAYS = 4                  # only consider jobs posted within this window
SEEN_FILE = "seen.json"
SEEN_TTL_DAYS = 21               # forget jobs after this many days so the file stays small

# When True, downrank/drop jobs that explicitly exclude India/APAC.
# Flip via env var LOCATION_FILTER=1 or set to True here.
LOCATION_FILTER = os.environ.get("LOCATION_FILTER", "0") in ("1", "true", "yes")

# Terms that signal the role is restricted to a region that excludes India/APAC.
_LOCATION_EXCLUDE = ["us only", "us-based", "us citizens", "us residents",
                     "eu only", "europe only", "uk only", "must be based in",
                     "authorized to work in the us", "authorized to work in us"]

# ---- Your profile. Edit these lists to retune relevance. ------------------ #
# A job must contain at least one ROLE_CORE term (in title/tags/description)
# to even be considered. This is what keeps generic "Software Engineer" roles
# from flooding the digest.
ROLE_CORE = [
    "qa", "quality assurance", "quality engineer", "sdet", "tester",
    "test engineer", "test automation", "automation engineer",
    "software engineer in test", "software development engineer in test",
    "test analyst", "qa engineer", "qa automation",
]
SKILLS_HIGH = [
    "python", "pytest", "selenium", "robot framework", "playwright",
    "cypress", "api testing", "rest api", "api automation", "postman",
    "appium", "requests",
]
SKILLS_MED = [
    "ci/cd", "jenkins", "github actions", "sql", "jira", "agile", "scrum",
    "automation framework", "regression", "docker", "kubernetes", "git",
    "json", "android", "adb",
]
NICE_TO_HAVE = [
    "genai", "generative ai", "llm", "ai", "performance testing",
    "jmeter", "load testing", "burp suite", "security testing",
]
NEGATIVE = [
    "unpaid", "internship", "intern ", "commission only", "no remote",
    "onsite only", "manual only", "entry level", "junior",  # candidate is senior (9 yrs)
]

# --------------------------------------------------------------------------- #
# SOURCES — each returns a list of normalized dicts:
#   {title, company, url, tags(list[str]), location, posted(datetime|None),
#    description, source}
# Every fetcher is wrapped so one dead source never kills the whole run.
# --------------------------------------------------------------------------- #

def _parse_date(value):
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            return dt.datetime.utcfromtimestamp(int(value))
        except Exception:
            return None
    s = str(value)
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%a, %d %b %Y %H:%M:%S %z"):
        try:
            d = dt.datetime.strptime(s.split(".")[0].replace("Z", "+0000"), fmt)
            return d.replace(tzinfo=None)
        except Exception:
            continue
    return None


def fetch_remoteok(limit=None):
    r = requests.get("https://remoteok.com/api", headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    out = []
    for j in data:
        if not isinstance(j, dict) or "position" not in j:
            continue  # first element is legal metadata
        out.append({
            "title": j.get("position", ""),
            "company": j.get("company", ""),
            "url": j.get("url", ""),
            "tags": [t.lower() for t in j.get("tags", [])],
            "location": j.get("location", "Remote"),
            "posted": _parse_date(j.get("date")),
            "description": j.get("description", ""),
            "source": "RemoteOK",
        })
        if limit and len(out) >= limit:
            break
    return out


def fetch_remotive(limit=None):
    r = requests.get("https://remotive.com/api/remote-jobs?limit=200",
                     headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    out = []
    for j in r.json().get("jobs", []):
        out.append({
            "title": j.get("title", ""),
            "company": j.get("company_name", ""),
            "url": j.get("url", ""),
            "tags": [t.lower() for t in j.get("tags", [])],
            "location": j.get("candidate_required_location", "Remote"),
            "posted": _parse_date(j.get("publication_date")),
            "description": j.get("description", ""),
            "source": "Remotive",
        })
        if limit and len(out) >= limit:
            break
    return out


def fetch_arbeitnow(limit=None):
    r = requests.get("https://www.arbeitnow.com/api/job-board-api",
                     headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    out = []
    for j in r.json().get("data", []):
        if not j.get("remote", False):
            continue
        out.append({
            "title": j.get("title", ""),
            "company": j.get("company_name", ""),
            "url": j.get("url", ""),
            "tags": [t.lower() for t in (j.get("tags", []) + j.get("job_types", []))],
            "location": j.get("location", "Remote"),
            "posted": _parse_date(j.get("created_at")),
            "description": j.get("description", ""),
            "source": "Arbeitnow",
        })
        if limit and len(out) >= limit:
            break
    return out


def fetch_jobicy(limit=None):
    r = requests.get("https://jobicy.com/api/v2/remote-jobs?count=100",
                     headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    out = []
    for j in r.json().get("jobs", []):
        industries = j.get("jobIndustry", [])
        jtypes = j.get("jobType", [])
        tags = [str(t).lower() for t in (industries + jtypes)]
        out.append({
            "title": j.get("jobTitle", ""),
            "company": j.get("companyName", ""),
            "url": j.get("url", ""),
            "tags": tags,
            "location": j.get("jobGeo", "Remote"),
            "posted": _parse_date(j.get("pubDate")),
            "description": j.get("jobExcerpt", ""),
            "source": "Jobicy",
        })
        if limit and len(out) >= limit:
            break
    return out


def fetch_wwr(limit=None):
    if feedparser is None:
        return []
    feeds = [
        "https://weworkremotely.com/categories/remote-programming-jobs.rss",
        "https://weworkremotely.com/categories/remote-devops-sysadmin-jobs.rss",
    ]
    out = []
    for url in feeds:
        feed = feedparser.parse(url)
        for e in feed.entries:
            title = e.get("title", "")
            company = ""
            if ":" in title:                       # WWR titles look like "Company: Role"
                company, _, title = title.partition(":")
            out.append({
                "title": title.strip(),
                "company": company.strip(),
                "url": e.get("link", ""),
                "tags": [],
                "location": "Remote",
                "posted": _parse_date(e.get("published")),
                "description": re.sub("<[^>]+>", " ", e.get("summary", "")),
                "source": "WeWorkRemotely",
            })
            if limit and len(out) >= limit:
                return out
    return out


SOURCES = [fetch_remoteok, fetch_remotive, fetch_arbeitnow, fetch_jobicy, fetch_wwr]


def gather_jobs(limit=None):
    all_jobs, errors = [], []
    for fn in SOURCES:
        name = fn.__name__.replace("fetch_", "")
        try:
            jobs = fn(limit=limit)
            all_jobs.extend(jobs)
            print(f"[ok]   {name}: {len(jobs)} jobs")
        except Exception as e:                     # noqa: BLE001 — keep going
            errors.append(f"{name}: {e}")
            print(f"[fail] {name}: {e}")
    return all_jobs, errors


# --------------------------------------------------------------------------- #
# DEDUPE + SCORE
# --------------------------------------------------------------------------- #

def _key(job):
    return (re.sub(r"\s+", " ", job["title"].lower()).strip(),
            re.sub(r"\s+", " ", job["company"].lower()).strip())


def dedupe(jobs):
    seen, out = set(), []
    for j in jobs:
        k = _key(j)
        if k in seen or not j["title"]:
            continue
        seen.add(k)
        out.append(j)
    return out


def _count(terms, text):
    return sum(1 for t in terms if t in text)


def _location_excluded(job):
    """Return True if the job explicitly restricts to a region excluding India/APAC."""
    if not LOCATION_FILTER:
        return False
    loc = (job.get("location", "") + " " + job.get("description", "")).lower()
    if any(term in loc for term in _LOCATION_EXCLUDE):
        # Pass-through if job also mentions worldwide/anywhere (contradictory listings)
        if any(ok in loc for ok in ("worldwide", "anywhere", "global", "all countries")):
            return False
        return True
    return False


def score(job):
    title = job["title"].lower()
    tags = " ".join(job["tags"]).lower()
    desc = job["description"].lower()
    blob = f"{title} {tags} {desc}"

    # Hard gate: must look like a QA/test role somewhere.
    if not any(t in blob for t in ROLE_CORE):
        return None

    # Location filter: drop or heavily penalise if restricted region detected.
    if _location_excluded(job):
        return None

    s = 0.0
    s += 10 * _count(ROLE_CORE, title)
    s += 5 * _count(ROLE_CORE, tags)
    s += 2 * min(_count(ROLE_CORE, desc), 2)

    s += 3 * _count(SKILLS_HIGH, blob)
    s += 1.5 * _count(SKILLS_MED, blob)
    s += 1 * _count(NICE_TO_HAVE, blob)
    s -= 8 * _count(NEGATIVE, blob)

    # recency bonus
    if job["posted"]:
        age = (dt.datetime.utcnow() - job["posted"]).days
        if age <= 1:
            s += 4
        elif age <= 3:
            s += 2

    job["score"] = round(s, 1)
    job["matched_skills"] = sorted({
        t for t in (SKILLS_HIGH + SKILLS_MED + NICE_TO_HAVE) if t in blob
    })
    return job


def recent_enough(job):
    if not job["posted"]:
        return True  # keep undated jobs; many RSS items lack reliable dates
    return (dt.datetime.utcnow() - job["posted"]).days <= RECENT_DAYS


def rank(jobs):
    scored = [score(j) for j in jobs]
    scored = [j for j in scored if j and j["score"] > 0 and recent_enough(j)]
    scored.sort(key=lambda j: j["score"], reverse=True)
    return scored


# --------------------------------------------------------------------------- #
# SEEN-STATE (avoid emailing the same job twice)
# --------------------------------------------------------------------------- #

def load_seen():
    try:
        with open(SEEN_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_seen(seen):
    cutoff = (dt.datetime.utcnow() - dt.timedelta(days=SEEN_TTL_DAYS)).isoformat()
    seen = {u: ts for u, ts in seen.items() if ts >= cutoff}
    with open(SEEN_FILE, "w") as f:
        json.dump(seen, f, indent=0)


def filter_unseen(jobs, seen):
    now = dt.datetime.utcnow().isoformat()
    fresh = [j for j in jobs if j["url"] and j["url"] not in seen]
    for j in fresh:
        seen[j["url"]] = now
    return fresh


# --------------------------------------------------------------------------- #
# OPTIONAL GEMINI RE-RANK (free tier). Skipped if no GEMINI_API_KEY.
# --------------------------------------------------------------------------- #

def _gemini_post(url, payload, retries=3):
    """POST to Gemini with exponential backoff on HTTP 429."""
    delay = 1
    for attempt in range(retries):
        r = requests.post(url, headers={"Content-Type": "application/json"},
                          json=payload, timeout=60)
        if r.status_code == 429:
            print(f"[warn] gemini 429 — waiting {delay}s (attempt {attempt + 1})")
            time.sleep(delay)
            delay *= 2
            continue
        r.raise_for_status()
        return r
    raise RuntimeError("Gemini rate-limited after all retries")


def gemini_annotate(jobs):
    key = os.environ.get("GEMINI_API_KEY")
    if not key or not jobs:
        return jobs
    model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{model}:generateContent?key={key}")

    listing = "\n".join(
        f'{i}. {j["title"]} @ {j["company"]} | skills: {", ".join(j["matched_skills"])}'
        for i, j in enumerate(jobs)
    )
    prompt = (
        "You are screening remote jobs for a Senior SDET / QA Automation Engineer "
        "with 9 years of experience in Python, Pytest, Selenium, Robot Framework, "
        "REST API testing, CI/CD (Jenkins), SQL and some GenAI tooling.\n"
        "For each job below, return a JSON array of objects with keys: "
        '"i" (the index), "fit" (0-100 integer), "why" (max 12 words). '
        "Return ONLY the JSON array, no markdown.\n\n" + listing
    )
    try:
        r = _gemini_post(url, {"contents": [{"parts": [{"text": prompt}]}]})
        text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
        text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
        annotations = {a["i"]: a for a in json.loads(text)}
        for i, j in enumerate(jobs):
            a = annotations.get(i, {})
            j["fit"] = a.get("fit")
            j["why"] = a.get("why", "")
        # re-rank by Gemini fit when available
        jobs.sort(key=lambda j: (j.get("fit") is not None, j.get("fit", 0)),
                  reverse=True)
        print(f"[ok]   gemini: annotated {len(annotations)} jobs")
    except Exception as e:                         # noqa: BLE001
        print(f"[warn] gemini skipped: {e}")
    return jobs


# --------------------------------------------------------------------------- #
# EMAIL + HTML
# --------------------------------------------------------------------------- #

def build_html(jobs, errors):
    today = dt.date.today().strftime("%A, %d %b %Y")
    rows = []
    for n, j in enumerate(jobs, 1):
        fit = f'<b>{j["fit"]}%</b> · ' if j.get("fit") is not None else ""
        why = (f'<div style="color:#555;font-size:13px;margin-top:2px">'
               f'{html.escape(j["why"])}</div>') if j.get("why") else ""
        skills = ", ".join(j["matched_skills"][:8])
        posted = j["posted"].strftime("%d %b") if j["posted"] else "—"
        rows.append(f"""
        <tr>
          <td style="padding:10px 8px;border-bottom:1px solid #eee;vertical-align:top">
            <div style="font-size:15px">
              {n}. <a href="{html.escape(j['url'])}" style="color:#0b5cff;text-decoration:none">
              {html.escape(j['title'])}</a>
            </div>
            <div style="color:#222;font-size:13px">{html.escape(j['company'])} ·
              {html.escape(str(j['location']))[:40]} · {j['source']} · {posted}</div>
            <div style="color:#777;font-size:12px;margin-top:2px">{fit}match {j['score']} ·
              {html.escape(skills)}</div>
            {why}
          </td>
        </tr>""")
    err = ""
    if errors:
        err = ('<p style="color:#b00;font-size:12px">Sources that failed today: '
               + "; ".join(html.escape(e) for e in errors) + "</p>")
    return f"""
    <div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:640px;margin:auto">
      <h2 style="margin-bottom:0">Remote QA / SDET jobs — {today}</h2>
      <p style="color:#666;margin-top:4px">{len(jobs)} new matches for your profile.</p>
      <table style="width:100%;border-collapse:collapse">{''.join(rows)}</table>
      {err}
      <p style="color:#999;font-size:11px;margin-top:18px">
        Auto-generated digest. Edit ROLE_CORE / SKILLS_* in job_hunter.py to retune.</p>
    </div>"""


def print_digest(jobs, errors):
    """Print a plain-text digest to stdout for --dry-run mode."""
    print(f"\n{'='*60}")
    print(f"  Remote QA/SDET Digest — {dt.date.today():%d %b %Y}")
    print(f"  {len(jobs)} new roles")
    print(f"{'='*60}\n")
    for n, j in enumerate(jobs, 1):
        posted = j["posted"].strftime("%d %b") if j["posted"] else "—"
        fit = f"  Gemini fit: {j['fit']}% — {j['why']}" if j.get("fit") else ""
        print(f"{n:2}. {j['title']} @ {j['company']}")
        print(f"    {j['location']} · {j['source']} · {posted} · score {j['score']}")
        if j["matched_skills"]:
            print(f"    Skills: {', '.join(j['matched_skills'][:8])}")
        if fit:
            print(f"   {fit}")
        print(f"    {j['url']}")
        print()
    if errors:
        print(f"Sources that failed: {'; '.join(errors)}")


def send_email(html_body, job_count):
    user = os.environ["GMAIL_USER"]
    pw = os.environ["GMAIL_APP_PASSWORD"]
    to = os.environ.get("MAIL_TO", user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[Jobs] {job_count} remote QA/SDET roles — {dt.date.today():%d %b}"
    msg["From"] = user
    msg["To"] = to
    msg.attach(MIMEText("Open in an HTML-capable client.", "plain"))
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(user, pw)
        server.sendmail(user, [a.strip() for a in to.split(",")], msg.as_string())
    print(f"[ok]   emailed {job_count} jobs to {to}")


# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #

def parse_args():
    p = argparse.ArgumentParser(description="Remote QA/SDET job digest")
    p.add_argument("--dry-run", action="store_true",
                   help="Print digest to terminal and write digest.html — no email, no secrets needed")
    p.add_argument("--limit", type=int, default=None, metavar="N",
                   help="Cap jobs fetched per source (useful for fast local runs)")
    return p.parse_args()


def main():
    args = parse_args()

    raw, errors = gather_jobs(limit=args.limit)
    print(f"[info] gathered {len(raw)} raw jobs")

    jobs = rank(dedupe(raw))
    print(f"[info] {len(jobs)} jobs passed the QA/SDET filter")

    seen = load_seen()
    jobs = filter_unseen(jobs, seen)[:TOP_N]
    print(f"[info] {len(jobs)} new jobs after dedupe-vs-history")

    jobs = gemini_annotate(jobs)

    html_body = build_html(jobs, errors) if jobs else (
        "<p>No new QA/SDET remote roles matched today. The pipeline ran fine.</p>"
    )

    if args.dry_run:
        if jobs:
            print_digest(jobs, errors)
        else:
            print("[info] No new jobs matched today.")
        with open("digest.html", "w", encoding="utf-8") as f:
            f.write(html_body)
        print("[dry-run] digest.html written. No email sent.")
        return

    send_email(html_body, len(jobs))
    save_seen(seen)

    with open("digest.html", "w", encoding="utf-8") as f:
        f.write(html_body)


if __name__ == "__main__":
    main()
