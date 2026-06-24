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
import concurrent.futures
import xml.etree.ElementTree as ET
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests

try:
    import feedparser  # only needed for the We Work Remotely / Working Nomads RSS source
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
ATS_TIMEOUT = 15          # shorter per-request timeout for ATS fetches
ATS_WORKERS = 10          # concurrent threads for ATS company fetching
COMPANIES_FILE = "companies.txt"
TOP_N = 35                      # how many jobs to include in the email
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
    "appium", "requests", "restassured", "rest assured", "rest-assured",
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


def fetch_himalayas(limit=None):
    """Pull QA/SDET roles from Himalayas free public API (no key required)."""
    queries = ["QA", "SDET", "test automation"]
    seen_urls = set()
    out = []
    for q in queries:
        try:
            url = f"https://himalayas.app/jobs/api/search?q={requests.utils.quote(q)}&seniority=Senior"
            r = requests.get(url, headers=UA, timeout=TIMEOUT)
            r.raise_for_status()
            for j in r.json().get("jobs", []):
                apply_url = j.get("applicationLink") or j.get("guid") or j.get("url", "")
                if not apply_url or apply_url in seen_urls:
                    continue
                seen_urls.add(apply_url)
                tags = []
                for field in ("categories", "keywords"):
                    val = j.get(field, [])
                    if isinstance(val, list):
                        tags.extend(str(v).lower() for v in val)
                    elif isinstance(val, str):
                        tags.append(val.lower())
                out.append({
                    "title": j.get("title", ""),
                    "company": j.get("companyName", ""),
                    "url": apply_url,
                    "tags": tags,
                    "location": ", ".join(j.get("locationRestrictions", [])) or "Remote",
                    "posted": _parse_date(j.get("pubDate") or j.get("publishedDate")),
                    "description": j.get("excerpt") or j.get("description", ""),
                    "source": "Himalayas",
                })
                if limit and len(out) >= limit:
                    return out
        except Exception as e:
            print(f"[warn] himalayas query '{q}': {e}")
    return out


def fetch_working_nomads(limit=None):
    """Pull remote jobs from Working Nomads public JSON API (no key required)."""
    out = []
    try:
        r = requests.get("https://www.workingnomads.com/api/exposed_jobs/",
                         headers=UA, timeout=TIMEOUT)
        r.raise_for_status()
        jobs = r.json()
        if not isinstance(jobs, list):
            jobs = jobs.get("jobs", [])
        for j in jobs:
            tags = []
            cats = j.get("tags", [])
            if isinstance(cats, list):
                tags = [str(c).lower() for c in cats]
            out.append({
                "title": j.get("title", ""),
                "company": j.get("company", ""),
                "url": j.get("url", "") or j.get("external_link", ""),
                "tags": tags,
                "location": j.get("location", "Remote"),
                "posted": _parse_date(j.get("pub_date") or j.get("created_at")),
                "description": j.get("description", ""),
                "source": "WorkingNomads",
            })
            if limit and len(out) >= limit:
                break
    except Exception as e:
        print(f"[warn] working_nomads JSON failed: {e}")
        # Fallback: try RSS if feedparser is available
        if feedparser is not None:
            try:
                feed = feedparser.parse("https://www.workingnomads.com/feed/")
                for e in feed.entries:
                    out.append({
                        "title": e.get("title", ""),
                        "company": "",
                        "url": e.get("link", ""),
                        "tags": [],
                        "location": "Remote",
                        "posted": _parse_date(e.get("published")),
                        "description": re.sub("<[^>]+>", " ", e.get("summary", "")),
                        "source": "WorkingNomads",
                    })
                    if limit and len(out) >= limit:
                        break
            except Exception as e2:
                print(f"[warn] working_nomads RSS also failed: {e2}")
    return out


# --------------------------------------------------------------------------- #
# ATS PUBLIC FEED LAYER
# --------------------------------------------------------------------------- #

_REMOTE_KEYWORDS = ("remote", "anywhere", "worldwide", "distributed",
                    "work from home", "wfh", "fully remote", "location flexible")


def _looks_remote(job):
    """Return True if the job appears to be remote based on location/description."""
    # Honour explicit _ashby_remote flag set during Ashby parsing
    if job.get("_ashby_remote"):
        return True
    blob = (job.get("location", "") + " " + job.get("description", "")).lower()
    return any(kw in blob for kw in _REMOTE_KEYWORDS)


def _strip_html(text):
    return re.sub("<[^>]+>", " ", text or "")


def _gh_fetch(slug):
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    r = requests.get(url, headers=UA, timeout=ATS_TIMEOUT)
    r.raise_for_status()
    out = []
    for j in r.json().get("jobs", []):
        loc = j.get("location", {})
        loc_name = loc.get("name", "") if isinstance(loc, dict) else str(loc)
        out.append({
            "title": j.get("title", ""),
            "company": slug,
            "url": j.get("absolute_url", ""),
            "tags": [d.get("name", "").lower() for d in j.get("departments", [])],
            "location": loc_name,
            "posted": _parse_date(j.get("updated_at")),
            "description": _strip_html(j.get("content", "")),
            "source": f"ATS:greenhouse/{slug}",
        })
    return out


def _lever_fetch(slug):
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    r = requests.get(url, headers=UA, timeout=ATS_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return []
    out = []
    for j in data:
        cats = j.get("categories", {})
        location = cats.get("location", "") or cats.get("allLocations", [""])[0] if cats.get("allLocations") else ""
        out.append({
            "title": j.get("text", ""),
            "company": slug,
            "url": j.get("hostedUrl", ""),
            "tags": [cats.get("team", "").lower(), cats.get("department", "").lower()],
            "location": location,
            "posted": _parse_date(j.get("createdAt")),
            "description": _strip_html(j.get("descriptionPlain") or j.get("description", "")),
            "source": f"ATS:lever/{slug}",
        })
    return out


def _ashby_fetch(slug):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
    r = requests.get(url, headers=UA, timeout=ATS_TIMEOUT)
    r.raise_for_status()
    out = []
    for j in r.json().get("jobs", []):
        is_remote = j.get("isRemote", False) or j.get("workplaceType", "") == "Remote"
        loc = j.get("location", "") or ("Remote" if is_remote else "")
        dept = j.get("department", "")
        tags = [dept.lower()] if isinstance(dept, str) and dept else []
        out.append({
            "title": j.get("title", ""),
            "company": slug,
            "url": j.get("applyUrl") or j.get("jobUrl", ""),
            "tags": tags,
            "location": loc,
            "posted": _parse_date(j.get("publishedDate")),
            "description": _strip_html(j.get("descriptionHtml") or j.get("description", "")),
            "source": f"ATS:ashby/{slug}",
            "_ashby_remote": is_remote,
        })
    return out


def _workable_fetch(slug):
    url = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"
    r = requests.post(url, headers={**UA, "Content-Type": "application/json"},
                      json={"query": "", "location": [], "department": [],
                            "worktype": ["telecommute"]},
                      timeout=ATS_TIMEOUT)
    r.raise_for_status()
    out = []
    for j in r.json().get("results", []):
        loc_obj = j.get("location", {})
        loc = loc_obj.get("city", "") if isinstance(loc_obj, dict) else str(loc_obj)
        out.append({
            "title": j.get("title", ""),
            "company": slug,
            "url": f"https://apply.workable.com/{slug}/j/{j.get('shortcode', '')}",
            "tags": [j.get("department", "").lower()],
            "location": loc or "Remote",
            "posted": _parse_date(j.get("created_at")),
            "description": _strip_html(j.get("description", "")),
            "source": f"ATS:workable/{slug}",
        })
    return out


def _recruitee_fetch(slug):
    url = f"https://{slug}.recruitee.com/api/offers/"
    r = requests.get(url, headers=UA, timeout=ATS_TIMEOUT)
    r.raise_for_status()
    out = []
    for j in r.json().get("offers", []):
        out.append({
            "title": j.get("title", ""),
            "company": slug,
            "url": j.get("careers_url", ""),
            "tags": [t.lower() for t in j.get("tags", [])],
            "location": j.get("location", "Remote"),
            "posted": _parse_date(j.get("created_at")),
            "description": _strip_html(j.get("description", "")),
            "source": f"ATS:recruitee/{slug}",
        })
    return out


def _smartrecruiters_fetch(slug):
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    r = requests.get(url, headers=UA, timeout=ATS_TIMEOUT)
    r.raise_for_status()
    out = []
    for j in r.json().get("content", []):
        loc = j.get("location", {})
        loc_str = loc.get("city", "") if isinstance(loc, dict) else str(loc)
        if isinstance(loc, dict) and loc.get("remote"):
            loc_str = "Remote"
        out.append({
            "title": j.get("name", ""),
            "company": slug,
            "url": j.get("ref", ""),
            "tags": [j.get("department", {}).get("label", "").lower()],
            "location": loc_str,
            "posted": _parse_date(j.get("releasedDate")),
            "description": "",
            "source": f"ATS:smartrecruiters/{slug}",
        })
    return out


def _personio_fetch(slug):
    out = []
    for tld in ("de", "com"):
        try:
            url = f"https://{slug}.jobs.personio.{tld}/xml?language=en"
            r = requests.get(url, headers=UA, timeout=ATS_TIMEOUT)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            for pos in root.findall(".//position"):
                def _t(tag):
                    el = pos.find(tag)
                    return el.text.strip() if el is not None and el.text else ""
                apply_url = _t("applicationUrl") or _t("url")
                out.append({
                    "title": _t("title") or _t("name"),
                    "company": slug,
                    "url": apply_url,
                    "tags": [_t("recruiting-category").lower()],
                    "location": _t("office"),
                    "posted": None,
                    "description": _strip_html(_t("description")),
                    "source": f"ATS:personio/{slug}",
                })
            if out:
                return out
        except Exception:
            continue
    return out


_ATS_FETCHERS = {
    "greenhouse": _gh_fetch,
    "lever": _lever_fetch,
    "ashby": _ashby_fetch,
    "workable": _workable_fetch,
    "recruitee": _recruitee_fetch,
    "smartrecruiters": _smartrecruiters_fetch,
    "personio": _personio_fetch,
}


def _autodetect_ats(slug):
    """Probe greenhouse → lever → ashby. Return (ats_name, jobs) or (None, [])."""
    for name in ("greenhouse", "lever", "ashby"):
        try:
            jobs = _ATS_FETCHERS[name](slug)
            if jobs:
                return name, jobs
        except Exception:
            pass
    return None, []


def _parse_companies_file():
    """Read companies.txt and return list of (line_index, ats_or_None, slug, original_line)."""
    if not os.path.exists(COMPANIES_FILE):
        return []
    entries = []
    with open(COMPANIES_FILE, encoding="utf-8") as f:
        for i, line in enumerate(f):
            stripped = line.rstrip("\n")
            clean = stripped.strip()
            if not clean or clean.startswith("#"):
                entries.append((i, None, None, stripped))
                continue
            if ":" in clean:
                ats, _, slug = clean.partition(":")
                ats = ats.strip().lower()
                slug = slug.strip()
                entries.append((i, ats, slug, stripped))
            else:
                # bare slug — auto-detect
                entries.append((i, "auto", clean, stripped))
    return entries


def _rewrite_companies_file(entries):
    """Write updated entries back to companies.txt."""
    lines = []
    for _, ats, slug, original in entries:
        if ats is None:
            lines.append(original)
        else:
            lines.append(original)
    with open(COMPANIES_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def _fetch_one_company(entry):
    """Fetch jobs for one entry. Returns (updated_entry, jobs, error_str_or_None)."""
    idx, ats, slug, original = entry
    if ats is None or slug is None:
        return entry, [], None

    if ats == "auto":
        # auto-detect and cache result
        try:
            detected, jobs = _autodetect_ats(slug)
            if detected:
                new_line = f"detected_{detected}:{slug}"
                new_entry = (idx, f"detected_{detected}", slug, new_line)
                return new_entry, jobs, None
            else:
                return entry, [], f"auto-detect found nothing for '{slug}'"
        except Exception as e:
            return entry, [], f"auto-detect error for '{slug}': {e}"

    # Handle "detected_ats" prefix written by previous auto-detect
    real_ats = ats.replace("detected_", "") if ats.startswith("detected_") else ats
    fetcher = _ATS_FETCHERS.get(real_ats)
    if fetcher is None:
        return entry, [], f"unknown ATS '{ats}' for slug '{slug}'"
    try:
        jobs = fetcher(slug)
        return entry, jobs, None
    except Exception as e:
        return entry, [], f"ATS:{ats}/{slug}: {e}"


def fetch_ats(limit=None):
    """Fetch jobs from all companies in companies.txt using public ATS APIs."""
    entries = _parse_companies_file()
    company_entries = [(i, a, s, o) for i, a, s, o in entries if a is not None and s is not None]

    if not company_entries:
        print("[warn] ats: companies.txt is empty or missing")
        return []

    all_jobs = []
    updated_entries = list(entries)  # track rewrites from auto-detect
    errors = []
    resolved = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=ATS_WORKERS) as pool:
        futures = {pool.submit(_fetch_one_company, e): e for e in company_entries}
        for future in concurrent.futures.as_completed(futures):
            try:
                new_entry, jobs, err = future.result()
                if err:
                    errors.append(err)
                else:
                    # Filter to remote-only jobs
                    remote_jobs = [j for j in jobs if _looks_remote(j)]
                    # Strip internal helper keys
                    for j in remote_jobs:
                        j.pop("_ashby_remote", None)
                    all_jobs.extend(remote_jobs)
                    if jobs:
                        resolved += 1
                    print(f"[ok]   ats:{new_entry[1]}/{new_entry[2]}: "
                          f"{len(jobs)} total, {len(remote_jobs)} remote")
                    # Update entry if auto-detect rewrote it
                    orig_idx = new_entry[0]
                    updated_entries[orig_idx] = new_entry
            except Exception as exc:
                errors.append(str(exc))

    for err in errors:
        print(f"[warn] {err}")

    # Persist any auto-detect rewrites
    if any(updated_entries[i][3] != entries[i][3]
           for i in range(len(entries)) if i < len(updated_entries)):
        try:
            _rewrite_companies_file(updated_entries)
        except Exception as e:
            print(f"[warn] could not rewrite companies.txt: {e}")

    print(f"[info] ats: {resolved}/{len(company_entries)} companies resolved, "
          f"{len(all_jobs)} remote jobs collected")
    if limit:
        return all_jobs[:limit]
    return all_jobs


SOURCES = [
    fetch_remoteok,
    fetch_remotive,
    fetch_arbeitnow,
    fetch_jobicy,
    fetch_wwr,
    fetch_himalayas,
    fetch_working_nomads,
    fetch_ats,
]


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
