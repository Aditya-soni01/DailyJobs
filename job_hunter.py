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
# PYTHON DEVELOPER PROFILE  (parallel pipeline, separate email)
# --------------------------------------------------------------------------- #

PYTHON_MAIL_TO = os.environ.get("PYTHON_MAIL_TO", "ashwarysoni17@gmail.com")
PY_SEEN_FILE = "seen_python.json"
PY_TOP_N = 25

# Role terms that must appear somewhere in the job to be considered.
# Secondary gate: "python" must also appear in the blob (enforced in score_python).
PY_ROLE_CORE = [
    "python developer", "python engineer", "python programmer",
    "senior python", "backend developer", "backend engineer",
    "software engineer", "software developer", "fullstack", "full stack",
    "full-stack", "api developer", "data engineer", "platform engineer",
]
PY_SKILLS_HIGH = [
    "python", "fastapi", "django", "flask", "rest api", "api development",
    "sql", "postgresql", "mysql", "oop", "object oriented", "pytest",
    "requests", "api integration", "json", "modular framework",
]
PY_SKILLS_MED = [
    "ci/cd", "jenkins", "github actions", "docker", "kubernetes", "git",
    "aws", "gcp", "azure", "agile", "scrum", "microservices", "celery",
    "redis", "rabbitmq", "kafka", "linux", "bash",
]
PY_NICE_TO_HAVE = [
    "genai", "generative ai", "llm", "langchain", "openai", "claude",
    "copilot", "ml", "machine learning", "data pipeline", "ocr",
    "computer vision", "confluence", "adb", "android",
]
PY_NEGATIVE = [
    "unpaid", "internship", "intern ", "commission only", "no remote",
    "onsite only", "entry level", "junior", "fresher",
]

# India-eligibility: always-on for BOTH pipelines and the ATS layer.
# A job is dropped unless Indians can plausibly apply to it.

# Reliable positive signal from the *structured location field itself*
# (e.g. "Worldwide", "APAC", "India, APAC, Europe"). Trusted even if the
# description also contains contradictory boilerplate — deliberately set
# location fields are the least ambiguous data we get from any source.
_LOCATION_ALLOW = re.compile(
    r"\b(india|apac|asia|worldwide|anywhere|global|international|"
    r"all countries)\b", re.I)

# Free-text signal is far noisier — companies routinely describe themselves
# as a "global brand" or mention "APAC/EMEA subsidiaries" as boilerplate
# with zero bearing on where THIS role can actually be worked. Bare region
# words are therefore never trusted here (only the structured location
# field is reliable enough for that) — only explicit "you may work from
# anywhere/India" phrasing about the role itself counts.
_DESC_ALLOW = re.compile(
    r"\b("
    r"work(ing)? from anywhere|remote from anywhere|apply from anywhere|"
    r"open to (candidates|applicants)[\w\s,]{0,30}"
    r"(anywhere|worldwide|globally|any country|india)|"
    r"hir(?:e|ing)[\w\s,]{0,20}(anywhere|worldwide|globally|in india|from india)|"
    r"no (?:location|geographic) restrictions?|"
    r"candidates? (?:from|in|based) any(?:where)? (?:country|location)|"
    r"remote[- ]?(?:role|position|job)?[\s,-]*(?:worldwide|anywhere)|"
    r"(?:this role|this position|the role) is (?:fully )?open (?:to|worldwide|globally)"
    r")\b", re.I)

# Phrases that restrict a role to a region/work-authorization that excludes India.
# NOTE: text is run through _normalize_abbrev() before matching, so "u.s."/
# "u.k." have already become "us"/"uk" — no need for period-literal alternatives.
_ELIGIBLE_EXCLUDE = re.compile(
    r"\b("
    r"us only|us-only|us-based|us citizen|us resident|us permanent resident|"
    r"authorized to work in (the )?us\b|authorized to work in the united states|"
    r"must be (located|based|residing) in the (us|united states)|"
    r"must reside in the (us|united states)|"
    r"must live in the (us|united states)|"
    r"work authorization required|unrestricted work authorization|"
    r"no (visa )?sponsorship|sponsorship (is )?not (available|provided)|"
    r"green card holders?( only)?|must have a green card|"
    r"right to work in the (us|uk|united kingdom)|"
    r"eu only|europe only|uk only|uk resident|uk citizen|"
    r"canada only|australia only|new zealand only|"
    r"must be (in|based in) (the )?(uk|europe|canada|australia)|"
    r"(est|pst|cst|mst|cet) time ?zone required|"
    r"must overlap with (us|est|pst|cst|mst) (business )?hours|"
    r"us business hours overlap"
    r")\b", re.I)

# Single-country/state/city location strings that, absent any positive signal
# above, mean the role is not actually open to India.
_NON_INDIA_LOCATIONS = {
    "united states", "usa", "us", "united kingdom", "uk", "canada",
    "australia", "new zealand", "germany", "france", "ireland",
    "netherlands", "spain", "italy", "poland", "singapore",
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming",
    "san francisco", "new york city", "los angeles", "seattle", "boston",
    "chicago", "austin", "denver", "atlanta", "dallas", "houston", "miami",
    "washington dc", "philadelphia", "san diego", "portland", "phoenix",
    "london", "toronto", "vancouver", "berlin", "dublin", "amsterdam",
    "paris", "madrid", "warsaw", "sydney", "melbourne",
}

_LOCATION_FILLER_WORDS = {
    "remote", "fully", "100", "distributed", "anywhere", "home", "work",
    "from", "wfh", "based", "only", "office", "hybrid", "flexible",
    "location", "in", "the", "or", "and", "region",
}
# Unambiguous US state postal codes (excludes ones that collide with common
# English words, e.g. "or", "in", "me", "hi", "ok", "la").
_SAFE_US_STATE_ABBR = {
    "ca", "ny", "tx", "wa", "ma", "co", "ga", "fl", "pa", "oh", "mi",
    "nc", "va", "nj", "az", "mn", "wi", "md", "ct", "nv", "tn", "il",
}


def _normalize_abbrev(text):
    """Collapse dotted abbreviations ("U.S.", "U.S.A.", "U.K.") to plain
    letters so downstream regexes don't need period-literal alternatives —
    a trailing \\b right after a "." never matches (both are non-word chars),
    which silently broke matching on strings like "Remote in U.S." otherwise."""
    return (text.replace("u.s.a.", "usa")
                .replace("u.s.", "us")
                .replace("u.k.", "uk"))


def _clean_location_segment(segment):
    tokens = [t for t in re.sub(r"[^a-z ]", " ", segment).split() if t]
    return tokens


def _location_is_locked_out(location):
    """True if the structured location field names one or more non-India
    places (country/state/major city) — e.g. "Ohio, United States" or
    "San Francisco, CA" — with no sign of remote flexibility. A location
    with any unrecognized segment is left ambiguous (not locked out)."""
    segments = re.split(r"[,/]| - | or ", location)
    matched_any = False
    for seg in segments:
        tokens = _clean_location_segment(seg)
        if not tokens:
            continue
        if any(t in _SAFE_US_STATE_ABBR for t in tokens):
            matched_any = True
            continue
        tokens = [t for t in tokens if t not in _LOCATION_FILLER_WORDS]
        if not tokens:
            continue
        if " ".join(tokens) in _NON_INDIA_LOCATIONS:
            matched_any = True
        else:
            return False
    return matched_any


# Catches restrictions that only show up in the job title, e.g. a Greenhouse
# posting whose structured location is a broad region code ("Remote in AMER")
# but whose title says "(Remote in Louisiana)" / "(Remote in United States)".
_NON_INDIA_PLACE_ALT = "|".join(
    re.escape(p) for p in sorted(_NON_INDIA_LOCATIONS, key=len, reverse=True))
_REMOTE_IN_PLACE = re.compile(
    rf"\bremote (?:in|from|for) (?:the )?({_NON_INDIA_PLACE_ALT})\b", re.I)


def is_eligible_for_india(job):
    """Return True if the job can plausibly be worked from India.
    Always-on for the QA/SDET pipeline, the Python pipeline, and ATS fetches."""
    location = _normalize_abbrev((job.get("location", "") or "").lower())
    title = _normalize_abbrev((job.get("title", "") or "").lower())
    description = _normalize_abbrev((job.get("description", "") or "").lower())
    # Title carries real location info for some ATS boards (e.g. Greenhouse
    # postings whose structured location is just a broad region code).
    description = f"{title} {description}"

    # Structured location field says India/APAC/worldwide/etc. — trust it.
    if _LOCATION_ALLOW.search(location):
        return True

    blob = f"{location} {description}"
    if _ELIGIBLE_EXCLUDE.search(blob) or _REMOTE_IN_PLACE.search(blob):
        return False

    # Explicit "open to anyone/India" phrasing in the free text — but not
    # bare marketing words like "global company" or "worldwide brand".
    if _DESC_ALLOW.search(description):
        return True

    if _location_is_locked_out(location):
        return False
    return True

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
            return dt.datetime.fromtimestamp(int(value), dt.timezone.utc).replace(tzinfo=None)
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


def _fetch_simple_rss(url, source_name, limit=None):
    """Generic RSS-feed fetcher for boards whose feed items are already
    plain "Title @ Company"-style postings (NoDesk, Jobspresso, Pangian,
    Virtual Vocations, ...). Returns [] quietly if feedparser is missing
    or the feed can't be reached, so a dead/renamed feed never kills the run."""
    if feedparser is None:
        return []
    out = []
    try:
        feed = feedparser.parse(url)
        for e in feed.entries:
            out.append({
                "title": e.get("title", ""),
                "company": e.get("author", "") or "",
                "url": e.get("link", ""),
                "tags": [t.get("term", "").lower() for t in e.get("tags", [])] if e.get("tags") else [],
                "location": "Remote",
                "posted": _parse_date(e.get("published")),
                "description": re.sub("<[^>]+>", " ", e.get("summary", "")),
                "source": source_name,
            })
            if limit and len(out) >= limit:
                break
    except Exception as e:
        print(f"[warn] {source_name} RSS failed: {e}")
    return out


def fetch_nodesk(limit=None):
    return _fetch_simple_rss("https://nodesk.co/remote-jobs/index.xml", "NoDesk", limit)


def fetch_jobspresso(limit=None):
    return _fetch_simple_rss("https://jobspresso.co/feed/?post_type=job_listing", "Jobspresso", limit)


def fetch_pangian(limit=None):
    return _fetch_simple_rss("https://pangian.com/feed/?post_type=job_listing", "Pangian", limit)


def fetch_virtual_vocations(limit=None):
    return _fetch_simple_rss("https://www.virtualvocations.com/jobs/rss", "VirtualVocations", limit)


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
                    # Filter to remote-only, India-eligible jobs
                    remote_jobs = [j for j in jobs
                                   if _looks_remote(j) and is_eligible_for_india(j)]
                    # Strip internal helper keys
                    for j in remote_jobs:
                        j.pop("_ashby_remote", None)
                    all_jobs.extend(remote_jobs)
                    if jobs:
                        resolved += 1
                    print(f"[ok]   ats:{new_entry[1]}/{new_entry[2]}: "
                          f"{len(jobs)} total, {len(remote_jobs)} remote+eligible")
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
    fetch_nodesk,
    fetch_jobspresso,
    fetch_pangian,
    fetch_virtual_vocations,
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


def score(job):
    title = job["title"].lower()
    tags = " ".join(job["tags"]).lower()
    desc = job["description"].lower()
    blob = f"{title} {tags} {desc}"

    # Hard gate: must look like a QA/test role somewhere.
    if not any(t in blob for t in ROLE_CORE):
        return None

    # Hard gate: must be India-eligible.
    if not is_eligible_for_india(job):
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
        age = (dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - job["posted"]).days
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
    return (dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - job["posted"]).days <= RECENT_DAYS


def rank(jobs):
    scored = [score(j) for j in jobs]
    scored = [j for j in scored if j and j["score"] > 0 and recent_enough(j)]
    scored.sort(key=lambda j: j["score"], reverse=True)
    return scored


def score_python(job):
    """Score a job against the Python developer profile. Returns None if not a match."""
    title = job["title"].lower()
    tags = " ".join(job["tags"]).lower()
    desc = job["description"].lower()
    blob = f"{title} {tags} {desc}"

    # Hard gate 1: must look like a dev role
    if not any(t in blob for t in PY_ROLE_CORE):
        return None
    # Hard gate 2: must mention Python somewhere
    if "python" not in blob:
        return None
    # Hard gate 3: must be India-eligible
    if not is_eligible_for_india(job):
        return None

    s = 0.0
    s += 10 * _count(PY_ROLE_CORE, title)
    s += 5 * _count(PY_ROLE_CORE, tags)
    s += 2 * min(_count(PY_ROLE_CORE, desc), 2)

    s += 3 * _count(PY_SKILLS_HIGH, blob)
    s += 1.5 * _count(PY_SKILLS_MED, blob)
    s += 1 * _count(PY_NICE_TO_HAVE, blob)
    s -= 8 * _count(PY_NEGATIVE, blob)

    if job["posted"]:
        age = (dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - job["posted"]).days
        if age <= 1:
            s += 4
        elif age <= 3:
            s += 2

    job = dict(job)  # don't mutate the original (shared with QA pipeline)
    job["score"] = round(s, 1)
    job["matched_skills"] = sorted({
        t for t in (PY_SKILLS_HIGH + PY_SKILLS_MED + PY_NICE_TO_HAVE) if t in blob
    })
    return job


def rank_python(jobs):
    scored = [score_python(j) for j in jobs]
    scored = [j for j in scored if j and j["score"] > 0 and recent_enough(j)]
    scored.sort(key=lambda j: j["score"], reverse=True)
    return scored


# --------------------------------------------------------------------------- #
# SEEN-STATE (avoid emailing the same job twice)
# --------------------------------------------------------------------------- #

def _load_seen_file(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_seen_file(seen, path):
    cutoff = (dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - dt.timedelta(days=SEEN_TTL_DAYS)).isoformat()
    seen = {u: ts for u, ts in seen.items() if ts >= cutoff}
    with open(path, "w") as f:
        json.dump(seen, f, indent=0)


def _filter_unseen_file(jobs, seen):
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).isoformat()
    fresh = [j for j in jobs if j["url"] and j["url"] not in seen]
    for j in fresh:
        seen[j["url"]] = now
    return fresh


# Thin wrappers kept for the QA pipeline (backward-compatible names)
def load_seen():
    return _load_seen_file(SEEN_FILE)


def save_seen(seen):
    _save_seen_file(seen, SEEN_FILE)


def filter_unseen(jobs, seen):
    return _filter_unseen_file(jobs, seen)


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


def gemini_annotate_python(jobs):
    """Gemini re-rank for the Python developer pipeline (same API key, different prompt)."""
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
        "You are screening remote jobs for a Senior Python Developer with 9 years of "
        "experience in Python, OOP, REST API development (FastAPI/Django/Flask), SQL, "
        "CI/CD (Jenkins), modular framework design, GenAI tooling (Claude, Copilot), "
        "and system/log data processing. Candidate is based in India.\n"
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
        jobs.sort(key=lambda j: (j.get("fit") is not None, j.get("fit", 0)), reverse=True)
        print(f"[ok]   gemini-python: annotated {len(annotations)} jobs")
    except Exception as e:
        print(f"[warn] gemini-python skipped: {e}")
    return jobs


def _print_digest(jobs, errors, label="QA/SDET"):
    """Print a plain-text digest to stdout for --dry-run mode."""
    print(f"\n{'='*60}")
    print(f"  Remote {label} Digest — {dt.date.today():%d %b %Y}")
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


# --------------------------------------------------------------------------- #
# EMAIL + HTML
# --------------------------------------------------------------------------- #

def build_html(jobs, errors, heading="Remote QA / SDET jobs"):
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
      <h2 style="margin-bottom:0">{html.escape(heading)} — {today}</h2>
      <p style="color:#666;margin-top:4px">{len(jobs)} new matches for your profile.</p>
      <table style="width:100%;border-collapse:collapse">{''.join(rows)}</table>
      {err}
      <p style="color:#999;font-size:11px;margin-top:18px">
        Auto-generated digest. Edit ROLE_CORE / SKILLS_* in job_hunter.py to retune.</p>
    </div>"""


def print_digest(jobs, errors):
    _print_digest(jobs, errors, label="QA/SDET")


def send_email(html_body, job_count, to=None, subject=None):
    user = os.environ.get("GMAIL_USER", "")
    pw = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not user or not pw:
        raise RuntimeError(
            "GMAIL_USER or GMAIL_APP_PASSWORD secret is not set in GitHub Actions. "
            "Go to Repo → Settings → Secrets and variables → Actions and add them."
        )
    to = to or os.environ.get("MAIL_TO", user)
    subject = subject or f"[Jobs] {job_count} remote QA/SDET roles — {dt.date.today():%d %b}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to
    msg.attach(MIMEText("Open in an HTML-capable client.", "plain"))
    msg.attach(MIMEText(html_body, "html"))

    print(f"[info] connecting to smtp.gmail.com:465 as {user} → sending to {to}")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(user, pw)
        server.sendmail(user, [a.strip() for a in to.split(",")], msg.as_string())
    print(f"[ok]   emailed {job_count} jobs to {to}")


def run_python_pipeline(raw, errors, dry_run=False):
    """Score raw jobs against the Python developer profile and email/print results."""
    print("\n[info] === Python Developer Pipeline ===")
    py_jobs = rank_python(dedupe(raw))
    print(f"[info] {len(py_jobs)} jobs passed Python developer filter (India-eligible, remote)")

    py_seen_data = _load_seen_file(PY_SEEN_FILE)
    py_jobs = _filter_unseen_file(py_jobs, py_seen_data)[:PY_TOP_N]
    print(f"[info] {len(py_jobs)} new Python jobs after dedupe-vs-history")

    py_jobs = gemini_annotate_python(py_jobs)

    html_body = build_html(
        py_jobs, errors,
        heading="Remote Python Developer jobs (India-eligible)"
    ) if py_jobs else (
        "<p>No new remote Python Developer roles matched today. Pipeline ran fine.</p>"
    )

    if dry_run:
        _print_digest(py_jobs, errors, label="Python Developer")
        with open("digest_python.html", "w", encoding="utf-8") as f:
            f.write(html_body)
        print("[dry-run] digest_python.html written. No email sent.")
        return

    subject = f"[Python Jobs] {len(py_jobs)} remote roles — {dt.date.today():%d %b}"
    try:
        send_email(html_body, len(py_jobs), to=PYTHON_MAIL_TO, subject=subject)
    except Exception as e:
        print(f"[fail] Python email: {e}")
    finally:
        _save_seen_file(py_seen_data, PY_SEEN_FILE)
        with open("digest_python.html", "w", encoding="utf-8") as f:
            f.write(html_body)


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

    # Fetch once — both pipelines share the same raw job pool
    raw, errors = gather_jobs(limit=args.limit)
    print(f"[info] gathered {len(raw)} raw jobs")

    # ── Pipeline 1: QA / SDET ───────────────────────────────────────────────
    print("\n[info] === QA/SDET Pipeline ===")
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
            print("[info] No new QA/SDET jobs matched today.")
        with open("digest.html", "w", encoding="utf-8") as f:
            f.write(html_body)
        print("[dry-run] digest.html written. No email sent.")
    else:
        try:
            send_email(html_body, len(jobs))
        except Exception as e:
            print(f"[fail] QA email: {e}")
        finally:
            # Always persist seen-state so re-runs don't resend the same jobs
            save_seen(seen)
            with open("digest.html", "w", encoding="utf-8") as f:
                f.write(html_body)

    # ── Pipeline 2: Python Developer (India-eligible) ────────────────────────
    run_python_pipeline(raw, errors, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
