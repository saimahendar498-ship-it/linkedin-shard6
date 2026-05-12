import requests
from bs4 import BeautifulSoup
import re
import time
import os
import json
import html
import sys
import argparse
import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
import concurrent.futures
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime, timezone

# --- CONFIGURATION ---
SPREADSHEET_ID       = "1muSKyg-YgV8DOiKGkvNkvtacMEPItDliy5PRBNRsUhk"
# Folder inside shared drive where raw HTML files are uploaded
DRIVE_HTML_FOLDER_ID = os.environ.get("DRIVE_HTML_FOLDER_ID", "10yuSAddLnuFNvEiASOewKCZgqRq77kyS")
JSONL_OUTPUT         = os.environ.get("JSONL_OUTPUT", "linkedin_output.jsonl")
# OAuth user credentials (for normal folder uploads)
OAUTH_REFRESH_TOKEN = os.environ.get("OAUTH_REFRESH_TOKEN", "")
OAUTH_CLIENT_ID     = os.environ.get("OAUTH_CLIENT_ID", "")
OAUTH_CLIENT_SECRET = os.environ.get("OAUTH_CLIENT_SECRET", "")

SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE",
    os.path.join(os.path.dirname(__file__), "omega-signifier-424714-g0-e400c4ed9411.json")
)

def _load_service_account_json() -> Optional[str]:
    for key in ("GOOGLE_SERVICE_ACCOUNT_JSON", "SERVICE_ACCOUNT_JSON",
                "GOOGLE_CREDENTIALS", "GCP_SERVICE_ACCOUNT"):
        v = os.environ.get(key)
        if v and v.strip():
            print(f"[env] using {key} (len={len(v)})", flush=True)
            return v
    gkeys = [k for k in os.environ if 'GOOGLE' in k or 'SERVICE' in k or 'GCP' in k]
    print(f"[env] no service account JSON env var found. Env keys: {gkeys}", flush=True)
    return None

SERVICE_ACCOUNT_JSON = _load_service_account_json()
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "Shard5")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}
MAX_RETRIES        = 2
SLEEP_TIME         = 6
MAX_WORKERS        = 6
BATCH_UPDATE_SIZE  = 40
CHUNK_SIZE         = 400
POLL_INTERVAL      = 60

# Column layout
# Column layout matching social-media-internal schema (A=1 ... AI=35)
# A=domain  B=linkedInUrl  C=ticket  D=extractionStatus
COL_COMPANY_NAME        = 5   # E  companyName
COL_COMPANY_SIZE        = 6   # F  companySize
COL_LIDB_REF_ID         = 7   # G  lidbReferenceId
COL_TAGLINE             = 8   # H  tagline
COL_FOLLOWERS           = 9   # I  followers
COL_EMPLOYEES           = 10  # J  employeesOnLinkedIn
COL_INDUSTRIES          = 11  # K  industries
COL_HEADQUARTERS        = 12  # L  headquarters
COL_COUNTRY_CODE        = 13  # M  countryCode
COL_FOUNDED_YEAR        = 14  # N  foundedYear
COL_WEBSITE             = 15  # O  website
COL_ABOUT               = 16  # P  about
COL_ORG_TYPE            = 17  # Q  organizationType
COL_TYPE                = 18  # R  type
COL_SPECIALTIES         = 19  # S  specialties
COL_LOGO                = 20  # T  logo
COL_LOCATIONS           = 21  # U  locations (JSON)
COL_AFFILIATED_PAGES    = 22  # V  affiliatedPages (JSON)
COL_JOB_POSTS           = 23  # W  jobPosts (JSON)
COL_CRUNCHBASE_URL      = 24  # X  fundingSourceCrunchbaseUrl
COL_FUND_INVESTORS      = 25  # Y  fundingSourceInvestors
COL_FUND_ROUNDS_COUNT   = 26  # Z  fundingRoundsCount
COL_LAST_ROUND_TYPE     = 27  # AA lastRoundType
COL_LAST_ROUND_DATE     = 28  # AB lastRoundDate
COL_LAST_ROUND_AMOUNT   = 29  # AC lastRoundAmount
COL_FUND_ROUNDS         = 30  # AD fundingRounds (JSON)
COL_SIMILAR_COMPANIES   = 31  # AE similarCompanies (JSON)
COL_EMPLOYEES_LIST      = 32  # AF employees (JSON)
COL_RAW_HTML            = 33  # AG rawHtml (Drive URL)
COL_FETCH_DATE          = 34  # AH fetchDate
COL_NOTE                = 35  # AI note (pipeline)

# ---------- Credentials ----------

_creds: Optional[Credentials] = None


# def get_creds() -> Credentials:
#     global _creds
#     if _creds is None:
#         scopes = [
#             'https://www.googleapis.com/auth/spreadsheets',
#             'https://www.googleapis.com/auth/drive',
#         ]
#         if SERVICE_ACCOUNT_JSON:
#             try:
#                 info = json.loads(SERVICE_ACCOUNT_JSON)
#             except json.JSONDecodeError as e:
#                 raise RuntimeError(f"Service account env var is invalid JSON: {e}")
#             _creds = Credentials.from_service_account_info(info, scopes=scopes)
#         elif os.path.exists(SERVICE_ACCOUNT_FILE):
#             _creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
#         else:
#             raise RuntimeError(
#                 "No credentials available. Set GOOGLE_SERVICE_ACCOUNT_JSON env var."
#             )
#     if not _creds.valid:
#         _creds.refresh(Request())
#     return _creds

def get_creds() -> Credentials:
    global _creds
    if _creds and _creds.valid:
        return _creds

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]

    # Use OAuth user token if provided (needed for normal folder uploads)
    if OAUTH_REFRESH_TOKEN and OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET:
        from google.oauth2.credentials import Credentials as OAuthCredentials
        _creds = OAuthCredentials(
            token=None,
            refresh_token=OAUTH_REFRESH_TOKEN,
            client_id=OAUTH_CLIENT_ID,
            client_secret=OAUTH_CLIENT_SECRET,
            token_uri='https://oauth2.googleapis.com/token',
        )
        _creds.refresh(Request())
        return _creds

    # Fallback: service account (for shared drives)
    if SERVICE_ACCOUNT_JSON:
        try:
            info = json.loads(SERVICE_ACCOUNT_JSON)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Service account env var is invalid JSON: {e}")
        _creds = Credentials.from_service_account_info(info, scopes=scopes)
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        _creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    else:
        raise RuntimeError("No credentials available.")

    if not _creds.valid:
        _creds.refresh(Request())
    return _creds


def get_gspread_client():
    return gspread.authorize(get_creds())


def open_sheet():
    gc = get_gspread_client()
    return gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)


# ---------- Google Drive HTML upload ----------

def upload_html_to_drive(filename: str, html_bytes: bytes) -> str:
    """Upload raw HTML to the shared Drive folder, return web view URL.
    Uses supportsAllDrives=true for shared drive compatibility.
    Returns empty string on failure.
    """
    if not DRIVE_HTML_FOLDER_ID:
        return ''
    try:
        creds = get_creds()
        boundary = 'LI_HTML_boundary_xk9'
        meta = json.dumps({
            'name': filename,
            'mimeType': 'text/html',
            'parents': [DRIVE_HTML_FOLDER_ID],
        })
        body = (
            f'--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n'
            + meta
            + f'\r\n--{boundary}\r\nContent-Type: text/html; charset=utf-8\r\n\r\n'
        ).encode('utf-8') + html_bytes + f'\r\n--{boundary}--\r\n'.encode('utf-8')

        resp = requests.post(
            'https://www.googleapis.com/upload/drive/v3/files'
            '?uploadType=multipart&fields=id&supportsAllDrives=true',
            headers={
                'Authorization': f'Bearer {creds.token}',
                'Content-Type': f'multipart/related; boundary={boundary}',
            },
            data=body,
            timeout=60,
        )
        if resp.status_code in (200, 201):
            file_id = resp.json().get('id', '')
            if file_id:
                return f'https://drive.google.com/file/d/{file_id}/view'
        print(f"Drive upload failed: {resp.status_code} {resp.text[:300]}", flush=True)
    except Exception as e:
        print(f"Drive upload error: {e}", flush=True)
    return ''


# ---------- Field extraction ----------

_LABEL_KEYS = {
    'Industry':     'industry',
    'Company size': 'company_size',
    'Headquarters': 'headquarters',
    'Type':         'type',
    'Founded':      'founded',
    'Specialties':  'specialties',
    'Website':      'website',
}

# LinkedIn UI / navigation text that leaks into parsed sections
_JUNK_LINE = re.compile(
    r'(^see (more|all)'
    r'|^more searches'
    r'|\bjobs$'                    # "PepsiCo jobs", "Analyst jobs", etc.
    r'|^crunchbase$'
    r'|^learn more'
    r'|^sign in'
    r'|^follow$'
    r'|^connect$'
    r'|^message$'
    r'|^report$'
    r'|^share$'
    r'|^save$'
    r'|^overview$'
    r'|^about us$'
    r'|^similar pages'
    r'|^people also'
    r'|^\d+\s+(connections|followers)'
    r'|^last round'
    r'|^total rounds'
    r'|^show \d+'
    r'|^view all'
    r'|^load more)',
    re.I
)


def _is_junk(line: str) -> bool:
    return bool(_JUNK_LINE.search(line.strip()))


_ROUND_PAT = (
    r'Pre-Seed|Seed|Angel|Series [A-Z]\d*|Bridge|Convertible Note'
    r'|Debt(?: Financing)?|Grant|ICO|IPO|M&A|Private Equity'
    r'|Post-IPO(?: Debt| Equity| Secondary)?|Secondary Market|Venture'
)


def _unescape(s: str) -> str:
    if not s:
        return ''
    return html.unescape(s).replace('\xa0', ' ').strip()


def extract_fields(raw_html: str, linkedin_url: str = '') -> Dict[str, Any]:
    """Extract all company fields per the social-media-internal schema.
    Returns a dict with all extractable fields; missing ones are omitted.
    """
    out: Dict[str, Any] = {}

    if linkedin_url:
        out['linkedin_url'] = linkedin_url

    # lidb_reference_id from entityUrn in page source
    m = re.search(r'"entityUrn"\s*:\s*"urn:li:(?:fs_normalized_)?company:(\d+)"', raw_html)
    if not m:
        m = re.search(r'"entityUrn"\s*:\s*"urn:li:organization:(\d+)"', raw_html)
    if not m:
        m = re.search(r'urn:li:organization:(\d+)', raw_html)
    if m:
        out['lidb_reference_id'] = m.group(1)

    # organizationType
    m = re.search(r'"organizationType"\s*:\s*"([^"]+)"', raw_html)
    if m:
        out['organization_type'] = m.group(1).lower()

    # JSON-LD Organization block
    for sm in re.finditer(r'<script type="application/ld\+json">(.*?)</script>', raw_html, re.S):
        try:
            j = json.loads(sm.group(1))
        except Exception:
            continue
        items = j.get('@graph', [j]) if isinstance(j, dict) else (j if isinstance(j, list) else [])
        for it in items:
            if not isinstance(it, dict) or it.get('@type') not in ('Organization', 'Corporation'):
                continue
            out.setdefault('name', _unescape(it.get('name', '') or ''))
            out.setdefault('about', _unescape(it.get('description', '') or ''))
            logo = it.get('logo')
            if isinstance(logo, dict):
                out.setdefault('logo', logo.get('contentUrl', '') or '')
            elif isinstance(logo, str):
                out.setdefault('logo', logo)
            noe = it.get('numberOfEmployees')
            if isinstance(noe, dict):
                val = noe.get('value')
                if val:
                    try:
                        out.setdefault('employees_on_linkedin', int(val))
                    except (ValueError, TypeError):
                        pass
            out.setdefault('website', it.get('sameAs', '') or '')
            addr = it.get('address')
            if isinstance(addr, dict):
                locality = addr.get('addressLocality', '')
                region = addr.get('addressRegion', '')
                country = addr.get('addressCountry', '')
                parts = [p for p in [locality, region] if p]
                out.setdefault('headquarters', ', '.join(parts) or country)
                if country:
                    out.setdefault('country_code', country[:2].upper())
            founded = it.get('foundingDate', '')
            if founded:
                try:
                    out.setdefault('founded_year', int(str(founded)[:4]))
                except (ValueError, TypeError):
                    pass

    # og:meta — followers, tagline, logo fallback
    soup = BeautifulSoup(raw_html, 'html.parser')
    ogi = soup.find('meta', property='og:image')
    if ogi and not out.get('logo'):
        out['logo'] = ogi.get('content', '') or ''
    ogd = soup.find('meta', property='og:description')
    if ogd:
        c = ogd.get('content', '') or ''
        m = re.search(r'([\d,]+)\s+followers on LinkedIn', c, re.I)
        if m:
            try:
                out['followers'] = int(m.group(1).replace(',', ''))
            except ValueError:
                pass
        m = re.search(r'followers on LinkedIn\.\s*(.+?)(?:\s*\||$)', c, re.S)
        if m:
            tl = _unescape(m.group(1))
            if tl and tl.lower() != 'about us':
                out.setdefault('tagline', tl[:300])

    # Affiliated pages (links with trk=affiliated-pages)
    affiliated: List[Dict] = []
    for a in soup.find_all('a', href=re.compile(r'trk=affiliated-pages', re.I)):
        link = a.get('href', '')
        h3 = a.find('h3', class_='base-aside-card__title')
        sub = a.find('p', class_='base-aside-card__subtitle')
        loc = a.find('p', class_='base-aside-card__second-subtitle')
        title = _unescape(h3.get_text(strip=True)) if h3 else _unescape(a.get_text(' ', strip=True))
        subtitle = _unescape(sub.get_text(strip=True)) if sub else ''
        location = _unescape(loc.get_text(strip=True)) if loc else ''
        if link and title:
            affiliated.append({'title': title, 'subtitle': subtitle, 'link': link, 'location': location})
    if affiliated:
        out['affiliated_pages'] = affiliated

    # Job posts (links with /jobs/view/)
    job_posts: List[Dict] = []
    seen_jobs: set = set()
    for a in soup.find_all('a', href=re.compile(r'/jobs/view/', re.I)):
        link = a.get('href', '').split('?')[0]
        if link in seen_jobs:
            continue
        seen_jobs.add(link)
        title = _unescape(a.get_text(' ', strip=True))
        if title and link:
            job_posts.append({'title': title, 'link': link, 'count': 0})
        if len(job_posts) >= 10:
            break
    if job_posts:
        out['job_posts'] = job_posts

    # Similar companies (links with trk=similar-pages)
    similar: List[Dict] = []
    seen_sim: set = set()
    for a in soup.find_all('a', href=re.compile(r'trk=similar-pages', re.I)):
        link = a.get('href', '')
        if link in seen_sim:
            continue
        seen_sim.add(link)
        h3 = a.find('h3', class_='base-aside-card__title')
        sub = a.find('p', class_='base-aside-card__subtitle')
        loc = a.find('p', class_='base-aside-card__second-subtitle')
        title = _unescape(h3.get_text(strip=True)) if h3 else _unescape(a.get_text(' ', strip=True))
        sub_title = _unescape(sub.get_text(strip=True)) if sub else ''
        location = _unescape(loc.get_text(strip=True)) if loc else ''
        if title and link:
            similar.append({'title': title, 'sub_title': sub_title, 'link': link, 'location': location})
    if similar:
        out['similar_companies'] = similar

    # Visible text: label:value pairs + structured sections
    for t in soup(['script', 'style', 'noscript', 'link']):
        t.decompose()
    text = soup.get_text(separator='\n', strip=True)
    lines = [l.strip() for l in text.split('\n') if l.strip()]

    for i, line in enumerate(lines):
        if line in _LABEL_KEYS and i + 1 < len(lines):
            key = _LABEL_KEYS[line]
            val = _unescape(lines[i + 1])
            # headquarters: prefer DOM (city-level) over JSON-LD (street address)
            if key == 'headquarters' or not out.get(key):
                out[key] = val

    # founded string -> founded_year int
    if 'founded' in out and not out.get('founded_year'):
        try:
            out['founded_year'] = int(out.pop('founded'))
        except (ValueError, TypeError):
            pass

    _extract_locations_text(lines, out, soup)
    _extract_funding_structured(lines, out)
    _extract_employees_text(lines, out)

    # Crunchbase URL — org slug is in raw HTML even without JS rendering
    if not out.get('funding_source_crunchbase_url'):
        cb = re.search(r'crunchbase\.com/organization/([a-z0-9_-]+)', raw_html, re.I)
        if cb:
            out['funding_source_crunchbase_url'] = (
                f"https://www.crunchbase.com/organization/{cb.group(1)}"
            )

    # fetchDate
    now = datetime.now(timezone.utc)
    out['fetch_date'] = {
        'day': now.day, 'month': now.month, 'year': now.year,
        'hours': now.hour, 'minutes': now.minute, 'seconds': now.second,
    }

    return out


def _extract_locations_text(lines: List[str], out: Dict[str, Any], soup=None) -> None:
    try:
        li = next(i for i, l in enumerate(lines) if l.strip().lower() == 'locations')
    except StopIteration:
        return
    block = lines[li + 1: li + 40]
    locs: List[Dict] = []
    current: Dict = {}
    for line in block:
        low = line.lower()
        if low in ('primary', 'secondary', 'hq'):
            if current.get('details'):
                locs.append(current)
            current = {'tag': line.title(), 'details': '', 'map_url': ''}
        elif 'bing.com/maps' in line or 'google.com/maps' in line:
            current['map_url'] = line
        elif current is not None and line and len(line) > 3 and not current.get('details') and low not in ('get directions', 'directions'):
            current['details'] = line
    if current.get('details'):
        locs.append(current)

    # Map URLs are in href attributes (not visible text), so extract from soup
    if soup and locs:
        map_hrefs = [
            a.get('href', '')
            for a in soup.find_all('a', href=re.compile(r'(bing\.com/maps|google\.com/maps|maps\.google)', re.I))
        ]
        url_iter = iter(map_hrefs)
        for loc in locs:
            if not loc.get('map_url'):
                loc['map_url'] = next(url_iter, '')

    if locs:
        out['locations'] = locs


_NAME_PAT = re.compile(r'^[A-Z][a-z\-\'\.]{1,}(\s+[A-Z][a-zA-Z\-\'\.]{1,}){1,4}$')


def _extract_employees_text(lines: List[str], out: Dict[str, Any]) -> None:
    # Static HTML: "Employees at {Company}" → names → "See all employees"
    ei = None
    for i, l in enumerate(lines):
        low = l.strip().lower()
        if low.startswith('employees at ') or low == 'employees':
            ei = i
            break
    if ei is None:
        return

    company_name = out.get('name', '').lower()
    employees: List[Dict] = []
    for line in lines[ei + 1: ei + 30]:
        s = line.strip()
        if not s or s.lower().startswith('see all'):
            break                                       # hard stop
        if ('…' in s or '...' in s                     # post excerpt
                or s.lower() == company_name           # company name repeated
                or _is_junk(s)
                or re.search(r'\d+[KM]?\s+followers', s, re.I)
                or not _NAME_PAT.match(s)):             # not a proper name pattern
            continue
        employees.append({'title': s, 'sub_title': ''})

    if employees:
        out['employees'] = employees


def _extract_funding_structured(lines: List[str], out: Dict[str, Any]) -> None:
    try:
        fi = next(i for i, l in enumerate(lines) if l.strip().lower() == 'funding')
    except StopIteration:
        return

    block = lines[fi: fi + 60]

    # Total rounds count
    m = re.search(r'(\d+)\s+total\s+rounds?', ' '.join(block), re.I)
    if m:
        out['funding_rounds_count'] = int(m.group(1))

    # Parse individual rounds (LinkedIn shows newest first)
    funding_rounds: List[Dict] = []
    for idx, line in enumerate(block):
        rm = re.search(_ROUND_PAT, line, re.I)
        if not rm:
            continue
        round_type = rm.group(0)
        round_date: Dict = {}
        amount = 0
        currency = 'USD'

        for j in range(idx + 1, min(idx + 8, len(block))):
            if not round_date:
                dm = re.search(r'([A-Z][a-z]{2,}\.?\s+\d{1,2},?\s+\d{4})', block[j])
                if dm:
                    try:
                        d = datetime.strptime(
                            dm.group(1).replace(',', '').replace('.', ''), '%b %d %Y'
                        )
                        round_date = {'day': d.day, 'month': d.month, 'year': d.year}
                    except ValueError:
                        pass
            if not amount:
                am = re.search(
                    r'((?:US\s*\$|USD|EUR|€|\$|£)\s*)([\d,.]+)\s*([MBKmb]?)', block[j]
                )
                if am:
                    sym = am.group(1).strip()
                    num_s = am.group(2).replace(',', '')
                    mult = am.group(3).upper()
                    currency = ('EUR' if ('€' in sym or 'EUR' in sym)
                                else 'GBP' if '£' in sym else 'USD')
                    try:
                        num = float(num_s)
                        if mult == 'M':
                            num *= 1_000_000
                        elif mult == 'B':
                            num *= 1_000_000_000
                        elif mult == 'K':
                            num *= 1_000
                        amount = int(num)
                    except ValueError:
                        pass

        funding_rounds.append({
            'type': round_type,
            'date': round_date,
            'amount': amount,
            'currency': currency,
        })

        # First match = most recent round
        if len(funding_rounds) == 1:
            out['funding_last_round_type'] = round_type
            if round_date:
                out['funding_last_round_date'] = (
                    f"{round_date['month']:02d}/{round_date['year']}"
                )
            if amount:
                out['funding_last_round_amount'] = f"{currency} {amount:,}"

    if funding_rounds:
        out['funding_rounds'] = funding_rounds

    # Investors list
    try:
        ii = next(j for j, l in enumerate(block) if l.strip().lower() == 'investors')
        inv_lines: List[str] = []
        for l in block[ii + 1:]:
            s = l.strip()
            if re.match(r'\+\s*\d+', s):
                out['funding_investors_more'] = s
                break
            if s and not _is_junk(s):
                inv_lines.append(_unescape(s))
            if len(inv_lines) >= 10:
                break
        if inv_lines:
            out['funding_source_investors'] = inv_lines
            out['funding_investors'] = '; '.join(inv_lines)
    except StopIteration:
        pass



# ---------- Fetch ----------

def fetch_linkedin(url: str) -> Tuple[Optional[Dict[str, Any]], str, str]:
    """Return (fields_dict or None, raw_html, note)."""
    clean_url = url.rstrip('/')
    for suf in ['/about', '/people', '/jobs', '/posts', '/insights']:
        if clean_url.endswith(suf):
            clean_url = clean_url[:-len(suf)]
    try:
        r = requests.get(clean_url, headers=HEADERS, timeout=20, allow_redirects=True)
    except Exception as e:
        return None, '', f"err_{type(e).__name__}"

    final = r.url.lower()
    if '/uas/login' in final or 'challenge.linkedin.com' in final or '/login?' in final:
        return None, '', "blocked_login"
    if r.status_code == 404:
        return None, '', "not_found"
    if r.status_code != 200:
        return None, '', f"http_{r.status_code}"

    og_title_m = re.search(r'<meta\s+property="og:title"\s+content="([^"]*)"', r.text)
    if og_title_m and og_title_m.group(1).startswith('LinkedIn Login'):
        return None, '', "blocked_login"

    fields = extract_fields(r.text, clean_url)
    return fields, r.text, ''


# ---------- JSONL output ----------

def save_to_jsonl(record: Dict[str, Any]) -> None:
    """Append one result as a JSON line to JSONL_OUTPUT file."""
    try:
        with open(JSONL_OUTPUT, 'a', encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    except Exception as e:
        print(f"JSONL write error: {e}", flush=True)


# ---------- Row worker ----------

def scrape_worker(row_index: int, domain: str, url: str) -> Dict[str, Any]:
    fields, raw_html, note = None, '', ''
    for attempt in range(MAX_RETRIES):
        fields, raw_html, note = fetch_linkedin(url)
        if fields or note in ("not_found", "blocked_login"):
            break
        time.sleep(SLEEP_TIME)

    result: Dict[str, Any] = {
        'row_index': row_index, 'domain': domain,
        'status': 'error', 'fields': {}, 'note': note,
        'raw_html_url': '',
    }

    if not fields:
        save_to_jsonl(result)
        return result

    if fields.get('employees_on_linkedin') or fields.get('name'):
        result['status'] = 'done'
    else:
        result['note'] = note or 'no_core_fields'
    result['fields'] = fields

    # Upload HTML to shared Google Drive folder
    if raw_html:
        safe_domain = re.sub(r'[^a-zA-Z0-9_-]', '_', domain)
        lidb_id = fields.get('lidb_reference_id', '')
        filename = f"{lidb_id}_{safe_domain}.html" if lidb_id else f"{safe_domain}.html"
        drive_url = upload_html_to_drive(filename, raw_html.encode('utf-8', errors='replace'))
        result['raw_html_url'] = drive_url
        fields['raw_html_url'] = drive_url

    save_to_jsonl(result)
    return result


def fetch_open_chunk(ws, limit: int) -> List[Tuple[int, str, str]]:
    rows = ws.get('A2:C')
    open_rows = []
    for i, r in enumerate(rows, start=2):
        if len(open_rows) >= limit:
            break
        pad = r + [''] * (3 - len(r))
        domain, url, ticket = pad[0].strip(), pad[1].strip(), pad[2].strip().lower()
        if ticket == 'open' and url and domain:
            open_rows.append((i, domain, url))
    return open_rows


def run_pipeline():
    print(f"Connecting to worksheet '{WORKSHEET_NAME}'...", flush=True)
    ws = open_sheet()
    open_rows = fetch_open_chunk(ws, CHUNK_SIZE)
    print(f"Open tickets this cycle: {len(open_rows)}", flush=True)
    if not open_rows:
        return

    def flush(batch):
        cells = []
        for r in batch:
            idx = r['row_index']
            f = r['fields'] or {}

            def j(v):
                return json.dumps(v, ensure_ascii=False) if v else ''

            fd = f.get('fetch_date') or {}
            fetch_str = (f"{fd.get('day','')}/{fd.get('month','')}/{fd.get('year','')} "
                         f"{fd.get('hours',''):02}:{fd.get('minutes',''):02}" if fd else '')

            cells.append(gspread.Cell(idx, 3,  'closed'))
            cells.append(gspread.Cell(idx, 4,  r['status']))
            cells.append(gspread.Cell(idx, COL_COMPANY_NAME,      f.get('name', '')))
            cells.append(gspread.Cell(idx, COL_COMPANY_SIZE,      f.get('company_size', '')))
            cells.append(gspread.Cell(idx, COL_LIDB_REF_ID,       f.get('lidb_reference_id', '')))
            cells.append(gspread.Cell(idx, COL_TAGLINE,           f.get('tagline', '')))
            cells.append(gspread.Cell(idx, COL_FOLLOWERS,         f.get('followers', '')))
            cells.append(gspread.Cell(idx, COL_EMPLOYEES,         f.get('employees_on_linkedin', '')))
            cells.append(gspread.Cell(idx, COL_INDUSTRIES,        f.get('industry', '')))
            cells.append(gspread.Cell(idx, COL_HEADQUARTERS,      f.get('headquarters', '')))
            cells.append(gspread.Cell(idx, COL_COUNTRY_CODE,      f.get('country_code', '')))
            cells.append(gspread.Cell(idx, COL_FOUNDED_YEAR,      f.get('founded_year', '')))
            cells.append(gspread.Cell(idx, COL_WEBSITE,           f.get('website', '')))
            cells.append(gspread.Cell(idx, COL_ABOUT,             f.get('about', '')[:49000]))
            cells.append(gspread.Cell(idx, COL_ORG_TYPE,          f.get('organization_type', '')))
            cells.append(gspread.Cell(idx, COL_TYPE,              f.get('type', '')))
            cells.append(gspread.Cell(idx, COL_SPECIALTIES,       f.get('specialties', '')))
            cells.append(gspread.Cell(idx, COL_LOGO,              f.get('logo', '')))
            cells.append(gspread.Cell(idx, COL_LOCATIONS,         j(f.get('locations'))))
            cells.append(gspread.Cell(idx, COL_AFFILIATED_PAGES,  j(f.get('affiliated_pages'))))
            cells.append(gspread.Cell(idx, COL_JOB_POSTS,         j(f.get('job_posts'))))
            cells.append(gspread.Cell(idx, COL_CRUNCHBASE_URL,    f.get('funding_source_crunchbase_url', '')))
            cells.append(gspread.Cell(idx, COL_FUND_INVESTORS,    f.get('funding_investors', '')))
            cells.append(gspread.Cell(idx, COL_FUND_ROUNDS_COUNT, f.get('funding_rounds_count', '')))
            cells.append(gspread.Cell(idx, COL_LAST_ROUND_TYPE,   f.get('funding_last_round_type', '')))
            cells.append(gspread.Cell(idx, COL_LAST_ROUND_DATE,   f.get('funding_last_round_date', '')))
            cells.append(gspread.Cell(idx, COL_LAST_ROUND_AMOUNT, f.get('funding_last_round_amount', '')))
            cells.append(gspread.Cell(idx, COL_FUND_ROUNDS,       j(f.get('funding_rounds'))))
            cells.append(gspread.Cell(idx, COL_SIMILAR_COMPANIES, j(f.get('similar_companies'))))
            cells.append(gspread.Cell(idx, COL_EMPLOYEES_LIST,    j(f.get('employees'))))
            cells.append(gspread.Cell(idx, COL_RAW_HTML,          r.get('raw_html_url', '')))
            cells.append(gspread.Cell(idx, COL_FETCH_DATE,        fetch_str))
            cells.append(gspread.Cell(idx, COL_NOTE,              r['note'][:200]))
        if not cells:
            return
        for attempt in range(5):
            try:
                ws.update_cells(cells)
                return
            except gspread.exceptions.APIError as e:
                if '429' in str(e) or 'Quota' in str(e):
                    time.sleep(30 * (attempt + 1))
                else:
                    raise

    completed = 0
    pending = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(scrape_worker, i, d, u) for (i, d, u) in open_rows]
        for fut in concurrent.futures.as_completed(futures):
            try:
                r = fut.result()
                pending.append(r)
                completed += 1
                if completed % 10 == 0:
                    f = r.get('fields') or {}
                    print(f"  [{completed}/{len(open_rows)}] {r['domain']} -> {r['status']}"
                          f" (emp={f.get('employees_on_linkedin','')})", flush=True)
                if len(pending) >= BATCH_UPDATE_SIZE:
                    flush(pending)
                    pending.clear()
            except Exception as e:
                print(f"Thread error: {e}", flush=True)
    if pending:
        flush(pending)
    print(f"Cycle complete. Processed {completed} rows.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', help='Test a single LinkedIn URL and print extracted fields')
    args = parser.parse_args()

    if args.url:
        print(f"Fetching: {args.url}", flush=True)
        fields, raw_html, note = fetch_linkedin(args.url)
        if note:
            print(f"Note: {note}")
        if fields:
            for k, v in fields.items():
                if k != 'fetch_date':
                    print(f"  {k}: {v}")
        else:
            print("No fields extracted.")
        sys.exit(0)

    print("LinkedIn Automation started.", flush=True)
    while True:
        try:
            run_pipeline()
        except Exception as e:
            print(f"Pipeline error: {e}", flush=True)
        time.sleep(POLL_INTERVAL)
