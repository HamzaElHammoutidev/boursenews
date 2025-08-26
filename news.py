# =========================
# Boursenews "Marchés" – FULL CONTENT scraper (JSON-LD aware)
# =========================

!pip -q install httpx[http2] selectolax pandas python-dateutil orjson tqdm

import asyncio, time, re, os, csv, gzip, orjson, json, random, html
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import Optional, List, Dict, Set
import httpx
from selectolax.parser import HTMLParser
from dateutil import parser as dateparser
import pandas as pd
from tqdm.auto import tqdm

BASE = "https://boursenews.ma"
SECTION = "/articles/marches"
START_URL = f"{BASE}{SECTION}"

# --------------------
# Config
# --------------------
OUTPUT_DIR = "/content/boursenews_marches_full"
CONCURRENCY = 24
REQS_PER_SEC = 8
TIMEOUT = 20.0
MAX_PAGES: Optional[int] = None           # e.g. 40
KEEP_ON_AFTER_DATE: Optional[str] = None  # "2025-07-01"
KEEP_ON_BEFORE_DATE: Optional[str] = None # "2025-12-31"
CHECKPOINT_EVERY = 50
DOWNLOAD_IMAGES = False

HEADERS = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

# --------------------
# Output dirs
# --------------------
os.makedirs(OUTPUT_DIR, exist_ok=True)
PAGES_DIR = os.path.join(OUTPUT_DIR, "pages")
IMAGES_DIR = os.path.join(OUTPUT_DIR, "images")
os.makedirs(PAGES_DIR, exist_ok=True)
if DOWNLOAD_IMAGES:
    os.makedirs(IMAGES_DIR, exist_ok=True)

JSONL_PATH = os.path.join(OUTPUT_DIR, "boursenews_marches.jsonl.gz")
CSV_PATH   = os.path.join(OUTPUT_DIR, "boursenews_marches.csv")
SEEN_PATH  = os.path.join(OUTPUT_DIR, "seen_urls.txt")

# --------------------
# Utils
# --------------------
def clean_ws(s: Optional[str]) -> Optional[str]:
    if not s: return s
    return re.sub(r"\s+", " ", s).strip()

def parse_date_iso(text: Optional[str]) -> Optional[str]:
    if not text: return None
    try:
        dt = dateparser.parse(text, dayfirst=True, fuzzy=True, languages=["fr","en"])
        return dt.date().isoformat() if dt else None
    except Exception:
        return None

def date_in_range(date_iso: Optional[str]) -> bool:
    if not (KEEP_ON_AFTER_DATE or KEEP_ON_BEFORE_DATE):
        return True
    if not date_iso:
        return False
    try:
        d = datetime.fromisoformat(date_iso).date()
        if KEEP_ON_AFTER_DATE and d < datetime.fromisoformat(KEEP_ON_AFTER_DATE).date():
            return False
        if KEEP_ON_BEFORE_DATE and d > datetime.fromisoformat(KEEP_ON_BEFORE_DATE).date():
            return False
        return True
    except Exception:
        return False

def listing_url(page: int) -> str:
    return START_URL if page == 1 else f"{START_URL}/{page}"

def slugify(url: str) -> str:
    path = urlparse(url).path.strip("/").replace("/", "-")
    path = re.sub(r"[^a-zA-Z0-9\-_.]", "-", path)[:140]
    return path or "article"

def append_jsonl_gz(obj: dict):
    with gzip.open(JSONL_PATH, "ab") as f:
        f.write(orjson.dumps(obj))
        f.write(b"\n")

def append_csv_row(row: dict, fields=("url","title","date_text","date_iso","author","snapshot_path","images_count")):
    header_needed = not os.path.exists(CSV_PATH)
    data = {
        "url": row.get("url"),
        "title": row.get("title"),
        "date_text": row.get("date_text"),
        "date_iso": row.get("date_iso"),
        "author": row.get("author"),
        "snapshot_path": row.get("snapshot_path"),
        "images_count": len(row.get("images") or []) if isinstance(row.get("images"), list) else 0
    }
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(fields))
        if header_needed:
            w.writeheader()
        w.writerow(data)

def load_seen_urls() -> Set[str]:
    if not os.path.exists(SEEN_PATH):
        return set()
    with open(SEEN_PATH, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def add_seen_url(u: str):
    with open(SEEN_PATH, "a", encoding="utf-8") as f:
        f.write(u + "\n")

def save_snapshot_html(article: dict):
    slug = slugify(article["url"])
    fname = f"{slug}.html"
    fpath = os.path.join(PAGES_DIR, fname)
    title = article.get("title") or slug
    body = article.get("content_html_clean") or ""
    date_txt = article.get("date_text") or ""
    html_doc = f"""<!doctype html>
<html lang="fr"><head><meta charset="utf-8">
<title>{title}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>body{{font-family:system-ui,-apple-system,Segoe UI,Roboto;max-width:820px;margin:40px auto;padding:0 16px;line-height:1.6}}</style>
</head><body>
<h1>{title}</h1>
<p><em>{date_txt}</em></p>
<hr/>
{body}
</body></html>"""
    with open(fpath, "w", encoding="utf-8") as f:
        f.write(html_doc)
    return os.path.relpath(fpath, OUTPUT_DIR)

# --------------------
# Networking + rate limit
# --------------------
class RateLimiter:
    def __init__(self, rps: float):
        self.interval = 1.0 / max(0.01, rps)
        self._last = 0.0
        self._lock = asyncio.Lock()
    async def wait(self):
        async with self._lock:
            now = time.perf_counter()
            delta = self._last + self.interval - now
            if delta > 0:
                await asyncio.sleep(delta)
                self._last = time.perf_counter()
            else:
                self._last = now

rate_limiter = RateLimiter(REQS_PER_SEC)

async def fetch(client: httpx.AsyncClient, url: str, *, max_attempts=5) -> Optional[str]:
    backoff = 1.0
    for attempt in range(1, max_attempts + 1):
        await rate_limiter.wait()
        try:
            r = await client.get(url, timeout=TIMEOUT)
            if r.status_code == 429:
                ra = int(r.headers.get("retry-after","3"))
                print(f"[429] {url} – sleep {ra}s"); await asyncio.sleep(ra); raise httpx.HTTPStatusError("429", request=r.request, response=r)
            if r.status_code >= 500:
                raise httpx.HTTPStatusError(str(r.status_code), request=r.request, response=r)
            r.raise_for_status()
            return r.text
        except Exception as e:
            if attempt == max_attempts:
                print(f"[ERROR] {url} – failed after {attempt} attempts: {e}")
                return None
            sl = min(backoff, 30.0) + random.uniform(0, 0.5)
            print(f"[WARN] {url} – attempt {attempt} failed, retry in {sl:.1f}s")
            await asyncio.sleep(sl)
            backoff = min(backoff * 2, 30.0)

def parse_listing(html: str) -> List[str]:
    tree = HTMLParser(html)
    urls = set()
    for a in tree.css("h3 a[href]"):
        href = (a.attributes.get("href") or "").strip()
        if "/article/" in href:
            urls.add(urljoin(BASE, href))
    if not urls:
        for a in tree.css("a[href]"):
            href = (a.attributes.get("href") or "").strip()
            if "/article/" in href:
                urls.add(urljoin(BASE, href))
    return list(urls)

# --------------------
# Article parsing (JSON-LD first)
# --------------------
def extract_jsonld_article(tree: HTMLParser) -> dict:
    out = {}
    for s in tree.css('script[type="application/ld+json"]'):
        try:
            raw = s.text() or ""
            data = json.loads(raw)
        except Exception:
            continue

        def to_list(x):
            if isinstance(x, list): return x
            return [x]

        for obj in to_list(data):
            if not isinstance(obj, dict): 
                continue
            t = obj.get("@type") or obj.get("@context")
            # NewsArticle object
            if (isinstance(t, str) and "NewsArticle" in t) or obj.get("@type") == "NewsArticle":
                out["headline"] = clean_ws(obj.get("headline"))
                out["datePublished"] = clean_ws(obj.get("datePublished"))
                out["dateModified"] = clean_ws(obj.get("dateModified"))
                art_body = obj.get("articleBody")
                if isinstance(art_body, str):
                    # JSON-LD on this site often contains HTML-escaped text
                    out["articleBody"] = html.unescape(art_body)
                # stop after the first good one
                return out
    return out

def parse_article(html_text: str, url: str) -> Dict:
    # default structure
    result = {
        "url": url, "title": None, "date_text": None, "date_iso": None,
        "author": None, "images": None, "content_text": None, "content_html_clean": None
    }

    try:
        tree = HTMLParser(html_text)
    except Exception:
        return result

    # 1) JSON-LD first
    jl = extract_jsonld_article(tree)
    if jl.get("headline"):
        result["title"] = jl["headline"]
    if jl.get("datePublished"):
        result["date_text"] = jl["datePublished"]
        result["date_iso"] = parse_date_iso(jl["datePublished"])
    # articleBody from JSON-LD becomes a fallback text if DOM body fails
    jsonld_body_text = jl.get("articleBody")

    # 2) DOM title/date fallback (site-specific)
    if not result["title"]:
        # .text_article_la_une h2 (with inner <a>)
        tnode = tree.css_first(".text_article_la_une")
        if tnode:
            h2 = tnode.css_first("h2")
            if h2:
                if h2.css_first("a") and h2.css_first("a").text():
                    result["title"] = clean_ws(h2.css_first("a").text())
                elif h2.text():
                    result["title"] = clean_ws(h2.text())
        if not result["title"]:
            # other fallbacks
            for sel in ("h1.article-title", ".post-title h1", "h1", "title"):
                n = tree.css_first(sel)
                if n and n.text():
                    result["title"] = clean_ws(n.text())
                    break

    if not result["date_text"]:
        dnode = tree.css_first(".text_article_la_une")
        if dnode:
            sp = dnode.css_first("span")
            if sp and sp.text():
                result["date_text"] = clean_ws(sp.text())
                result["date_iso"] = parse_date_iso(result["date_text"])

    # 3) BODY: keep only .article_detail_description
    body = tree.css_first(".article_detail_description")
    if not body:
        # guarded fallbacks (avoid site chrome)
        for sel in (".entry-content", ".article-content", "article"):
            body = tree.css_first(sel)
            if body and body.css_first("p"):
                break
    if not body:
        body = tree.body or tree

    # remove junk (no comma-joined selectors)
    for tag in ("script", "style", "nav", "aside", "iframe", "form", "noscript"):
        for n in body.css(tag):
            n.decompose()

    # absolutize links
    for el in body.css("[href]"):
        val = (el.attributes.get("href") or "").strip()
        if val:
            el.attributes["href"] = urljoin(url, val)
    for el in body.css("[src]"):
        val = (el.attributes.get("src") or "").strip()
        if val:
            el.attributes["src"] = urljoin(url, val)

    # full cleaned HTML (with tables, lists, inline styles retained)
    result["content_html_clean"] = body.html or None

    # plain text: from p and li only (keeps narrative, skips nav)
    parts = []
    for tag in ("p", "li"):
        for n in body.css(tag):
            t = clean_ws(n.text() or "")
            if t and len(t) > 2:
                parts.append(t)
    if parts:
        result["content_text"] = "\n\n".join(parts)
    elif jsonld_body_text:
        # fallback to JSON-LD articleBody plain text
        result["content_text"] = clean_ws(html.unescape(jsonld_body_text))

    # images from body
    images = []
    if result["content_html_clean"]:
        try:
            t2 = HTMLParser(result["content_html_clean"])
            for img in t2.css("img[src]"):
                s = (img.attributes.get("src") or "").strip()
                if s:
                    images.append(s)
            # dedupe
            seen = set(); out=[]
            for s in images:
                if s not in seen:
                    seen.add(s); out.append(s)
            images = out
        except Exception:
            images = None
    result["images"] = images or None

    # author (heuristic “- par …” inside date_text)
    if result["date_text"]:
        m = re.search(r"\s+-\s*par\s+(.*)$", result["date_text"], flags=re.I)
        if m:
            result["author"] = m.group(1).strip()

    return result

# --------------------
# Optional image downloader
# --------------------
async def download_image(client: httpx.AsyncClient, url: str):
    try:
        await rate_limiter.wait()
        r = await client.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        name = slugify(url)
        ext = os.path.splitext(urlparse(url).path)[1] or ".bin"
        path = os.path.join(IMAGES_DIR, f"{name}{ext}")
        with open(path, "wb") as f:
            f.write(r.content)
    except Exception:
        pass

# --------------------
# Crawl
# --------------------
async def crawl():
    seen = load_seen_urls()
    total_saved = len(seen)
    print(f"[INFO] Resuming with {total_saved} articles already saved.")

    limits = httpx.Limits(max_connections=64, max_keepalive_connections=32, keepalive_expiry=30.0)
    async with httpx.AsyncClient(http2=True, headers=HEADERS, limits=limits, follow_redirects=True) as client:
        page = 1
        pbar = tqdm(desc="Listing pages", unit="page")
        EMPTY_PAGES_LIMIT = 2  # stop after 2 pages with zero article links
        empty_streak = 0

        while True:
            if MAX_PAGES and page > MAX_PAGES:
                break

            list_url = listing_url(page)
            html_page = await fetch(client, list_url)
            if not html_page:
                print(f"[ERROR] listing {page} failed; stopping.")
                break

            # Find all article links on this page (not just new ones)
            all_links = parse_listing(html_page)
            if not all_links:
                empty_streak += 1
                print(f"[INFO] Page {page} has no article links (empty_streak={empty_streak}).")
                if empty_streak >= EMPTY_PAGES_LIMIT:
                    print("[INFO] Reached empty pages limit. Stopping.")
                    break
                page += 1
                pbar.update(1)
                continue

            empty_streak = 0
            links = [u for u in all_links if u not in seen]
            print(f"[PAGE {page}] total={len(all_links)} new={len(links)} (seen={len(seen)})")

            if links:
                sem = asyncio.Semaphore(CONCURRENCY)

                async def process_link(aurl: str):
                    nonlocal total_saved
                    async with sem:
                        art_html = await fetch(client, aurl)
                        if not art_html:
                            return
                        art = parse_article(art_html, aurl)
                        if not date_in_range(art.get("date_iso")):
                            return

                        snap_rel = save_snapshot_html(art)
                        art["snapshot_path"] = snap_rel

                        if DOWNLOAD_IMAGES and art.get("images"):
                            await asyncio.gather(*[download_image(client, im) for im in art["images"]])

                        append_jsonl_gz(art)
                        append_csv_row(art)
                        add_seen_url(aurl)
                        seen.add(aurl)
                        total_saved += 1
                        if total_saved % 10 == 0:
                            print(f"[PROGRESS] saved={total_saved}")
                        if total_saved % CHECKPOINT_EVERY == 0:
                            print(f"[CHECKPOINT] {total_saved} articles stored…")

                # Structured concurrency: all tasks complete/cancel before leaving the block
                try:
                    tg = asyncio.TaskGroup()
                    async with tg:
                        for u in links:
                            tg.create_task(process_link(u))
                except* Exception as eg:
                    # Non-fatal: continue to next page
                    print(f"[ERROR] A task failed on page {page}: {eg.exceptions[0]}")

            # advance to next listing page even if there were 0 new links
            page += 1
            pbar.update(1)

        pbar.close()

        print(f"[DONE] Total saved: {total_saved}")
        print(f"[PATH] JSONL (gz): {JSONL_PATH}")
        print(f"[PATH] CSV       : {CSV_PATH}")
        print(f"[PATH] Snapshots : {PAGES_DIR}")

await crawl()
