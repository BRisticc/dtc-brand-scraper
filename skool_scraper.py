"""
Skool / Free-Community Scraper — Apify Actor
=============================================
Finds advertisers promoting FREE communities/groups on Meta Ad Library.
Searches with join/community/free-flavoured keywords, filters to free-only
ads, classifies verticals, and optionally verifies member count by visiting
the advertiser's actual community page.

Input:
    searchTerms       list[str]   (optional — uses defaults if omitted)
    joinKeywords      list[str]   (extra join/free keywords for ad filter)
    freeOnly          bool        (default: True — keep only "free" ads)
    checkMemberCount  bool        (default: False — visit site to scrape count)
    minMembers        int | null  (e.g. 500 — skip communities below this)
    maxMembers        int | null  (e.g. 50000 — skip communities above this)
    targetVerticals   list[str]   (e.g. ["coaching","business"])
    country           str         (default: ALL)
    isTargetedCountry bool        (default: False)
    adsLimitPerTerm   int         (default: 200)
    maxBrands         int         (default: 500)
    memberCheckTimeout int        (ms per site visit, default: 12000)
"""

import asyncio
import json
import logging
import re
from urllib.parse import urlparse, quote

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

from apify import Actor
from playwright.async_api import async_playwright, Page, BrowserContext

from classifier import classify, confidence_label

log = logging.getLogger(__name__)


# ─────────────────────────────────────────
#  COMMUNITY VERTICALS (extends classifier)
# ─────────────────────────────────────────
# Used for post-classification overrides when the base classifier
# scores low but community-specific signals are present.

COMMUNITY_KEYWORDS: dict[str, list[str]] = {
    "coaching": [
        "coach", "coaching", "mentor", "mentorship", "1-on-1", "one-on-one",
        "accountability", "transformation", "breakthrough", "mindset",
        "life coach", "business coach", "high-ticket", "results",
    ],
    "courses": [
        "course", "curriculum", "lesson", "module", "masterclass", "workshop",
        "bootcamp", "program", "learn", "skill", "certification", "training",
        "e-learning", "online course", "self-paced", "cohort",
    ],
    "business": [
        "entrepreneur", "startup", "founder", "ceo", "agency", "freelance",
        "revenue", "sales", "marketing", "growth", "scale", "leads", "funnel",
        "ecommerce", "dropshipping", "amazon fba", "passive income", "side hustle",
    ],
    "investing": [
        "invest", "stock", "crypto", "trading", "portfolio", "dividend",
        "real estate", "wealth", "financial freedom", "forex", "options",
        "nft", "defi", "asset", "roi", "market",
    ],
    "health_wellness": [
        "health", "wellness", "mindfulness", "meditation", "mental health",
        "therapy", "holistic", "nutrition", "weight loss", "gut", "hormone",
        "sleep", "stress", "anxiety", "depression", "self-care",
    ],
    "content_creator": [
        "youtube", "instagram", "tiktok", "creator", "influencer", "content",
        "social media", "brand deal", "sponsorship", "monetize", "viral",
        "newsletter", "podcast", "audience", "followers",
    ],
    "community": [
        "community", "tribe", "group", "network", "members", "membership",
        "join", "connect", "peer", "support group", "mastermind",
        "private group", "exclusive", "inner circle",
    ],
}


def classify_community(text: str) -> tuple[str, str]:
    """
    Try community-specific verticals first; fall back to base classifier.
    Returns (vertical, confidence_label).
    """
    lower = text.lower()
    scores: dict[str, int] = {}

    for vertical, keywords in COMMUNITY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in lower)
        if score:
            scores[vertical] = score

    if scores:
        best = max(scores, key=scores.get)
        return best, confidence_label(scores[best])

    # Fall back to DTC classifier
    v, s = classify(text)
    return v, confidence_label(s)


# ─────────────────────────────────────────
#  DEFAULT SEARCH TERMS
# ─────────────────────────────────────────

DEFAULT_SEARCH_TERMS = [
    "join free community",
    "join our free group",
    "free online community",
    "join the community",
    "join for free",
    "free membership community",
    "join now free",
    "free private community",
    "skool community free",
    "free masterclass join",
    "join free challenge",
    "free training community",
    "join waitlist free",
    "free access community",
    "free community entrepreneurs",
    "free group women",
    "free fitness community",
    "free coaching community",
    "free investing community",
    "free business community",
]

FREE_SIGNALS = [
    "free", "no cost", "at no charge", "$0", "zero cost", "complimentary",
    "join for free", "free access", "free membership", "free community",
    "free group", "free training", "free challenge", "free masterclass",
    "free coaching", "free workshop",
]

# ─────────────────────────────────────────
#  MEMBER COUNT
# ─────────────────────────────────────────

# Matches patterns in ad text like:
#   "Join 5,000+ members"  "12k members"  "2.3M members"  "500 member community"
_MEMBER_RE = re.compile(
    r"""
    (?:join\s+)?               # optional "join "
    (\d[\d,]*\.?\d*)           # number with optional commas / decimals
    \s*([kKmM]?)               # optional multiplier k/K/M
    \s*\+?\s*                  # optional +
    (?:members?|subscribers?|people|followers?)   # unit
    """,
    re.VERBOSE | re.IGNORECASE,
)


def parse_member_count(text: str) -> int | None:
    """Extract the first member-count mention from text. Returns int or None."""
    m = _MEMBER_RE.search(text)
    if not m:
        return None
    raw, multiplier = m.group(1), m.group(2).upper()
    try:
        n = float(raw.replace(",", ""))
        if multiplier == "K":
            n *= 1_000
        elif multiplier == "M":
            n *= 1_000_000
        return int(n)
    except ValueError:
        return None


# CSS / text selectors used when visiting community pages
_MEMBER_SITE_PATTERNS = [
    # Skool.com
    r"(\d[\d,]*\.?\d*)\s*([kKmM]?)\s*\+?\s*(?:members?)",
    # Generic: "X members", "X+ members"
    r"(\d[\d,]*)\s*\+?\s*(?:members?|subscribers?|people)",
]
_MEMBER_SITE_RE = re.compile(
    "|".join(_MEMBER_SITE_PATTERNS),
    re.IGNORECASE,
)


async def scrape_member_count(page: Page, url: str, timeout_ms: int) -> int | None:
    """
    Visit `url` and try to extract a member count from visible text.
    Returns int or None if not found.
    """
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(3000)
        text = await page.evaluate("() => document.body?.innerText || ''")
        m = _MEMBER_SITE_RE.search(text)
        if not m:
            return None
        # Group 1/2 from first pattern, 3 from second
        raw = m.group(1) or m.group(3)
        mult = (m.group(2) or "").upper()
        if not raw:
            return None
        n = float(raw.replace(",", ""))
        if mult == "K":
            n *= 1_000
        elif mult == "M":
            n *= 1_000_000
        return int(n)
    except Exception:
        return None


# ─────────────────────────────────────────
#  STEALTH
# ─────────────────────────────────────────

STEALTH_SCRIPT = """
() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const arr = [{ name:'Chrome PDF Plugin' }, { name:'Chrome PDF Viewer' }, { name:'Native Client' }];
            arr.item = i => arr[i];
            arr.namedItem = n => arr.find(p => p.name === n);
            arr.refresh = () => {};
            Object.defineProperty(arr, 'length', { get: () => 3 });
            return arr;
        }
    });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    if (!window.chrome) {
        window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {}, app: {} };
    }
    const origQuery = window.navigator.permissions && window.navigator.permissions.query;
    if (origQuery) {
        window.navigator.permissions.query = (params) =>
            params.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : origQuery.call(window.navigator.permissions, params);
    }
    const getParam = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {
        if (param === 37445) return 'Intel Inc.';
        if (param === 37446) return 'Intel Iris OpenGL Engine';
        return getParam.call(this, param);
    };
    window.navigator.getBattery && (window.navigator.getBattery = undefined);
}"""

AD_LIBRARY_BASE = "https://www.facebook.com/ads/library/"
SCROLL_STEP_PX  = 1200
SCROLL_WAIT_MS  = 2500


# ─────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────

def build_url(term: str, country: str, is_targeted: bool = False) -> str:
    return (
        f"{AD_LIBRARY_BASE}"
        f"?active_status=active"
        f"&ad_type=all"
        f"&country={country}"
        f"&is_targeted_country={'true' if is_targeted else 'false'}"
        f"&media_type=all"
        f"&q={quote(term)}"
        f"&search_type=keyword_unordered"
        f"&sort_data[direction]=desc"
        f"&sort_data[mode]=total_impressions"
    )


def extract_domain(url: str) -> str | None:
    if not url or not url.startswith("http"):
        return None
    try:
        host = urlparse(url).netloc.lower().replace("www.", "")
        skip = {"facebook.com", "fb.com", "fb.watch", "instagram.com",
                "l.facebook.com", "m.facebook.com", ""}
        return host if host not in skip and "." in host else None
    except Exception:
        return None


def detect_free(text: str) -> bool:
    lower = text.lower()
    return any(sig in lower for sig in FREE_SIGNALS)


def detect_join(text: str, extra_keywords: list[str]) -> bool:
    lower = text.lower()
    base  = ["join", "sign up", "register", "enroll", "subscribe"]
    return any(kw in lower for kw in base + extra_keywords)


# ─────────────────────────────────────────
#  GRAPHQL PARSER
# ─────────────────────────────────────────

def _find_ads(node: object, out: list) -> None:
    if isinstance(node, dict):
        page_name = (
            node.get("page_name") or node.get("pageName")
            or node.get("advertiser_name") or ""
        )
        snapshot = node.get("snapshot") or {}
        link_url = (
            (snapshot.get("link_url") or snapshot.get("linkUrl") if isinstance(snapshot, dict) else None)
            or node.get("link_url") or node.get("linkUrl")
            or node.get("website_url") or node.get("websiteUrl")
            or ""
        )

        if page_name and link_url:
            domain = extract_domain(str(link_url))
            if domain:
                body, cta = "", ""
                if isinstance(snapshot, dict):
                    b    = snapshot.get("body") or {}
                    body = (b.get("markup") if isinstance(b, dict) else str(b)) or snapshot.get("title", "")
                    cta  = snapshot.get("cta_text") or snapshot.get("ctaText") or ""
                out.append({
                    "domain":     domain,
                    "website":    f"https://{domain}",
                    "advertiser": str(page_name)[:80],
                    "ad_text":    str(body)[:300],
                    "cta":        str(cta)[:80],
                    "source":     "graphql",
                })

        for v in node.values():
            _find_ads(v, out)

    elif isinstance(node, list):
        for item in node:
            _find_ads(item, out)


def parse_graphql(text: str) -> list[dict]:
    out: list[dict] = []
    try:
        _find_ads(json.loads(text), out)
        if out:
            return out
    except (json.JSONDecodeError, ValueError):
        pass
    for line in text.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            _find_ads(json.loads(line), out)
        except (json.JSONDecodeError, ValueError):
            pass
    return out


# ─────────────────────────────────────────
#  DOM FALLBACK
# ─────────────────────────────────────────

async def extract_dom(page: Page) -> list[dict]:
    return await page.evaluate("""
        () => {
            const out  = [];
            const seen = new Set();
            const skipDomains = ['metastatus.com','fbcdn.net','fbsbx.com',
                                 'facebook.com','instagram.com','fb.watch','fb.com'];

            function unwrap(href) {
                try {
                    const u = new URL(href);
                    const real = u.searchParams.get('u') || u.searchParams.get('target');
                    if (real) return decodeURIComponent(real);
                } catch(e) {}
                return href;
            }

            for (const a of document.querySelectorAll('a[href]')) {
                const raw = a.href || '';
                if (!raw.startsWith('http')) continue;
                const href = unwrap(raw);
                try {
                    const u = new URL(href);
                    const d = u.hostname.replace(/^www\\./, '');
                    if (!d || !d.includes('.')) continue;
                    if (seen.has(d)) continue;
                    if (skipDomains.some(s => d === s || d.endsWith('.' + s))) continue;
                    seen.add(d);

                    let card = a;
                    for (let i = 0; i < 20; i++) {
                        if (!card.parentElement) break;
                        card = card.parentElement;
                        if ((card.innerText || '').length > 100) break;
                    }

                    let advertiser = '', adText = '', cta = '';
                    for (const fl of card.querySelectorAll('a[href*="facebook.com"]')) {
                        const t = (fl.innerText || '').trim();
                        if (t.length > 1 && t.length < 80) { advertiser = t; break; }
                    }
                    for (const el of card.querySelectorAll('span,p,div')) {
                        const t = (el.innerText || '').trim();
                        if (t.length > 25 && t.length < 400) { adText = t; break; }
                    }
                    for (const btn of card.querySelectorAll('a[role="button"],button,[class*="cta"]')) {
                        const t = (btn.innerText || '').trim();
                        if (t.length > 1 && t.length < 60) { cta = t; break; }
                    }

                    out.push({
                        domain: d, website: 'https://' + d,
                        advertiser: advertiser || 'Unknown',
                        ad_text: adText.slice(0, 300),
                        cta: cta.slice(0, 80),
                        source: 'dom'
                    });
                } catch(e) {}
            }
            return out;
        }
    """) or []


# ─────────────────────────────────────────
#  SCRAPE ONE SEARCH TERM
# ─────────────────────────────────────────

async def scrape_term(
    context: BrowserContext,
    term: str,
    country: str,
    limit: int,
    is_targeted: bool = False,
) -> list[dict]:
    url  = build_url(term, country, is_targeted)
    page = await context.new_page()
    await page.add_init_script(STEALTH_SCRIPT)

    captured: list[dict] = []

    async def on_response(response):
        if "api/graphql" not in response.url and "graphql" not in response.url:
            return
        try:
            text = await response.text()
            if len(text) < 50:
                return
            ads = parse_graphql(text)
            if ads:
                captured.extend(ads)
        except Exception:
            pass

    page.on("response", on_response)

    log.info(f"→ '{term}'")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)

        for sel in ['[aria-label="Close"]', '[data-testid*="cookie"] button',
                    'div[role="dialog"] button:last-child']:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=800):
                    await btn.click()
                    await page.wait_for_timeout(500)
            except Exception:
                pass

    except Exception as e:
        log.error(f"  Nav failed: {e}")
        await page.close()
        return []

    try:
        shot = await page.screenshot()
        await Actor.set_value(f"ss_{term[:20].replace(' ','_')}", shot, content_type="image/png")
    except Exception:
        pass

    collected: dict[str, dict] = {}
    no_new = 0

    for scroll_n in range(200):
        if len(collected) >= limit:
            break

        before = len(collected)

        for ad in captured:
            if ad["domain"] not in collected:
                collected[ad["domain"]] = ad
        captured.clear()

        if len(collected) == before:
            dom_ads = await extract_dom(page)
            for ad in dom_ads:
                if ad["domain"] not in collected:
                    collected[ad["domain"]] = ad

        new = len(collected) - before
        log.info(f"  scroll {scroll_n+1} | brands={len(collected)} +{new}")

        if new == 0:
            no_new += 1
            if no_new >= 5:
                log.info("  5 empty scrolls — done")
                break
        else:
            no_new = 0

        await page.evaluate(f"window.scrollBy(0, {SCROLL_STEP_PX})")
        await page.wait_for_timeout(SCROLL_WAIT_MS)

        at_bottom = await page.evaluate(
            "(window.innerHeight + window.scrollY) >= document.body.scrollHeight - 400"
        )
        if at_bottom and scroll_n > 2:
            log.info("  page bottom")
            break

    await page.close()
    log.info(f"  Done: {len(collected)} brands")
    return list(collected.values())


# ─────────────────────────────────────────
#  MEMBER COUNT CHECK PASS
# ─────────────────────────────────────────

async def check_member_counts(
    context: BrowserContext,
    brands: list[dict],
    timeout_ms: int,
) -> None:
    """
    Visit each brand's website and try to scrape the actual member count.
    Updates brand dict in-place: adds 'members_verified' and 'members_source'.
    """
    page = await context.new_page()
    await page.add_init_script(STEALTH_SCRIPT)

    for b in brands:
        # First try to parse from ad text (fast, no network)
        ad_count = parse_member_count(f"{b.get('ad_text','')} {b.get('cta','')}")
        if ad_count is not None:
            b["members_from_ad"]   = ad_count
            b["members_verified"]  = ad_count  # will be overwritten if site check succeeds
            b["members_source"]    = "ad_text"

        # Then visit the site
        site_count = await scrape_member_count(page, b["website"], timeout_ms)
        if site_count is not None:
            b["members_verified"] = site_count
            b["members_source"]   = "site"
            log.info(f"  members {b['domain']}: {site_count:,} (site)")
        elif ad_count is not None:
            log.info(f"  members {b['domain']}: {ad_count:,} (ad text)")
        else:
            b.setdefault("members_verified", None)
            b.setdefault("members_source",   "none")
            log.info(f"  members {b['domain']}: not found")

    await page.close()


# ─────────────────────────────────────────
#  ENRICH + FILTER
# ─────────────────────────────────────────

def enrich(brands: list[dict], join_keywords: list[str]) -> None:
    for b in brands:
        text = f"{b.get('ad_text','')} {b.get('cta','')}".lower()
        b["is_free"]   = detect_free(text)
        b["has_join"]  = detect_join(text, join_keywords)

        # Member count from ad text (without site visit)
        if "members_verified" not in b:
            count = parse_member_count(text)
            b["members_from_ad"]  = count
            b["members_verified"] = count
            b["members_source"]   = "ad_text" if count is not None else "none"

        # Vertical classification
        v, conf = classify_community(text)
        b["vertical"]   = v
        b["confidence"] = conf


def apply_filters(
    brands: list[dict],
    free_only: bool,
    join_keywords: list[str],
    target_verts: list[str],
    min_members: int | None,
    max_members: int | None,
) -> list[dict]:

    if free_only:
        before = len(brands)
        brands = [b for b in brands if b.get("is_free")]
        log.info(f"freeOnly:      {before} → {len(brands)}")

    if join_keywords:
        kws    = [k.lower() for k in join_keywords]
        before = len(brands)
        brands = [
            b for b in brands
            if any(kw in f"{b.get('ad_text','')} {b.get('cta','')}".lower() for kw in kws)
        ]
        log.info(f"joinKeywords:  {before} → {len(brands)}")

    if target_verts:
        tvs    = [v.lower() for v in target_verts]
        before = len(brands)
        brands = [b for b in brands if b.get("vertical", "") in tvs]
        log.info(f"targetVerts:   {before} → {len(brands)}")

    if min_members is not None:
        before = len(brands)
        # Keep brands where count is known AND >= min, or count is unknown (None)
        # If you want strict filtering (drop unknowns), change `or count is None` to `and count is not None`
        brands = [
            b for b in brands
            if (b.get("members_verified") or 0) >= min_members
            or b.get("members_verified") is None
        ]
        log.info(f"minMembers≥{min_members}: {before} → {len(brands)}")

    if max_members is not None:
        before = len(brands)
        brands = [
            b for b in brands
            if b.get("members_verified") is None
            or b.get("members_verified") <= max_members
        ]
        log.info(f"maxMembers≤{max_members}: {before} → {len(brands)}")

    return brands


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────

async def main() -> None:
    async with Actor:
        inp = await Actor.get_input() or {}

        search_terms        = inp.get("searchTerms",        DEFAULT_SEARCH_TERMS)
        join_keywords       = inp.get("joinKeywords",       [])
        free_only           = inp.get("freeOnly",           True)
        check_members       = inp.get("checkMemberCount",   False)
        min_members         = inp.get("minMembers",         None)
        max_members         = inp.get("maxMembers",         None)
        target_verts        = inp.get("targetVerticals",    [])
        country             = inp.get("country",            "ALL")
        is_targeted         = inp.get("isTargetedCountry",  False)
        ads_limit           = inp.get("adsLimitPerTerm",    200)
        max_brands          = inp.get("maxBrands",          500)
        member_timeout      = inp.get("memberCheckTimeout", 12000)

        log.info(
            f"Config | terms={len(search_terms)} freeOnly={free_only} "
            f"checkMembers={check_members} min={min_members} max={max_members} "
            f"country={country}"
        )

        all_brands: dict[str, dict] = {}

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
                locale="en-US",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )

            # Phase 1 — scrape Meta Ad Library
            for term in search_terms:
                brands = await scrape_term(context, term, country, ads_limit, is_targeted)
                for b in brands:
                    if b["domain"] not in all_brands:
                        all_brands[b["domain"]] = b
                if len(all_brands) >= max_brands:
                    log.info(f"maxBrands={max_brands} reached")
                    break

            result = list(all_brands.values())[:max_brands]

            # Enrich: free/join flags + vertical + ad-text member count
            jkws = [k.lower() for k in join_keywords]
            enrich(result, jkws)

            # Phase 2 (optional) — visit each site to verify member count
            if check_members:
                log.info(f"Checking member counts for {len(result)} brands…")
                await check_member_counts(context, result, member_timeout)

            await browser.close()

        # Filter
        result = apply_filters(result, free_only, jkws, target_verts, min_members, max_members)

        log.info(f"Pushing {len(result)} brands")
        await Actor.push_data(result)

        # Summary stats
        from collections import Counter
        free_n  = sum(1 for b in result if b.get("is_free"))
        join_n  = sum(1 for b in result if b.get("has_join"))
        known_m = [b["members_verified"] for b in result if b.get("members_verified")]
        avg_m   = int(sum(known_m) / len(known_m)) if known_m else 0

        log.info(
            f"Summary | total={len(result)} is_free={free_n} has_join={join_n} "
            f"member_count_known={len(known_m)} avg_members={avg_m:,}"
        )
        log.info("Verticals: " + " | ".join(
            f"{k}:{c}" for k, c in Counter(b["vertical"] for b in result).most_common()
        ))


if __name__ == "__main__":
    asyncio.run(main())
