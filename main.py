"""
DTC Brand Scraper — Apify Actor
================================
Playwright + stealth mode scraper for Meta Ad Library.
Intercepts GraphQL API responses for structured ad data.
Falls back to DOM extraction if GraphQL yields nothing.

Input:
    searchTerms      list[str]
    filterKeywords   list[str]   (optional)
    targetVerticals  list[str]   (optional)
    country          str         (default: US)
    adsLimitPerTerm  int         (default: 200)
    maxBrands        int         (default: 500)
"""

import asyncio
import json
import logging
from urllib.parse import urlparse, urlencode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

from apify import Actor
from playwright.async_api import async_playwright, Page, BrowserContext

from classifier import classify, confidence_label

# Stealth JS injected before every page load
# Patches the signals Facebook uses to detect headless Chromium
STEALTH_SCRIPT = """
() => {
    // 1. Remove webdriver flag
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

    // 2. Fake plugins (real Chrome has plugins)
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

    // 3. Languages
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });

    // 4. Chrome runtime object
    if (!window.chrome) {
        window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {}, app: {} };
    }

    // 5. Permissions API (headless fails this check)
    const origQuery = window.navigator.permissions && window.navigator.permissions.query;
    if (origQuery) {
        window.navigator.permissions.query = (params) =>
            params.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : origQuery.call(window.navigator.permissions, params);
    }

    // 6. WebGL vendor/renderer (headless exposes SwiftShader)
    const getParam = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {
        if (param === 37445) return 'Intel Inc.';
        if (param === 37446) return 'Intel Iris OpenGL Engine';
        return getParam.call(this, param);
    };

    // 7. Hide automation-related console errors
    window.navigator.getBattery && (window.navigator.getBattery = undefined);
}"""

log = logging.getLogger(__name__)

AD_LIBRARY_BASE = "https://www.facebook.com/ads/library/"
SCROLL_STEP_PX  = 1200
SCROLL_WAIT_MS  = 2500


# ─────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────

def build_url(term: str, country: str, is_targeted: bool = False) -> str:
    from urllib.parse import quote
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


# ─────────────────────────────────────────
#  GRAPHQL RESPONSE PARSER
# ─────────────────────────────────────────

def _find_ads(node: object, out: list) -> None:
    """
    Recursively walk JSON and collect ad-like objects.
    Handles both snake_case and camelCase field names from Facebook.
    """
    if isinstance(node, dict):
        # Field name variations Facebook uses
        page_name = (
            node.get("page_name")
            or node.get("pageName")
            or node.get("advertiser_name")
            or ""
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
                body = ""
                if isinstance(snapshot, dict):
                    b = snapshot.get("body") or {}
                    body = (b.get("markup") if isinstance(b, dict) else str(b)) or snapshot.get("title", "")
                out.append({
                    "domain":     domain,
                    "website":    f"https://{domain}",
                    "advertiser": str(page_name)[:80],
                    "ad_text":    str(body)[:200],
                    "source":     "graphql",
                })

        for v in node.values():
            _find_ads(v, out)

    elif isinstance(node, list):
        for item in node:
            _find_ads(item, out)


def parse_graphql(text: str) -> list[dict]:
    out: list[dict] = []
    # Try whole body first (single large JSON object)
    try:
        _find_ads(json.loads(text), out)
        if out:
            return out
    except (json.JSONDecodeError, ValueError):
        pass
    # Try line-by-line (streamed / multi-object responses)
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
#  DOM FALLBACK EXTRACTOR
# ─────────────────────────────────────────

async def extract_dom(page: Page) -> list[dict]:
    """
    Extract advertiser CTA links from DOM.
    Facebook wraps all outbound links as l.facebook.com/l.php?u=REAL_URL —
    we unwrap those to get the actual advertiser domain.
    """
    return await page.evaluate("""
        () => {
            const out  = [];
            const seen = new Set();
            const skipDomains = ['metastatus.com','fbcdn.net','fbsbx.com',
                                 'facebook.com','instagram.com','fb.watch','fb.com'];

            function unwrap(href) {
                // Facebook redirect: l.facebook.com/l.php?u=ENCODED or
                // www.facebook.com/flx/warn/?u=ENCODED
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

                    // Walk up to find ad card
                    let card = a;
                    for (let i = 0; i < 20; i++) {
                        if (!card.parentElement) break;
                        card = card.parentElement;
                        if ((card.innerText || '').length > 100) break;
                    }

                    let advertiser = '', adText = '';
                    for (const fl of card.querySelectorAll('a[href*="facebook.com"]')) {
                        const t = (fl.innerText || '').trim();
                        if (t.length > 1 && t.length < 80) { advertiser = t; break; }
                    }
                    for (const el of card.querySelectorAll('span,p,div')) {
                        const t = (el.innerText || '').trim();
                        if (t.length > 25 && t.length < 300) { adText = t; break; }
                    }

                    out.push({ domain: d, website: 'https://' + d,
                               advertiser: advertiser || 'Unknown',
                               ad_text: adText.slice(0, 200), source: 'dom' });
                } catch(e) {}
            }
            return out;
        }
    """) or []


# ─────────────────────────────────────────
#  SCRAPE ONE SEARCH TERM
# ─────────────────────────────────────────

async def scrape_term(context: BrowserContext, term: str, country: str, limit: int, is_targeted: bool = False) -> list[dict]:
    url  = build_url(term, country, is_targeted)
    page = await context.new_page()
    await page.add_init_script(STEALTH_SCRIPT)

    captured: list[dict] = []

    async def on_response(response):
        url_str = response.url
        if "api/graphql" not in url_str and "graphql" not in url_str:
            return
        try:
            text = await response.text()
            if len(text) < 50:
                return
            ads = parse_graphql(text)
            if ads:
                log.debug(f"  GQL batch +{len(ads)}")
                captured.extend(ads)
        except Exception:
            pass

    page.on("response", on_response)

    log.info(f"→ '{term}'")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)

        # Dismiss any overlays
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

    title    = await page.title()
    txt_len  = await page.evaluate("() => document.body?.innerText?.length || 0")
    log.info(f"  title='{title}' text_chars={txt_len}")

    # Debug: count all links and sample external ones
    link_debug = await page.evaluate("""
        () => {
            const all   = document.querySelectorAll('a[href]');
            const ext   = [...all].filter(a => {
                try { return !new URL(a.href).hostname.includes('facebook'); } catch(e) { return false; }
            });
            const blank = [...all].filter(a => a.target === '_blank');
            const sample = ext.slice(0, 5).map(a => a.href);
            return { total: all.length, external: ext.length, blank: blank.length, sample };
        }
    """)
    log.info(f"  links: total={link_debug['total']} external={link_debug['external']} blank={link_debug['blank']}")
    if link_debug['sample']:
        log.info(f"  sample external: {link_debug['sample']}")

    # Screenshot
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

        # Merge GraphQL captures
        for ad in captured:
            if ad["domain"] not in collected:
                collected[ad["domain"]] = ad
        captured.clear()

        # DOM fallback if GraphQL gave nothing this round
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
                log.info(f"  5 empty scrolls — done")
                break
        else:
            no_new = 0

        await page.evaluate(f"window.scrollBy(0, {SCROLL_STEP_PX})")
        await page.wait_for_timeout(SCROLL_WAIT_MS)

        at_bottom = await page.evaluate(
            "(window.innerHeight + window.scrollY) >= document.body.scrollHeight - 400"
        )
        if at_bottom and scroll_n > 2:
            log.info(f"  page bottom")
            break

    await page.close()
    log.info(f"  Done: {len(collected)} brands")
    return list(collected.values())


# ─────────────────────────────────────────
#  CLASSIFY + FILTER
# ─────────────────────────────────────────

def classify_all(brands: list[dict]) -> None:
    for b in brands:
        v, s = classify(f"{b['domain']} {b['ad_text']}")
        b["vertical"]   = v
        b["confidence"] = confidence_label(s)


def apply_filters(brands, filter_kws, target_verts):
    if filter_kws:
        kws    = [k.lower() for k in filter_kws]
        brands = [b for b in brands if any(k in f"{b['domain']} {b['ad_text']}".lower() for k in kws)]
        log.info(f"After filterKeywords: {len(brands)}")
    if target_verts:
        tvs    = [v.lower() for v in target_verts]
        brands = [b for b in brands if b.get("vertical", "") in tvs]
        log.info(f"After targetVerticals: {len(brands)}")
    return brands


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────

async def main() -> None:
    async with Actor:
        inp = await Actor.get_input() or {}

        search_terms     = inp.get("searchTerms",        ["belts"])
        filter_kws       = inp.get("filterKeywords",     [])
        target_verts     = inp.get("targetVerticals",    [])
        country          = inp.get("country",            "ALL")
        is_targeted      = inp.get("isTargetedCountry",  False)
        ads_limit        = inp.get("adsLimitPerTerm",    200)
        max_brands       = inp.get("maxBrands",          500)

        log.info(f"Starting | terms={len(search_terms)} | country={country} | limit={ads_limit}")

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

            for term in search_terms:
                brands = await scrape_term(context, term, country, ads_limit, is_targeted)
                for b in brands:
                    if b["domain"] not in all_brands:
                        all_brands[b["domain"]] = b
                if len(all_brands) >= max_brands:
                    break

            await browser.close()

        result = list(all_brands.values())[:max_brands]
        classify_all(result)
        result = apply_filters(result, filter_kws, target_verts)

        log.info(f"Pushing {len(result)} brands")
        await Actor.push_data(result)

        from collections import Counter
        log.info("Verticals: " + " | ".join(
            f"{k}:{c}" for k, c in Counter(b["vertical"] for b in result).most_common()
        ))


if __name__ == "__main__":
    asyncio.run(main())
