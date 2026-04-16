"""
DTC Brand Scraper — Apify Actor
================================
Scrapes Meta Ad Library via Playwright + GraphQL interception.
Primary: captures XHR/GraphQL API responses (structured JSON).
Fallback: HTML extraction of CTA links from ad cards.

Input (Apify UI):
    searchTerms      list[str]
    filterKeywords   list[str]   (optional, filters results)
    targetVerticals  list[str]   (optional, filters results)
    country          str
    adsLimitPerTerm  int
    maxBrands        int
"""

import asyncio
import json
import logging
import re
from urllib.parse import urlparse, urlencode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

from apify import Actor
from playwright.async_api import async_playwright, Page, BrowserContext

from classifier import classify, confidence_label

log = logging.getLogger(__name__)

AD_LIBRARY_BASE = "https://www.facebook.com/ads/library/"
SCROLL_STEP_PX  = 1200
SCROLL_WAIT_MS  = 2500


# ─────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────

def build_search_url(term: str, country: str) -> str:
    params = {
        "active_status": "all",
        "ad_type":       "all",
        "country":       country,
        "q":             term,
        "search_type":   "keyword_unordered",
        "media_type":    "all",
    }
    return f"{AD_LIBRARY_BASE}?{urlencode(params)}"


def extract_domain(url: str) -> str | None:
    """Return clean domain from URL, or None if invalid/Facebook."""
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

def _find_ads_recursive(node: object, results: list) -> None:
    """
    Walk any JSON structure and collect objects that look like ad entries.
    Facebook's GraphQL response nests ad data deeply — we don't rely on
    a fixed schema, just look for objects containing page_name + a URL.
    """
    if isinstance(node, dict):
        page_name = node.get("page_name") or node.get("advertiser_name") or ""
        snapshot  = node.get("snapshot") or {}

        # Candidate CTA URLs in order of preference
        link_url = (
            (snapshot.get("link_url") if isinstance(snapshot, dict) else None)
            or (snapshot.get("website_url") if isinstance(snapshot, dict) else None)
            or node.get("link_url")
            or node.get("website_url")
            or ""
        )

        # Ad body text
        body = ""
        if isinstance(snapshot, dict):
            body_node = snapshot.get("body") or {}
            body = (
                (body_node.get("markup") if isinstance(body_node, dict) else body_node)
                or snapshot.get("title")
                or snapshot.get("caption")
                or ""
            )

        domain = extract_domain(str(link_url))
        if page_name and domain:
            results.append({
                "domain":     domain,
                "website":    f"https://{domain}",
                "advertiser": str(page_name)[:80],
                "ad_text":    str(body)[:200],
                "cta_url":    str(link_url),
                "source":     "graphql",
            })

        for v in node.values():
            _find_ads_recursive(v, results)

    elif isinstance(node, list):
        for item in node:
            _find_ads_recursive(item, results)


def parse_graphql_response(text: str) -> list[dict]:
    """Parse a raw GraphQL response body and extract ad entries."""
    results: list[dict] = []
    try:
        # Facebook sometimes returns multiple JSON objects on separate lines
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                _find_ads_recursive(data, results)
            except json.JSONDecodeError:
                pass
    except Exception as e:
        log.debug(f"GraphQL parse error: {e}")
    return results


# ─────────────────────────────────────────
#  HTML FALLBACK EXTRACTOR
# ─────────────────────────────────────────

async def extract_ads_html_fallback(page: Page) -> list[dict]:
    """
    Fallback: find CTA links (target=_blank to external domains) on the page.
    Only used if GraphQL interception yielded nothing.
    """
    ads = await page.evaluate("""
        () => {
            const results = [];
            const seen    = new Set();

            // Target links that open in new tab (CTA buttons) to external domains
            const links = Array.from(document.querySelectorAll('a[href][target="_blank"], a[href][rel*="noopener"]'));

            for (const link of links) {
                const href = link.href || '';
                if (!href.startsWith('http')) continue;

                try {
                    const url    = new URL(href);
                    const domain = url.hostname.replace(/^www\\./, '');
                    const skip   = ['facebook.com','fb.com','instagram.com','fb.watch','metastatus.com'];

                    if (!domain || seen.has(domain)) continue;
                    if (skip.some(s => domain.includes(s))) continue;
                    if (!domain.includes('.')) continue;

                    seen.add(domain);

                    // Walk up max 15 levels to find the ad card
                    let card = link;
                    for (let i = 0; i < 15; i++) {
                        if (!card.parentElement) break;
                        card = card.parentElement;
                        // Stop at a "large enough" container
                        if (card.innerText && card.innerText.length > 100) break;
                    }

                    // Advertiser: look for FB page links within the card
                    let advertiser = '';
                    const fbLinks = card.querySelectorAll('a[href*="facebook.com/"]');
                    for (const fl of fbLinks) {
                        const t = (fl.innerText || '').trim();
                        if (t.length > 1 && t.length < 80) { advertiser = t; break; }
                    }

                    // Ad text: first substantial text block
                    let adText = '';
                    const texts = card.querySelectorAll('span, p');
                    for (const el of texts) {
                        const t = (el.innerText || '').trim();
                        if (t.length > 30 && t.length < 300) { adText = t; break; }
                    }

                    results.push({
                        domain:     domain,
                        website:    'https://' + domain,
                        advertiser: advertiser || 'Unknown',
                        ad_text:    adText.slice(0, 200),
                        cta_url:    href,
                        source:     'html_fallback',
                    });
                } catch (e) { continue; }
            }
            return results;
        }
    """)
    return ads or []


# ─────────────────────────────────────────
#  OVERLAY DISMISSAL
# ─────────────────────────────────────────

async def dismiss_overlays(page: Page) -> None:
    """Dismiss cookie banners, login prompts, and other overlays."""
    selectors = [
        '[data-testid="cookie-policy-manage-dialog"] button',
        'div[role="dialog"] [aria-label="Close"]',
        '[aria-label="Close"]',
        'button[title="Close"]',
        '[data-cookiebanner] button',
        # Facebook "Continue without logging in" or similar
        'div[role="dialog"] button:last-child',
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1000):
                await btn.click()
                await page.wait_for_timeout(600)
        except Exception:
            pass


# ─────────────────────────────────────────
#  CORE SCRAPER
# ─────────────────────────────────────────

async def scrape_search_term(
    context: BrowserContext,
    term: str,
    country: str,
    limit: int,
) -> list[dict]:
    url  = build_search_url(term, country)
    page = await context.new_page()

    # Collect all GraphQL responses for this page
    graphql_ads: list[dict] = []

    async def on_response(response):
        if "api/graphql" not in response.url and "graphql" not in response.url:
            return
        try:
            text = await response.text()
            # Quick check before full parse
            if "page_name" not in text and "advertiser_name" not in text:
                return
            found = parse_graphql_response(text)
            if found:
                log.debug(f"  GraphQL batch: +{len(found)} ads")
                graphql_ads.extend(found)
        except Exception:
            pass

    page.on("response", on_response)

    log.info(f"Loading: {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(5000)
        await dismiss_overlays(page)
        await page.wait_for_timeout(1500)
    except Exception as e:
        log.error(f"Navigation failed for '{term}': {e}")
        await page.close()
        return []

    title = await page.title()
    # Log visible text length — helps diagnose blank vs blocked vs loaded page
    body_text_len = await page.evaluate("() => document.body ? document.body.innerText.length : 0")
    link_count    = await page.evaluate("() => document.querySelectorAll('a[href]').length")
    log.info(f"  Page: title='{title}' | text_chars={body_text_len} | links={link_count}")
    try:
        screenshot = await page.screenshot(full_page=False)
        await Actor.set_value(
            f"screenshot_{term[:30].replace(' ', '_')}",
            screenshot,
            content_type="image/png",
        )
        log.info("  Screenshot saved to KV store")
    except Exception as e:
        log.warning(f"  Screenshot failed: {e}")

    collected: dict[str, dict] = {}
    no_new_streak = 0

    # Scroll loop
    for scroll_num in range(150):  # hard cap
        if len(collected) >= limit:
            break

        # Merge latest GraphQL results
        before = len(collected)
        for ad in graphql_ads:
            if ad["domain"] not in collected:
                collected[ad["domain"]] = ad
        graphql_ads.clear()

        new_from_graphql = len(collected) - before

        # If GraphQL gave nothing, try HTML fallback
        new_from_html = 0
        if new_from_graphql == 0:
            html_ads = await extract_ads_html_fallback(page)
            for ad in html_ads:
                if ad["domain"] not in collected:
                    collected[ad["domain"]] = ad
                    new_from_html += 1

        total_new = new_from_graphql + new_from_html
        log.info(
            f"  Scroll {scroll_num+1} | '{term}' | "
            f"total={len(collected)} (+gql={new_from_graphql} +html={new_from_html})"
        )

        if total_new == 0:
            no_new_streak += 1
            if no_new_streak >= 4:
                log.info(f"  No new ads for 4 scrolls — stopping '{term}'")
                break
        else:
            no_new_streak = 0

        # Scroll down
        await page.evaluate(f"window.scrollBy(0, {SCROLL_STEP_PX})")
        await page.wait_for_timeout(SCROLL_WAIT_MS)

        # Check bottom
        at_bottom = await page.evaluate(
            "(window.innerHeight + window.scrollY) >= document.body.scrollHeight - 300"
        )
        if at_bottom and scroll_num > 2:
            log.info(f"  Reached bottom for '{term}'")
            break

    await page.close()
    log.info(f"Done '{term}': {len(collected)} unique brands")
    return list(collected.values())


# ─────────────────────────────────────────
#  CLASSIFICATION + FILTERS
# ─────────────────────────────────────────

def classify_brands(brands: list[dict]) -> None:
    for brand in brands:
        text            = f"{brand['domain']} {brand['ad_text']}"
        vertical, score = classify(text)
        brand["vertical"]   = vertical
        brand["confidence"] = confidence_label(score)
        brand["kw_score"]   = score


def apply_filters(
    brands: list[dict],
    filter_keywords: list[str],
    target_verticals: list[str],
) -> list[dict]:
    result = brands

    if filter_keywords:
        kws    = [kw.lower() for kw in filter_keywords]
        result = [
            b for b in result
            if any(kw in f"{b['domain']} {b['ad_text']}".lower() for kw in kws)
        ]
        log.info(f"After filterKeywords: {len(result)} brands")

    if target_verticals:
        tvs    = [v.lower() for v in target_verticals]
        result = [b for b in result if b.get("vertical", "").lower() in tvs]
        log.info(f"After targetVerticals: {len(result)} brands")

    return result


# ─────────────────────────────────────────
#  ACTOR ENTRY POINT
# ─────────────────────────────────────────

async def main() -> None:
    async with Actor:
        inp = await Actor.get_input() or {}

        search_terms     = inp.get("searchTerms",    ["shop now free shipping", "buy now limited offer"])
        filter_keywords  = inp.get("filterKeywords",  [])
        target_verticals = inp.get("targetVerticals", [])
        country          = inp.get("country",         "US")
        ads_limit            = inp.get("adsLimitPerTerm",       200)
        max_brands           = inp.get("maxBrands",             500)
        use_residential_proxy = inp.get("useResidentialProxy",  True)

        log.info(
            f"DTC Scraper | terms={len(search_terms)} | "
            f"limit/term={ads_limit} | country={country} | "
            f"filter={filter_keywords or 'none'} | verticals={target_verticals or 'all'} | "
            f"proxy={'residential' if use_residential_proxy else 'none'}"
        )

        all_brands: dict[str, dict] = {}

        # Proxy: try RESIDENTIAL first, fall back to datacenter (faster)
        proxy_url = None
        if use_residential_proxy:
            for groups in (["RESIDENTIAL"], []):
                try:
                    proxy_cfg = await Actor.create_proxy_configuration(
                        groups=groups,
                        country_code="US",
                    )
                    proxy_url = await proxy_cfg.new_url()
                    label = "residential" if groups else "datacenter"
                    log.info(f"Proxy configured ({label}): {proxy_url[:40]}...")
                    break
                except Exception as e:
                    log.warning(f"Proxy group {groups} unavailable: {e}")

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                ],
                proxy={"server": proxy_url} if proxy_url else None,
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
                locale="en-US",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )

            # Remove webdriver fingerprint that Facebook detects
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {} };
            """)

            for term in search_terms:
                brands = await scrape_search_term(context, term, country, ads_limit)
                for b in brands:
                    if b["domain"] not in all_brands:
                        all_brands[b["domain"]] = b
                if len(all_brands) >= max_brands:
                    log.info(f"Reached maxBrands cap ({max_brands})")
                    break

            await browser.close()

        brand_list = list(all_brands.values())[:max_brands]
        classify_brands(brand_list)
        brand_list = apply_filters(brand_list, filter_keywords, target_verticals)

        log.info(f"Pushing {len(brand_list)} brands to dataset")
        await Actor.push_data(brand_list)

        from collections import Counter
        v = Counter(b["vertical"] for b in brand_list)
        log.info("Verticals: " + " | ".join(f"{k}:{c}" for k, c in v.most_common()))


if __name__ == "__main__":
    asyncio.run(main())
