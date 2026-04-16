"""
DTC Brand Scraper — Apify Actor
================================
Scrapes Meta Ad Library directly via Playwright (no paid sub-actors).
Classifies brands by vertical using keyword matching.

Input (via Apify UI or API):
    searchTerms      list[str]   Keywords to search
    country          str         Country code (default: US)
    adsLimitPerTerm  int         Ads to scroll per term (default: 200)
    maxBrands        int         Max unique brands to output (default: 500)
"""

import asyncio
import logging
import re
from urllib.parse import urlparse, urlencode

from apify import Actor
from playwright.async_api import async_playwright, Page, BrowserContext

from classifier import classify, confidence_label

log = logging.getLogger(__name__)

AD_LIBRARY_BASE = "https://www.facebook.com/ads/library/"

# How long to wait for ads to load after scroll (ms)
SCROLL_WAIT_MS = 2000

# Pixels to scroll per step
SCROLL_STEP_PX = 1500


# ─────────────────────────────────────────
#  FACEBOOK AD LIBRARY SCRAPER
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


async def dismiss_overlays(page: Page) -> None:
    """Close cookie banners and login prompts if they appear."""
    overlay_selectors = [
        '[data-testid="cookie-policy-manage-dialog"] button',
        'div[role="dialog"] [aria-label="Close"]',
        '[aria-label="Close"]',
        'button[title="Close"]',
    ]
    for sel in overlay_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1500):
                await btn.click()
                await page.wait_for_timeout(500)
        except Exception:
            pass


async def extract_ads_from_page(page: Page) -> list[dict]:
    """
    Extract ad data from currently rendered Ad Library page.

    Strategy: intercept all <a> tags that point to external domains
    (these are the CTA links like "Shop Now", "Learn More") and
    pair them with the nearest advertiser name in the card.
    """
    ads = await page.evaluate("""
        () => {
            const results = [];
            const seen    = new Set();

            // Ad cards: Facebook wraps each ad in a div with role or
            // a predictable structure. We target the CTA links with
            // external URLs as anchors, then walk up to the card.
            const allLinks = Array.from(document.querySelectorAll('a[href]'));

            for (const link of allLinks) {
                const href = link.href || '';

                // Only external links (skip facebook.com internal)
                if (!href.startsWith('http')) continue;
                try {
                    const url = new URL(href);
                    if (url.hostname.includes('facebook.com')) continue;
                    if (url.hostname.includes('instagram.com')) continue;
                    if (url.hostname.includes('fb.com')) continue;
                    if (url.hostname.includes('fb.watch')) continue;

                    const domain = url.hostname.replace(/^www\\./, '');
                    if (!domain || seen.has(domain)) continue;
                    seen.add(domain);

                    // Walk up to find the ad card container (max 12 levels)
                    let card = link;
                    for (let i = 0; i < 12; i++) {
                        card = card.parentElement;
                        if (!card) break;
                    }

                    // Extract advertiser name: look for a link to a FB page
                    // (they contain /ads/library/?active_status or just a page URL)
                    let advertiser = '';
                    if (card) {
                        const pageLinks = card.querySelectorAll('a[href*="facebook.com"]');
                        for (const pl of pageLinks) {
                            const text = pl.innerText.trim();
                            if (text.length > 0 && text.length < 80) {
                                advertiser = text;
                                break;
                            }
                        }

                        // Fallback: any heading-like element
                        if (!advertiser) {
                            const heading = card.querySelector('h2, h3, strong, b');
                            if (heading) advertiser = heading.innerText.trim().slice(0, 80);
                        }
                    }

                    // Ad body text: grab visible text around the CTA link
                    let adText = '';
                    if (card) {
                        const paragraphs = card.querySelectorAll('p, span, div');
                        for (const p of paragraphs) {
                            const t = p.innerText.trim();
                            if (t.length > 20 && t.length < 300) {
                                adText = t;
                                break;
                            }
                        }
                    }

                    results.push({
                        domain:     domain,
                        website:    `https://${domain}`,
                        advertiser: advertiser || 'Unknown',
                        ad_text:    adText.slice(0, 200),
                        cta_url:    href,
                    });
                } catch (e) {
                    continue;
                }
            }
            return results;
        }
    """)
    return ads or []


async def scrape_search_term(
    context: BrowserContext,
    term: str,
    country: str,
    limit: int,
) -> list[dict]:
    """Scrape one search term, scrolling until limit reached or no new ads."""
    url  = build_search_url(term, country)
    page = await context.new_page()

    log.info(f"Navigating: {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)
        await dismiss_overlays(page)
        await page.wait_for_timeout(1000)
    except Exception as e:
        log.error(f"Failed to load page for '{term}': {e}")
        await page.close()
        return []

    collected: dict[str, dict] = {}
    no_new_streak = 0

    while len(collected) < limit:
        # Extract ads from current view
        batch = await extract_ads_from_page(page)
        new_count = 0
        for ad in batch:
            if ad["domain"] not in collected:
                collected[ad["domain"]] = ad
                new_count += 1

        log.info(f"  '{term}': {len(collected)} unique brands (batch +{new_count})")

        if new_count == 0:
            no_new_streak += 1
            if no_new_streak >= 3:
                log.info(f"  No new ads after 3 scrolls, stopping '{term}'")
                break
        else:
            no_new_streak = 0

        # Scroll down
        await page.evaluate(f"window.scrollBy(0, {SCROLL_STEP_PX})")
        await page.wait_for_timeout(SCROLL_WAIT_MS)

        # Check if we hit the bottom
        at_bottom = await page.evaluate(
            "(window.innerHeight + window.scrollY) >= document.body.scrollHeight - 200"
        )
        if at_bottom:
            log.info(f"  Reached page bottom for '{term}'")
            break

    await page.close()
    log.info(f"Finished '{term}': {len(collected)} brands")
    return list(collected.values())


# ─────────────────────────────────────────
#  CLASSIFICATION
# ─────────────────────────────────────────

def classify_brands(brands: list[dict]) -> list[dict]:
    """Classify each brand using keyword matching on domain + ad text."""
    for brand in brands:
        text            = f"{brand['domain']} {brand['ad_text']}"
        vertical, score = classify(text)
        brand["vertical"]    = vertical
        brand["confidence"]  = confidence_label(score)
        brand["kw_score"]    = score
    return brands


def apply_filters(
    brands: list[dict],
    filter_keywords: list[str],
    target_verticals: list[str],
) -> list[dict]:
    """
    Apply research filters to classified brands.

    filter_keywords  — keep only brands whose domain or ad text contains
                       at least one of these keywords (case-insensitive).
                       Empty list = keep all.

    target_verticals — keep only brands in these verticals.
                       Empty list = keep all.
    """
    result = brands

    if filter_keywords:
        kws = [kw.lower() for kw in filter_keywords]
        result = [
            b for b in result
            if any(kw in f"{b['domain']} {b['ad_text']}".lower() for kw in kws)
        ]
        log.info(f"After filterKeywords ({filter_keywords}): {len(result)} brands")

    if target_verticals:
        tvs = [v.lower() for v in target_verticals]
        result = [b for b in result if b.get("vertical", "").lower() in tvs]
        log.info(f"After targetVerticals ({target_verticals}): {len(result)} brands")

    return result


# ─────────────────────────────────────────
#  ACTOR ENTRY POINT
# ─────────────────────────────────────────

async def main() -> None:
    async with Actor:
        inp = await Actor.get_input() or {}

        search_terms      = inp.get("searchTerms",     ["shop now free shipping", "buy now limited offer", "try risk free"])
        filter_keywords   = inp.get("filterKeywords",   [])
        target_verticals  = inp.get("targetVerticals",  [])
        country           = inp.get("country",          "US")
        ads_limit         = inp.get("adsLimitPerTerm",  200)
        max_brands        = inp.get("maxBrands",        500)

        log.info(
            f"Starting DTC scraper | terms={len(search_terms)} | "
            f"limit/term={ads_limit} | country={country} | "
            f"filterKeywords={filter_keywords or 'all'} | "
            f"targetVerticals={target_verticals or 'all'}"
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
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )

            for term in search_terms:
                brands = await scrape_search_term(context, term, country, ads_limit)
                for b in brands:
                    if b["domain"] not in all_brands:
                        all_brands[b["domain"]] = b
                if len(all_brands) >= max_brands:
                    log.info(f"Reached maxBrands limit ({max_brands}), stopping")
                    break

            await browser.close()

        # Classify
        brand_list = list(all_brands.values())[:max_brands]
        classify_brands(brand_list)

        # Apply research filters
        brand_list = apply_filters(brand_list, filter_keywords, target_verticals)

        log.info(f"Total brands after filtering: {len(brand_list)}")

        # Push to Apify dataset
        await Actor.push_data(brand_list)

        # Summary log
        from collections import Counter
        verticals = Counter(b["vertical"] for b in brand_list)
        log.info("Vertical breakdown: " + " | ".join(f"{v}:{c}" for v, c in verticals.most_common()))


if __name__ == "__main__":
    asyncio.run(main())
