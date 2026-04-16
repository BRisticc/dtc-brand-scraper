"""
DTC Brand Scraper + Vertical Classifier
========================================
100% Apify infrastructure — no LLM, no local HTTP calls.
Designed to run as an Apify actor (GitHub integration).
APIFY_API_TOKEN is injected automatically by the platform.

Pipeline:
  1. Apify: facebook-ads-library-scraper  → raw ads
  2. Extract unique advertiser domains
  3. Keyword-classify what we can (free, instant)
  4. Apify: website-content-crawler        → homepage text for unclear brands
  5. Keyword-classify again with homepage text
  6. Save results to CSV + Apify dataset

Local development:
    pip install apify-client
    APIFY_API_TOKEN=xxx python dtc_scraper.py
"""

import csv
import time
import logging
from urllib.parse import urlparse

from apify_client import ApifyClient

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
ADS_LIMIT_PER_TERM    = 200   # ads per search term (~$0.50 / 1000 results)
MAX_SITES_TO_CLASSIFY = 500   # max brands to process
OUTPUT_CSV            = "dtc_brands.csv"

# Apify actor IDs
ACTOR_ADS     = "apify/facebook-ads-library-scraper"
ACTOR_CRAWLER = "apify/website-content-crawler"

SEARCH_TERMS = [
    "shop now free shipping",
    "buy now limited offer",
    "try risk free",
    "subscribe and save",
    "as seen on tiktok",
]

# ─────────────────────────────────────────
#  VERTICAL TAXONOMY
#  More keywords = better keyword-only accuracy
# ─────────────────────────────────────────
VERTICALS: dict[str, list[str]] = {
    "beauty": [
        "skincare", "makeup", "cosmetics", "serum", "moisturizer", "lipstick",
        "foundation", "beauty", "glow", "toner", "cleanser", "sunscreen", "spf",
        "retinol", "hyaluronic", "niacinamide", "blush", "concealer", "mascara",
        "eyeshadow", "bronzer", "highlighter", "primer", "exfoliant", "face mask",
        "derma", "anti-aging", "wrinkle", "acne", "pore",
    ],
    "supplements": [
        "supplement", "vitamin", "protein", "collagen", "probiotic", "omega",
        "wellness", "health", "nootropic", "magnesium", "zinc", "iron", "b12",
        "ashwagandha", "turmeric", "melatonin", "biotin", "creatine", "electrolyte",
        "gut health", "immune", "energy boost", "metabolism", "detox", "cleanse",
        "greens powder", "superfoods",
    ],
    "fashion": [
        "clothing", "apparel", "dress", "shirt", "jeans", "fashion", "outfit",
        "wear", "style", "shoes", "sneakers", "boots", "jacket", "hoodie",
        "leggings", "swimwear", "activewear", "loungewear", "streetwear",
        "collection", "season", "wardrobe", "fits", "tee", "pants", "shorts",
        "blazer", "coat", "accessories", "bags", "purse", "jewelry",
    ],
    "food_beverage": [
        "coffee", "tea", "snack", "food", "drink", "nutrition", "meal", "organic",
        "eat", "beverage", "sauce", "spice", "seasoning", "protein bar", "granola",
        "keto", "paleo", "vegan food", "gluten-free", "dairy-free", "smoothie",
        "juice", "energy drink", "soda", "sparkling water", "kombucha", "chocolate",
        "candy", "cookie", "chip", "popcorn", "jerky", "nut butter", "honey",
    ],
    "fitness": [
        "workout", "gym", "training", "fitness", "exercise", "yoga", "pilates",
        "sport", "active", "running", "cycling", "hiit", "crossfit", "weightlifting",
        "resistance band", "dumbbell", "kettlebell", "foam roller", "mat", "bench",
        "treadmill", "rowing", "home gym",
    ],
    "pet": [
        "dog", "cat", "pet", "paw", "treat", "fur", "animal", "puppy", "kitten",
        "kibble", "leash", "collar", "grooming", "vet", "flea", "tick", "breed",
        "canine", "feline", "aquarium", "bird", "hamster", "rabbit",
    ],
    "home": [
        "home", "decor", "furniture", "candle", "clean", "kitchen", "bedroom",
        "living", "bathroom", "organization", "storage", "shelf", "lamp", "rug",
        "curtain", "pillow", "bedding", "mattress", "sofa", "desk", "chair",
        "plant", "garden", "outdoor", "patio", "cleaning", "laundry", "dishwasher",
    ],
    "baby_kids": [
        "baby", "kids", "toddler", "child", "infant", "parent", "mom", "dad",
        "newborn", "diaper", "stroller", "car seat", "nursing", "breastfeed",
        "pacifier", "teether", "toy", "educational", "school", "backpack",
    ],
    "tech_gadgets": [
        "device", "gadget", "tech", "smart", "wireless", "charger", "earbuds",
        "wearable", "bluetooth", "usb", "cable", "phone", "laptop", "tablet",
        "speaker", "headphones", "camera", "drone", "smartwatch", "tracking",
        "gps", "sensor", "led", "app",
    ],
    "other": [],
}

# ─────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
#  STEP 1: SCRAPE META AD LIBRARY (Apify)
# ─────────────────────────────────────────
def scrape_meta_ads(client: ApifyClient, search_terms: list[str], limit_per_term: int) -> list[dict]:
    """Pull ads from Meta Ad Library via Apify actor."""
    all_ads: list[dict] = []

    for term in search_terms:
        log.info(f"Scraping ads for: '{term}' (limit={limit_per_term})")
        for attempt in range(3):
            try:
                run = client.actor(ACTOR_ADS).call(
                    run_input={
                        "searchTerms": [term],
                        "country":     "US",
                        "adType":      "ALL",
                        "maxResults":  limit_per_term,
                    }
                )
                items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
                log.info(f"  ✓ {len(items)} ads")
                all_ads.extend(items)
                break
            except Exception as e:
                log.error(f"  Attempt {attempt+1}/3 failed: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))

        time.sleep(2)

    log.info(f"Total ads collected: {len(all_ads)}")
    return all_ads


# ─────────────────────────────────────────
#  STEP 2: EXTRACT UNIQUE DOMAINS
# ─────────────────────────────────────────
def extract_brands(ads: list[dict]) -> list[dict]:
    """Extract unique advertiser domains from raw ads."""
    seen: set[str] = set()
    brands: list[dict] = []

    for ad in ads:
        url = (
            ad.get("websiteUrl") or ad.get("website_url")
            or ad.get("link_url") or ""
        )
        advertiser = (
            ad.get("pageName") or ad.get("page_name")
            or ad.get("advertiserName") or "Unknown"
        )
        page_id  = ad.get("pageId") or ad.get("page_id") or ""
        ad_title = (
            ad.get("title") or ad.get("ad_creative_body")
            or ad.get("body", "")[:80] or ""
        )

        if not url:
            continue

        try:
            parsed = urlparse(url if url.startswith("http") else f"https://{url}")
            domain = parsed.netloc.lower().replace("www.", "")
            if not domain or domain in seen:
                continue
            seen.add(domain)
            brands.append({
                "domain":         domain,
                "website":        f"https://{domain}",
                "advertiser":     advertiser,
                "page_id":        page_id,
                "sample_ad":      ad_title,
                "vertical":       None,
                "business_model": None,
                "confidence":     None,
                "source":         None,
            })
        except Exception:
            continue

    log.info(f"Unique domains: {len(brands)}")
    return brands


# ─────────────────────────────────────────
#  STEP 3a: KEYWORD CLASSIFIER
# ─────────────────────────────────────────
def keyword_classify(text: str) -> tuple[str, int]:
    """
    Score text against all verticals.
    Returns (best_vertical, score). Score 0 means no match.
    """
    lowered = text.lower()
    scores  = {v: 0 for v in VERTICALS}

    for vertical, keywords in VERTICALS.items():
        for kw in keywords:
            if kw in lowered:
                scores[vertical] += 1

    best     = max(scores, key=scores.get)
    best_score = scores[best]
    return best, best_score


def classify_pass1(brands: list[dict]) -> list[dict]:
    """
    Fast keyword pass using domain + ad text only.
    Marks brand as classified or flags for web crawl.
    """
    needs_crawl = []

    for brand in brands:
        text = f"{brand['domain']} {brand['sample_ad']}"
        vertical, score = keyword_classify(text)

        if score >= 2:                      # confident match
            brand["vertical"]       = vertical
            brand["business_model"] = "dtc_only"
            brand["confidence"]     = "high" if score >= 4 else "medium"
            brand["source"]         = "keyword_pass1"
        elif score == 1:                    # weak match, crawl to confirm
            brand["vertical"]       = vertical
            brand["business_model"] = "dtc_only"
            brand["confidence"]     = "low"
            brand["source"]         = "keyword_pass1_weak"
            needs_crawl.append(brand)
        else:                               # no match at all, must crawl
            needs_crawl.append(brand)

    classified = len(brands) - len(needs_crawl)
    log.info(f"Pass 1 — classified: {classified} | needs crawl: {len(needs_crawl)}")
    return needs_crawl


# ─────────────────────────────────────────
#  STEP 3b: BATCH CRAWL HOMEPAGES (Apify)
# ─────────────────────────────────────────
def crawl_homepages(client: ApifyClient, brands: list[dict]) -> dict[str, str]:
    """
    Use Apify website-content-crawler to fetch homepage text for a batch of brands.
    Returns {domain: text_content} mapping.
    """
    if not brands:
        return {}

    start_urls = [{"url": brand["website"]} for brand in brands]
    log.info(f"Crawling {len(start_urls)} homepages via Apify...")

    try:
        run = client.actor(ACTOR_CRAWLER).call(
            run_input={
                "startUrls":         start_urls,
                "maxCrawlDepth":     0,      # homepage only
                "maxCrawlPages":     len(start_urls),
                "maxResults":        len(start_urls),
                "crawlerType":       "cheerio",   # fast, no JS rendering needed
                "excludeUrlGlobs":   [],
                "removeCookieWarnings": True,
            }
        )
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        log.info(f"  ✓ {len(items)} pages crawled")
    except Exception as e:
        log.error(f"Crawler error: {e}")
        return {}

    # Map back to domain → text
    result: dict[str, str] = {}
    for item in items:
        url  = item.get("url", "")
        text = item.get("text") or item.get("markdown") or item.get("content", "")
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower().replace("www.", "")
            if domain:
                result[domain] = str(text)[:2000]
        except Exception:
            continue

    return result


# ─────────────────────────────────────────
#  STEP 3c: RE-CLASSIFY WITH HOMEPAGE TEXT
# ─────────────────────────────────────────
def classify_pass2(brands: list[dict], website_texts: dict[str, str]) -> None:
    """
    Re-run keyword classifier using full homepage text.
    Updates brands in place.
    """
    for brand in brands:
        domain  = brand["domain"]
        hp_text = website_texts.get(domain, "")
        combined = f"{domain} {brand['sample_ad']} {hp_text}"

        vertical, score = keyword_classify(combined)
        brand["vertical"]       = vertical
        brand["business_model"] = "dtc_only"
        brand["source"]         = "keyword_pass2"

        if score >= 4:
            brand["confidence"] = "high"
        elif score >= 2:
            brand["confidence"] = "medium"
        else:
            brand["confidence"] = "low"

    log.info("Pass 2 classification complete")


# ─────────────────────────────────────────
#  STEP 4: SAVE TO CSV + PUSH TO APIFY DATASET
# ─────────────────────────────────────────
def save_csv(brands: list[dict], filepath: str) -> None:
    if not brands:
        log.warning("No brands to save.")
        return

    fieldnames = [
        "domain", "website", "advertiser", "page_id",
        "vertical", "business_model", "confidence", "source", "sample_ad",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(brands)

    log.info(f"Saved {len(brands)} brands → {filepath}")


def push_to_apify_dataset(client: ApifyClient, brands: list[dict], dataset_name: str = "dtc-brands") -> str:
    """
    Push results to a named Apify dataset for storage / downstream use.
    Returns the dataset ID.
    """
    try:
        dataset = client.datasets().get_or_create(name=dataset_name)
        dataset_id = dataset["id"]
        client.dataset(dataset_id).push_items(brands)
        log.info(f"Pushed {len(brands)} brands → Apify dataset '{dataset_name}' (id={dataset_id})")
        return dataset_id
    except Exception as e:
        log.error(f"Failed to push to Apify dataset: {e}")
        return ""


# ─────────────────────────────────────────
#  STEP 5: SUMMARY
# ─────────────────────────────────────────
def print_summary(brands: list[dict]) -> None:
    from collections import Counter

    verticals   = Counter(b["vertical"] for b in brands)
    confidences = Counter(b["confidence"] for b in brands)
    sources     = Counter(b["source"] for b in brands)

    max_count = max(verticals.values(), default=1)

    print("\n" + "="*55)
    print("  VERTICAL BREAKDOWN")
    print("="*55)
    for v, count in verticals.most_common():
        bar = "█" * (count * 30 // max_count)
        print(f"  {v:<18} {bar} {count}")

    print("\n" + "="*55)
    print("  CONFIDENCE")
    print("="*55)
    for c, count in confidences.most_common():
        print(f"  {c:<12} {count}")

    print("\n" + "="*55)
    print("  CLASSIFICATION SOURCE")
    print("="*55)
    for s, count in sources.most_common():
        print(f"  {s:<25} {count}")
    print("="*55 + "\n")


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
def main() -> None:
    log.info("DTC Brand Scraper starting (Apify-only mode)...")

    # ApifyClient automatically picks up APIFY_API_TOKEN from the environment
    client = ApifyClient()

    # 1. Scrape ads
    ads = scrape_meta_ads(client, SEARCH_TERMS, ADS_LIMIT_PER_TERM)
    if not ads:
        log.error("No ads fetched. Check your Apify token.")
        return

    # 2. Extract unique brands
    brands = extract_brands(ads)
    if not brands:
        log.error("No websites extracted from ads.")
        return

    brands = brands[:MAX_SITES_TO_CLASSIFY]

    # 3a. Fast keyword pass (no API call)
    needs_crawl = classify_pass1(brands)

    # 3b. Crawl unclear brands via Apify
    website_texts = crawl_homepages(client, needs_crawl)

    # 3c. Re-classify with homepage text
    classify_pass2(needs_crawl, website_texts)

    # 4. Save locally
    save_csv(brands, OUTPUT_CSV)

    # 5. Push to Apify dataset (optional — comment out if not needed)
    push_to_apify_dataset(client, brands)

    # 6. Summary
    print_summary(brands)

    log.info(f"Done. Output: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
