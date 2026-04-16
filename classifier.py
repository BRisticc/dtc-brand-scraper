"""
Keyword-based vertical classifier for DTC brands.
Two-pass: domain+ad text first, homepage text second.
"""

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
        "wellness", "nootropic", "magnesium", "zinc", "b12", "ashwagandha",
        "turmeric", "melatonin", "biotin", "creatine", "electrolyte", "gut health",
        "immune", "energy boost", "metabolism", "detox", "greens powder", "superfoods",
    ],
    "fashion": [
        "clothing", "apparel", "dress", "shirt", "jeans", "fashion", "outfit",
        "wear", "style", "shoes", "sneakers", "boots", "jacket", "hoodie",
        "leggings", "swimwear", "activewear", "loungewear", "streetwear",
        "collection", "wardrobe", "tee", "pants", "shorts", "blazer", "coat",
        "accessories", "bags", "purse", "jewelry",
    ],
    "food_beverage": [
        "coffee", "tea", "snack", "food", "drink", "nutrition", "meal", "organic",
        "beverage", "sauce", "spice", "seasoning", "protein bar", "granola",
        "keto", "paleo", "vegan", "gluten-free", "dairy-free", "smoothie",
        "juice", "energy drink", "soda", "kombucha", "chocolate", "candy",
        "cookie", "chip", "popcorn", "jerky", "nut butter", "honey",
    ],
    "fitness": [
        "workout", "gym", "training", "fitness", "exercise", "yoga", "pilates",
        "sport", "running", "cycling", "hiit", "crossfit", "weightlifting",
        "resistance band", "dumbbell", "kettlebell", "foam roller", "home gym",
    ],
    "pet": [
        "dog", "cat", "pet", "paw", "treat", "fur", "animal", "puppy", "kitten",
        "kibble", "leash", "collar", "grooming", "canine", "feline",
    ],
    "home": [
        "home", "decor", "furniture", "candle", "kitchen", "bedroom", "living",
        "bathroom", "organization", "storage", "lamp", "rug", "curtain", "pillow",
        "bedding", "mattress", "sofa", "desk", "plant", "garden", "patio",
        "cleaning", "laundry",
    ],
    "baby_kids": [
        "baby", "kids", "toddler", "child", "infant", "parent", "mom", "dad",
        "newborn", "diaper", "stroller", "nursing", "breastfeed", "pacifier",
        "teether", "toy", "educational",
    ],
    "tech_gadgets": [
        "device", "gadget", "tech", "smart", "wireless", "charger", "earbuds",
        "wearable", "bluetooth", "usb", "cable", "speaker", "headphones",
        "camera", "drone", "smartwatch", "gps", "sensor", "led",
    ],
    "other": [],
}


def classify(text: str) -> tuple[str, int]:
    """
    Score text against all verticals.
    Returns (best_vertical, score). Score 0 → no match.
    """
    lowered = text.lower()
    scores  = {v: 0 for v in VERTICALS}

    for vertical, keywords in VERTICALS.items():
        for kw in keywords:
            if kw in lowered:
                scores[vertical] += 1

    best       = max(scores, key=scores.get)
    best_score = scores[best]
    return best, best_score


def confidence_label(score: int) -> str:
    if score >= 4:
        return "high"
    if score >= 2:
        return "medium"
    if score >= 1:
        return "low"
    return "none"
