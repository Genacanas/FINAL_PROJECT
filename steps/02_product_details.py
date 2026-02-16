import os
import json
import time
import random
import re
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv

load_dotenv()

# =========================
# CONFIG
# =========================
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "real-time-amazon-data.p.rapidapi.com")
BASE_URL = f"https://{RAPIDAPI_HOST}"

COUNTRY = os.getenv("COUNTRY", "DE")
LANGUAGE = os.getenv("LANGUAGE", "en_US")

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "1.8"))

# Límite del usuario: 20 requests por segundo
RPS_LIMIT = float(os.getenv("RPS_LIMIT", "20"))

# Concurrencia (cuántos ASINs en vuelo a la vez)
CONCURRENCY = int(os.getenv("CONCURRENCY", "30"))

def get_execution_date() -> str:
    env_date = os.getenv("EXECUTION_DATE")
    if env_date and env_date.strip():
        return env_date.strip()
    return datetime.now(timezone.utc).date().isoformat()

EXECUTION_DATE = get_execution_date()

# =========================
# PATHS
# =========================
BASE_DIR = Path(__file__).resolve().parent.parent
SNAPSHOT_DIR = BASE_DIR / "data" / "snapshots" / EXECUTION_DATE
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

BEST_SELLERS_NORMALIZED_PATH = SNAPSHOT_DIR / "best_sellers_normalized.json"
NORMALIZED_OUTPUT_PATH = SNAPSHOT_DIR / "product_details_normalized.json"

# =========================
# IO HELPERS
# =========================
def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def load_best_sellers_items(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"No existe {path}. Ejecutá 01_best_sellers primero.")

    payload = read_json(path)
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("best_sellers_normalized.json: 'items' no es una lista.")

    return [x for x in items if isinstance(x, dict)]

def load_asins_from_best_sellers_items(items: List[Dict[str, Any]]) -> List[str]:
    asins: List[str] = []
    for it in items:
        asin = it.get("asin")
        if isinstance(asin, str) and asin.strip():
            asins.append(asin.strip())

    # dedupe preservando orden
    seen = set()
    out: List[str] = []
    for a in asins:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out

def build_asin_category_map(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Optional[str]]]:
    m: Dict[str, Dict[str, Optional[str]]] = {}
    for it in items:
        asin = it.get("asin")
        if not isinstance(asin, str) or not asin.strip():
            continue
        a = asin.strip()
        if a not in m:
            m[a] = {
                "main_category": it.get("main_category") if isinstance(it.get("main_category"), str) else None,
                "subcategory_name": it.get("subcategory_name") if isinstance(it.get("subcategory_name"), str) else None,
                "subcategory_id": it.get("subcategory_id") if isinstance(it.get("subcategory_id"), str) else None,
                "product_url": it.get("product_url") if isinstance(it.get("product_url"), str) else None,  # 👈 nuevo
            }
    return m



# =========================
# NORMALIZATION
# =========================
BRAND_RE = re.compile(r"^\s*Brand:\s*(.+?)\s*$", re.IGNORECASE)

def extract_brand(product_byline: Optional[str]) -> Optional[str]:
    if not product_byline or not isinstance(product_byline, str):
        return None
    m = BRAND_RE.match(product_byline)
    if m:
        return m.group(1).strip() or None
    return product_byline.strip() or None

def normalize_details(api_json: Dict[str, Any], asin_category_map: Dict[str, Dict[str, Optional[str]]]) -> Dict[str, Any]:
    data = api_json.get("data", {})
    if not isinstance(data, dict):
        data = {}

    product_info = data.get("product_information", {})
    if not isinstance(product_info, dict):
        product_info = {}

    asin = data.get("asin") or product_info.get("ASIN")

    images: List[str] = []
    if isinstance(data.get("product_photo"), str):
        images.append(data["product_photo"])
    extra_photos = data.get("product_photos") or data.get("images") or data.get("photos")
    if isinstance(extra_photos, list):
        for p in extra_photos:
            if isinstance(p, str):
                images.append(p)

    seen = set()
    images = [x for x in images if not (x in seen or seen.add(x))]

    about = data.get("about_product")
    if isinstance(about, list):
        about = [x for x in about if isinstance(x, str)]
    else:
        about = []

    manufacturer = data.get("manufacturer") or product_info.get("Manufacturer")

    cat = asin_category_map.get(str(asin).strip(), {}) if asin else {}

    return {
        "asin": asin,
        # ✅ NUEVO: metadata de categoría/subcategoría desde Step 01
        "subcategory_id": cat.get("subcategory_id"),
        "main_category": cat.get("main_category"),
        "subcategory_name": cat.get("subcategory_name"),
        "title": data.get("product_title"),
        "price": data.get("product_price"),
        "currency": data.get("currency"),
        "brand_name": extract_brand(data.get("product_byline")),
        "reviews_amount": data.get("product_num_ratings"),
        "star_rating": data.get("product_star_rating"),
        "sales_volume_last_month": data.get("sales_volume"),
        "dimensions": product_info.get("Product Dimensions") or data.get("dimensions"),
        "weight": product_info.get("Item Weight") or data.get("weight"),
        "manufacturer": manufacturer,
        "description": data.get("product_description"),
        "product_url": cat.get("product_url"),
        "about_product": about,
        "images": images,
    }

# =========================
# RATE LIMITER (20 RPS)
# =========================
class RateLimiter:
    """
    Token bucket simple: permite ~RPS_LIMIT solicitudes por segundo globalmente.
    """
    def __init__(self, rps: float):
        self.rps = rps
        self.tokens = rps
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last
            self.last = now

            # recargar tokens
            self.tokens = min(self.rps, self.tokens + elapsed * self.rps)

            if self.tokens < 1.0:
                # esperar lo necesario para tener 1 token
                wait_s = (1.0 - self.tokens) / self.rps
                await asyncio.sleep(wait_s)
                # luego de dormir, consumimos 1 token
                self.tokens = 0.0
            else:
                self.tokens -= 1.0

# =========================
# HTTP
# =========================
def build_headers() -> Dict[str, str]:
    if not RAPIDAPI_KEY:
        raise ValueError("Falta RAPIDAPI_KEY en .env")

    return {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
        "accept": "application/json",
        "user-agent": "Mozilla/5.0",
    }

async def fetch_product_details(
    client: httpx.AsyncClient,
    limiter: RateLimiter,
    asin: str
) -> Tuple[int, Dict[str, Any]]:
    url = f"{BASE_URL}/product-details"
    params = {"asin": asin, "country": COUNTRY, "language": LANGUAGE}

    last_status = -1
    last_json: Dict[str, Any] = {}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # respetar 20 rps global
            await limiter.acquire()

            r = await client.get(url, params=params)
            last_status = r.status_code

            try:
                last_json = r.json()
            except Exception:
                last_json = {"_raw_text": r.text}

            if last_status == 200:
                return last_status, last_json

            if last_status == 429:
                sleep_s = (BACKOFF_BASE ** attempt) * 4 + random.uniform(0, 1.0)
                await asyncio.sleep(sleep_s)
                continue

            if last_status in (500, 502, 503, 504):
                sleep_s = (BACKOFF_BASE ** attempt) + random.uniform(0, 0.6)
                await asyncio.sleep(sleep_s)
                continue

            return last_status, last_json

        except (httpx.RequestError, httpx.TimeoutException) as e:
            last_status = -1
            last_json = {"error": str(e)}
            sleep_s = (BACKOFF_BASE ** attempt) + random.uniform(0, 0.6)
            await asyncio.sleep(sleep_s)

    return last_status, last_json

# =========================
# MAIN ASYNC
# =========================
async def run_async(asins: List[str], asin_category_map: Dict[str, Dict[str, Optional[str]]]) -> List[Dict[str, Any]]:
    headers = build_headers()
    limiter = RateLimiter(RPS_LIMIT)
    sem = asyncio.Semaphore(CONCURRENCY)

    timeout = httpx.Timeout(REQUEST_TIMEOUT)
    limits = httpx.Limits(max_keepalive_connections=CONCURRENCY, max_connections=CONCURRENCY)

    results: List[Dict[str, Any]] = []
    done = 0
    total = len(asins)

    async with httpx.AsyncClient(headers=headers, timeout=timeout, limits=limits, http2=True) as client:
        async def worker(asin: str) -> Optional[Dict[str, Any]]:
            nonlocal done
            async with sem:
                status, api_json = await fetch_product_details(client, limiter, asin)

            done += 1
            if done % 25 == 0 or done == total:
                print(f"[{done}/{total}] progreso...")

            if status == 200 and isinstance(api_json, dict) and api_json.get("status") == "OK":
                return normalize_details(api_json, asin_category_map)

            api_status = api_json.get("status") if isinstance(api_json, dict) else None
            print(f"FAIL asin={asin} status_code={status} api_status={api_status}")
            return None

        tasks = [worker(a) for a in asins]
        out = await asyncio.gather(*tasks)

    for x in out:
        if isinstance(x, dict):
            results.append(x)

    return results

def main():
    best_items = load_best_sellers_items(BEST_SELLERS_NORMALIZED_PATH)
    asin_category_map = build_asin_category_map(best_items)
    asins = load_asins_from_best_sellers_items(best_items)

    print(f"Execution date: {EXECUTION_DATE}")
    print(f"ASINs total (dedupe): {len(asins)}")
    print(f"CONCURRENCY={CONCURRENCY} | RPS_LIMIT={RPS_LIMIT} | MAX_RETRIES={MAX_RETRIES}")

    normalized_batch = asyncio.run(run_async(asins, asin_category_map))

    # Merge normalized (resumible)
    existing_norm_items: List[Dict[str, Any]] = []
    if NORMALIZED_OUTPUT_PATH.exists():
        try:
            prev = read_json(NORMALIZED_OUTPUT_PATH)
            prev_items = prev.get("items", [])
            if isinstance(prev_items, list):
                existing_norm_items = [x for x in prev_items if isinstance(x, dict)]
        except Exception:
            existing_norm_items = []

    merged: Dict[str, Dict[str, Any]] = {}
    for it in existing_norm_items:
        a = it.get("asin")
        if isinstance(a, str) and a.strip():
            merged[a.strip()] = it
    for it in normalized_batch:
        a = it.get("asin")
        if isinstance(a, str) and a.strip():
            merged[a.strip()] = it

    out_norm = {
        "execution_date": EXECUTION_DATE,
        "country": COUNTRY,
        "language": LANGUAGE,
        "total_items": len(merged),
        "items": list(merged.values()),
    }
    write_json(NORMALIZED_OUTPUT_PATH, out_norm)

    print("\nDone.")
    print(f"NORM -> {NORMALIZED_OUTPUT_PATH}")
    print(f"Normalized items total: {len(merged)}")

if __name__ == "__main__":
    main()
