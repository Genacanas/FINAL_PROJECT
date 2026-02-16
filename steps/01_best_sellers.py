import os
import json
import time
import random
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from dotenv import load_dotenv  # en EC2 lo podemos hacer opcional después

# =========================
# CONFIG
# =========================
load_dotenv()

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "real-time-amazon-data.p.rapidapi.com")

COUNTRY = os.getenv("COUNTRY", "DE")
LANGUAGE = os.getenv("LANGUAGE", "en_US")
TYPE = "BEST_SELLERS"
TARGET_PER_CATEGORY = int(os.getenv("TARGET_PER_CATEGORY", "100"))

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
SLEEP_MIN = float(os.getenv("SLEEP_MIN", "1.0"))
SLEEP_MAX = float(os.getenv("SLEEP_MAX", "2.5"))

# =========================
# PATH SETUP
# =========================
BASE_DIR = Path(__file__).resolve().parent.parent  # FINAL_PROJECT/

def get_execution_date() -> str:
    env = os.getenv("EXECUTION_DATE")
    return env.strip() if env and env.strip() else datetime.now(timezone.utc).date().isoformat()

TODAY = get_execution_date()

SUBCATS_FILE = BASE_DIR / "subcategorias_nivel_2.json"

SNAPSHOT_DIR = BASE_DIR / "data" / "snapshots" / TODAY
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

RAW_OUTPUT_PATH = SNAPSHOT_DIR / "best_sellers_raw.json"
NORMALIZED_OUTPUT_PATH = SNAPSHOT_DIR / "best_sellers_normalized.json"

# =========================
# HELPERS
# =========================

def load_subcategories(path: Path) -> Dict[str, List[Dict[str, str]]]:
    """
    Espera:
    {
      "Automotive": [
        { "Car Parts": "automotive/79923031" },
        ...
      ],
      ...
    }
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError(
            "subcategorias_nivel_2.json debe ser un dict: {MainCategory: [ {SubcatName: SubcatId}, ... ]}"
        )

    # Validación ligera
    for main_cat, subcats in data.items():
        if not isinstance(main_cat, str) or not isinstance(subcats, list):
            raise ValueError("Formato inválido: cada main category debe mapear a una lista.")
        for entry in subcats:
            if not isinstance(entry, dict) or len(entry) != 1:
                raise ValueError(
                    "Formato inválido: cada subcategoría debe ser un objeto con un solo par {name: id}."
                )
            (subcat_name, subcat_id), = entry.items()
            if not isinstance(subcat_name, str) or not isinstance(subcat_id, str):
                raise ValueError("Formato inválido: subcat_name y subcat_id deben ser strings.")
            if not subcat_id.strip():
                # Permitimos ids vacíos si todavía los estás completando, pero avisamos.
                pass

    return data


def extract_best_sellers_list(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalmente:
      { status, request_id, data: { best_sellers: [ ... ] } }
    Defensivo por si cambia.
    """
    data = resp.get("data")
    if isinstance(data, dict):
        bs = data.get("best_sellers")
        if isinstance(bs, list):
            return [x for x in bs if isinstance(x, dict)]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def normalize_item(
    item: Dict[str, Any],
    main_category: str,
    subcategory_name: str,
    subcategory_id: str,
) -> Dict[str, Any]:
    return {
        "main_category": main_category,
        "subcategory_name": subcategory_name,
        "subcategory_id": subcategory_id,
        "asin": item.get("asin"),
        "rank": item.get("rank"),
        "title": item.get("product_title") or item.get("title") or item.get("name"),
        "price": item.get("product_price") or item.get("price"),
        "rating": item.get("product_star_rating") or item.get("rating"),
        "reviews_count": item.get("product_num_ratings") or item.get("reviews_count"),
        "product_url": item.get("product_url") or item.get("url"),
        "product_photo": item.get("product_photo") or item.get("image") or item.get("image_url"),
        "raw": item,
    }


def fetch_best_sellers_for_category(session: requests.Session, subcategory_id: str, page: int) -> requests.Response:
    url = f"https://{RAPIDAPI_HOST}/best-sellers"
    params = {
        "category": subcategory_id,  # ID tipo automotive/79923031
        "type": TYPE,
        "page": page,
        "country": COUNTRY,
        "language": LANGUAGE,
    }
    return session.get(url, params=params, timeout=REQUEST_TIMEOUT)


# =========================
# MAIN
# =========================
def main():
    if not RAPIDAPI_KEY:
        raise ValueError("Falta RAPIDAPI_KEY en tu .env")

    if not SUBCATS_FILE.exists():
        raise FileNotFoundError(f"No existe {SUBCATS_FILE}")

    session = requests.Session()
    session.headers.update({
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST,
        "accept": "application/json",
        "user-agent": "Mozilla/5.0",
    })

    subcats_map = load_subcategories(SUBCATS_FILE)
    if not subcats_map:
        print("No hay subcategorías para procesar.")
        return

    # Total subcategorías para un progreso real
    total_subcats = sum(len(v) for v in subcats_map.values())

    all_raw: List[Dict[str, Any]] = []
    all_normalized: List[Dict[str, Any]] = []

    print(f"Loaded {total_subcats} subcategories from {SUBCATS_FILE.name}")
    print("Fetching best sellers for all subcategories...\n")

    processed = 0
    stop_all = False

    for main_category, subcats in subcats_map.items():
        if stop_all:
            break

        for entry in subcats:
            processed += 1
            subcategory_name, subcategory_id = next(iter(entry.items()))

            if not subcategory_id or not subcategory_id.strip():
                print(f"[{processed}/{total_subcats}] {main_category} > {subcategory_name} -> SKIP (subcategory_id vacío)")
                continue

            print(f"[{processed}/{total_subcats}] {main_category} > {subcategory_name} ({subcategory_id})")

            category_items: List[Dict[str, Any]] = []

            for page in range(1, 50):  # techo alto, cortamos por target
                resp = fetch_best_sellers_for_category(session, subcategory_id, page)
                status = resp.status_code

                # Guardamos raw por request (útil para auditoría)
                raw_payload: Dict[str, Any]
                try:
                    raw_payload = resp.json()
                except Exception:
                    raw_payload = {"error": "invalid_json", "text": resp.text}

                all_raw.append({
                    "main_category": main_category,
                    "subcategory_name": subcategory_name,
                    "subcategory_id": subcategory_id,
                    "page": page,
                    "status_code": status,
                    "payload": raw_payload,
                })

                if status == 429:
                    print("  -> 429 Too Many Requests. Cortando ejecución (rate limit).")
                    stop_all = True
                    break

                if status >= 400:
                    print(f"  -> HTTP {status}. Saltando esta subcategoría.")
                    break

                items = extract_best_sellers_list(raw_payload)

                for item in items:
                    category_items.append(
                        normalize_item(
                            item,
                            main_category=main_category,
                            subcategory_name=subcategory_name,
                            subcategory_id=subcategory_id,
                        )
                    )

                print(f"  -> Page {page}: {len(items)} items (acc={len(category_items)})")

                if len(category_items) >= TARGET_PER_CATEGORY:
                    category_items = category_items[:TARGET_PER_CATEGORY]
                    break

                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

            all_normalized.extend(category_items)

    # Guardar RAW (todo junto)
    RAW_OUTPUT_PATH.write_text(
        json.dumps(
            {
                "fetched_at": TODAY,
                "country": COUNTRY,
                "language": LANGUAGE,
                "total_main_categories": len(subcats_map),
                "total_subcategories": total_subcats,
                "requests": all_raw,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    # Guardar NORMALIZED (todo junto)
    NORMALIZED_OUTPUT_PATH.write_text(
        json.dumps(
            {
                "fetched_at": TODAY,
                "country": COUNTRY,
                "language": LANGUAGE,
                "total_items": len(all_normalized),
                "items": all_normalized,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    print("\nDone.")
    print(f"Raw JSON saved to: {RAW_OUTPUT_PATH}")
    print(f"Normalized JSON saved to: {NORMALIZED_OUTPUT_PATH}")
    print(f"Total normalized items: {len(all_normalized)}")


if __name__ == "__main__":
    main()
