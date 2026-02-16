import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values, Json
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")

def get_conn():
    if not DB_URL:
        raise ValueError("Falta DB_URL en .env")

    # Supabase: casi siempre SSL requerido
    # Si tu DB_URL ya trae ?sslmode=require, no pasa nada.
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    return conn


UPSERT_SQL = """
INSERT INTO products_snapshot (
    execution_date,
    asin,
    title,
    price,
    currency,
    brand_name,
    reviews_amount,
    star_rating,
    sales_volume_last_month,
    dimensions,
    weight,
    manufacturer,
    description,
    about_product,
    images,
    product_url,
    main_category,
    subcategory_name,
    subcategory_id,

    interest_pass,
    interest_reason,
    interest_model,

    brand_pass,
    brand_confidence,
    brand_reason,
    brand_model
)
VALUES %s
ON CONFLICT (execution_date, asin)
DO UPDATE SET
    title = EXCLUDED.title,
    price = EXCLUDED.price,
    currency = EXCLUDED.currency,
    brand_name = EXCLUDED.brand_name,
    reviews_amount = EXCLUDED.reviews_amount,
    star_rating = EXCLUDED.star_rating,
    sales_volume_last_month = EXCLUDED.sales_volume_last_month,
    dimensions = EXCLUDED.dimensions,
    weight = EXCLUDED.weight,
    manufacturer = EXCLUDED.manufacturer,
    description = EXCLUDED.description,
    about_product = EXCLUDED.about_product,
    images = EXCLUDED.images,
    product_url = EXCLUDED.product_url,
    main_category = EXCLUDED.main_category,
    subcategory_name = EXCLUDED.subcategory_name,
    subcategory_id = EXCLUDED.subcategory_id,

    interest_pass = EXCLUDED.interest_pass,
    interest_reason = EXCLUDED.interest_reason,
    interest_model = EXCLUDED.interest_model,

    brand_pass = EXCLUDED.brand_pass,
    brand_confidence = EXCLUDED.brand_confidence,
    brand_reason = EXCLUDED.brand_reason,
    brand_model = EXCLUDED.brand_model,

    updated_at = NOW()
;
"""


def _row_from_product(execution_date: str, p: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        execution_date,
        p.get("asin"),
        p.get("title"),
        p.get("price"),
        p.get("currency"),
        p.get("brand_name"),
        p.get("reviews_amount"),
        p.get("star_rating"),
        p.get("sales_volume_last_month"),
        p.get("dimensions"),
        p.get("weight"),
        p.get("manufacturer"),
        p.get("description"),
        Json(p.get("about_product") or []),
        Json(p.get("images") or []),
        p.get("product_url"),
        p.get("main_category"),
        p.get("subcategory_name"),
        p.get("subcategory_id"),

        # Step 03
        p.get("interest_pass"),
        p.get("interest_reason"),
        p.get("interest_model"),

        # Step 04
        p.get("brand_pass"),
        p.get("brand_confidence"),
        p.get("brand_reason"),
        p.get("brand_model"),
    )


def upsert_products_snapshot(
    conn,
    execution_date: str,
    products: List[Dict[str, Any]],
    chunk_size: int = 300,
) -> int:
    """
    Upsert masivo por chunks. No pisa interest_passed / brand_passed.
    Devuelve cuántos registros intentó upsertear.
    """
    if not products:
        return 0

    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(products), chunk_size):
            chunk = products[i:i+chunk_size]
            rows = [_row_from_product(execution_date, p) for p in chunk]

            execute_values(
                cur,
                UPSERT_SQL,
                rows,
                page_size=len(rows)
            )
            total += len(rows)

    conn.commit()
    return total
