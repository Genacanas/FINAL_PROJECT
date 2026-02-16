import os
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
import re

from db import get_conn, upsert_products_snapshot

BASE_DIR = Path(__file__).resolve().parent
STEPS_DIR = BASE_DIR / "steps"

# =========================================================
# ===================== HELPERS ===========================
# =========================================================

def execution_date() -> str:
    env = os.getenv("EXECUTION_DATE")
    if env and env.strip():
        return env.strip()
    return datetime.now(timezone.utc).date().isoformat()

def run_step(script_name: str, env: dict):
    script_path = STEPS_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"No existe {script_path}")
    subprocess.run([sys.executable, str(script_path)], check=True, env=env)

def clean_price(value):
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        s = re.sub(r"[^\d.,-]", "", s)
        s = s.replace(",", "")
        try:
            return float(s)
        except:
            return None
    return None

# =========================================================
# ========================== MAIN =========================
# =========================================================

def main():

    date = execution_date()
    env = os.environ.copy()
    env["EXECUTION_DATE"] = date

    print(f"\nEXECUTION_DATE={date}")

    # -----------------------------------------------------
    # STEP 01
    # -----------------------------------------------------
    print("\n[1/5] Running 01_best_sellers.py")
    run_step("01_best_sellers.py", env)

    # -----------------------------------------------------
    # STEP 02
    # -----------------------------------------------------
    print("\n[2/5] Running 02_product_details.py")
    run_step("02_product_details.py", env)

    # -----------------------------------------------------
    # STEP 03
    # -----------------------------------------------------
    print("\n[3/5] Running 03_llm_interest.py")
    run_step("03_llm_interest.py", env)

    # -----------------------------------------------------
    # STEP 04
    # -----------------------------------------------------
    print("\n[4/5] Running 04_llm_brand.py")
    run_step("04_llm_brand.py", env)

    # -----------------------------------------------------
    # FINAL DB UPSERT
    # -----------------------------------------------------

    snapshot_dir = BASE_DIR / "data" / "snapshots" / date
    final_json_path = snapshot_dir / "llm_brand_results.json"

    if not final_json_path.exists():
        raise FileNotFoundError(f"No existe {final_json_path}")

    print("\n[5/5] Upserting final dataset into Supabase")

    final_items = json.loads(final_json_path.read_text(encoding="utf-8"))

    if not isinstance(final_items, list):
        raise ValueError("llm_brand_results.json debe ser una lista plana")

    # Limpieza mínima antes de DB
    for it in final_items:
        it["price"] = clean_price(it.get("price"))

    conn = get_conn()
    try:
        upserted = upsert_products_snapshot(
            conn=conn,
            execution_date=date,
            products=final_items,
            chunk_size=300
        )
        print(f"Upserted {upserted} rows into products_snapshot")
    finally:
        conn.close()

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
