import os
import json
import time
import asyncio
from pathlib import Path
from typing import Any, Dict, List

from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# =========================================================
# ===================== CONFIG ============================
# =========================================================

MODEL = os.getenv("MODEL", "gpt-5.1")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
MAX_CONCURRENT_BATCHES = int(os.getenv("MAX_CONCURRENT_BATCHES", "5"))

POLL_EVERY_SECONDS = int(os.getenv("POLL_EVERY_SECONDS", "10"))
POLL_UNTIL_DONE = os.getenv("POLL_UNTIL_DONE", "true").lower() == "true"

EXECUTION_DATE = (os.getenv("EXECUTION_DATE") or "").strip()
if not EXECUTION_DATE:
    raise ValueError("Falta EXECUTION_DATE")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("Falta OPENAI_API_KEY")

DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()
SNAPSHOT_DIR = DATA_DIR / "snapshots" / EXECUTION_DATE

INPUT_JSON = SNAPSHOT_DIR / "llm_interest_results.json"
OUTPUT_JSON = SNAPSHOT_DIR / "llm_brand_results.json"

WORKDIR = SNAPSHOT_DIR / "brand_batchs"
WORKDIR.mkdir(parents=True, exist_ok=True)

PROMPT_TEMPLATE = """You are evaluating whether a product's brand is suitable for resale sourcing from AliExpress.

Return TRUE only if the brand is:
- Small
- Unknown
- Generic
- Likely just a reseller of mass-produced products

Return FALSE if the brand is:
- Large
- Recognizable
- Established
- Known for original products

Return ONLY this strict JSON:
{{
  "pass": true/false,
  "confidence": 0.0-1.0,
  "reason": "short explanation"
}}

Product:
- brand_name: "{brand_name}"
- title: "{title}"
"""

# =========================================================
# ===================== HELPERS ===========================
# =========================================================

def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def chunk_list(items: List[Any], size: int):
    return [items[i:i+size] for i in range(0, len(items), size)]

def build_prompt(p: Dict[str, Any]) -> str:
    return PROMPT_TEMPLATE.format(
        brand_name=(p.get("brand_name") or "").replace("\n", " ").strip(),
        title=(p.get("title") or "").replace("\n", " ").strip(),
    )

def write_jsonl_requests(batch_products, jsonl_path):
    with jsonl_path.open("w", encoding="utf-8") as f:
        for p in batch_products:
            asin = p["asin"]
            line = {
                "custom_id": f"asin-{asin}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": MODEL,
                    "messages": [
                        {"role": "system", "content": "JSON only."},
                        {"role": "user", "content": build_prompt(p)},
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.2,
                },
            }
            f.write(json.dumps(line) + "\n")

def submit_batch(client, jsonl_path, job_name):
    batch_input_file = client.files.create(file=jsonl_path.open("rb"), purpose="batch")
    batch = client.batches.create(
        input_file_id=batch_input_file.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"job": job_name},
    )
    return batch.id

def poll_until_done(client, batch_id):
    while True:
        b = client.batches.retrieve(batch_id)
        print(f"[poll] batch_id={batch_id} status={b.status}")
        if b.status in ("completed", "failed", "cancelled", "expired"):
            return b
        time.sleep(POLL_EVERY_SECONDS)

def parse_batch_output(text: str):
    out = {}
    for line in text.splitlines():
        obj = json.loads(line)
        custom_id = obj.get("custom_id")
        asin = custom_id.replace("asin-", "") if custom_id else None

        parsed = None
        try:
            parsed = json.loads(obj["response"]["body"]["choices"][0]["message"]["content"])
        except Exception:
            pass

        if asin:
            out[asin] = parsed

    return out

# =========================================================
# ===================== ASYNC =============================
# =========================================================

async def run_all_batches(client, products):
    batches = chunk_list(products, BATCH_SIZE)
    sem = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)
    results = {}

    async def run_one(i, batch_products):
        async with sem:
            jsonl_path = WORKDIR / f"brand_batch_{i}.jsonl"
            write_jsonl_requests(batch_products, jsonl_path)

            batch_id = await asyncio.to_thread(submit_batch, client, jsonl_path, f"brand_{i}")

            if not POLL_UNTIL_DONE:
                return

            info = await asyncio.to_thread(poll_until_done, client, batch_id)

            if info.output_file_id:
                text = await asyncio.to_thread(
                    lambda: client.files.content(info.output_file_id).read().decode("utf-8")
                )
                results.update(parse_batch_output(text))

    tasks = [run_one(i+1, b) for i, b in enumerate(batches)]
    await asyncio.gather(*tasks)

    return results

# =========================================================
# ===================== MAIN ==============================
# =========================================================

def main():

    if not INPUT_JSON.exists():
        raise FileNotFoundError("No existe llm_interest_results.json")

    items = read_json(INPUT_JSON)

    # Solo interest_pass == true
    items = [p for p in items if p.get("interest_pass") is True]

    print(f"Productos aprobados por interest: {len(items)}")

    if not items:
        print("No hay productos para evaluar brand.")
        return

    client = OpenAI(api_key=OPENAI_API_KEY)
    brand_results = asyncio.run(run_all_batches(client, items))

    # Merge plano
    final = []
    for p in items:
        asin = p.get("asin")
        brand = brand_results.get(asin)

        merged = dict(p)

        if isinstance(brand, dict):
            merged.update({
                "brand_pass": brand.get("pass"),
                "brand_confidence": brand.get("confidence"),
                "brand_reason": brand.get("reason"),
                "brand_model": MODEL
            })
        else:
            merged.update({
                "brand_pass": None,
                "brand_confidence": None,
                "brand_reason": None,
                "brand_model": MODEL
            })

        final.append(merged)

    write_json(OUTPUT_JSON, final)

    print(f"\nSaved: {OUTPUT_JSON}")
    print(f"Total items: {len(final)}")

if __name__ == "__main__":
    main()
