import os
import json
import time
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# =========================================================
# ===================== CONFIG ============================
# =========================================================

MODEL = os.getenv("MODEL", "gpt-5.1")

# Requerido por tu spec:
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))                 # 1000 por batch
MAX_CONCURRENT_BATCHES = int(os.getenv("MAX_CONCURRENT_BATCHES", "5"))  # 5 en paralelo

POLL_EVERY_SECONDS = int(os.getenv("POLL_EVERY_SECONDS", "10"))
POLL_UNTIL_DONE = os.getenv("POLL_UNTIL_DONE", "true").lower() == "true"

EXECUTION_DATE = (os.getenv("EXECUTION_DATE") or "").strip()
if not EXECUTION_DATE:
    raise ValueError("Falta EXECUTION_DATE (env var). Ej: 2026-02-13")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("Falta OPENAI_API_KEY (env var).")

DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()
SNAPSHOT_DIR = DATA_DIR / "snapshots" / EXECUTION_DATE
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_JSON = SNAPSHOT_DIR / "product_details_normalized.json"
OUTPUT_JSON = SNAPSHOT_DIR / "llm_interest_results.json"

# Workdir para inputs/outputs batch (queda dentro del snapshot)
WORKDIR = SNAPSHOT_DIR / "interest_batchs"
WORKDIR.mkdir(parents=True, exist_ok=True)

PROMPT_TEMPLATE = """You are evaluating products for potential resale.

Decide if this product is suitable.

Rules (must all be satisfied):
1) It must NOT be a medical product and must NOT require any medical certification, prescription, or be intended to diagnose, treat, cure, or prevent any disease.
2) It must NOT be a chemical product of any kind.
   - Exception: air fresheners are allowed.

Return ONLY this strict JSON object (no extra keys, no commentary):

{{
  "pass": true/false,
  "reason": "short reason"
}}

Product:
- title: "{title}"
- description: "{description}"
- about_product: {about_product}
"""


# =========================================================
# ===================== IO HELPERS ========================
# =========================================================

def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def chunk_list(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i:i+size] for i in range(0, len(items), size)]


# =========================================================
# ===================== PROMPT ============================
# =========================================================

def build_prompt(p: Dict[str, Any]) -> str:
    about = p.get("about_product")
    if about is None:
        about_json = "null"
    elif isinstance(about, (dict, list)):
        about_json = json.dumps(about, ensure_ascii=False)
    else:
        about_json = json.dumps(str(about), ensure_ascii=False)

    return PROMPT_TEMPLATE.format(
        title=(p.get("title") or "").replace("\n", " ").strip(),
        description=(p.get("description") or "").replace("\n", " ").strip(),
        about_product=about_json
    )


# =========================================================
# ===================== BATCH HELPERS =====================
# =========================================================

def write_jsonl_requests(batch_products: List[Dict[str, Any]], jsonl_path: Path) -> None:
    with jsonl_path.open("w", encoding="utf-8") as f:
        for p in batch_products:
            asin = p.get("asin")
            if not isinstance(asin, str) or not asin.strip():
                continue

            prompt = build_prompt(p)
            line = {
                "custom_id": f"asin-{asin.strip()}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": MODEL,
                    "messages": [
                        {"role": "system", "content": "You are a strict JSON-only classifier. Output JSON only."},
                        {"role": "user", "content": prompt},
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.2,
                },
            }
            f.write(json.dumps(line, ensure_ascii=False) + "\n")

def submit_batch(client: OpenAI, jsonl_path: Path, job_name: str) -> Dict[str, Any]:
    batch_input_file = client.files.create(file=jsonl_path.open("rb"), purpose="batch")
    batch = client.batches.create(
        input_file_id=batch_input_file.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"job": job_name},
    )
    return {
        "batch_id": batch.id,
        "input_file_id": batch_input_file.id,
        "jsonl_path": str(jsonl_path),
    }

def poll_batch_until_done(client: OpenAI, batch_id: str) -> Dict[str, Any]:
    while True:
        b = client.batches.retrieve(batch_id)
        status = getattr(b, "status", None) or (b.get("status") if isinstance(b, dict) else None)
        print(f"[poll] batch_id={batch_id} status={status}")

        if status in ("completed", "failed", "cancelled", "expired"):
            return b.model_dump() if hasattr(b, "model_dump") else dict(b)

        time.sleep(POLL_EVERY_SECONDS)

def download_file_text(client: OpenAI, file_id: str) -> str:
    content = client.files.content(file_id)
    if hasattr(content, "read"):
        data = content.read()
        return data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else str(data)
    if isinstance(content, (bytes, bytearray)):
        return content.decode("utf-8")
    return str(content)

def parse_batch_output(jsonl_text: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for line in jsonl_text.splitlines():
        if not line.strip():
            continue

        obj = json.loads(line)
        custom_id = obj.get("custom_id")
        response = obj.get("response", {}) or {}
        status_code = response.get("status_code")
        body = response.get("body", {}) or {}

        raw_content = None
        parsed = None
        try:
            choices = body.get("choices", [])
            if choices:
                raw_content = choices[0]["message"]["content"]
                parsed = json.loads(raw_content)
        except Exception:
            parsed = None

        asin = None
        if isinstance(custom_id, str) and custom_id.startswith("asin-"):
            asin = custom_id.replace("asin-", "", 1)

        out.append({
            "asin": asin,
            "custom_id": custom_id,
            "status_code": status_code,
            "llm_raw": raw_content,
            "llm_parsed": parsed,
        })
    return out


# =========================================================
# ===================== ASYNC RUNNER =======================
# =========================================================

async def run_one_batch(client: OpenAI, batch_products: List[Dict[str, Any]], batch_index: int) -> Dict[str, Any]:
    job_name = f"llm_interest_batch_{batch_index}"
    jsonl_path = WORKDIR / f"interest_batch_{batch_index}.jsonl"
    write_jsonl_requests(batch_products, jsonl_path)

    meta = await asyncio.to_thread(submit_batch, client, jsonl_path, job_name)

    batch_id = meta["batch_id"]
    result: Dict[str, Any] = {
        "batch_index": batch_index,
        "batch_id": batch_id,
        "input_file_id": meta["input_file_id"],
        "jsonl_path": meta["jsonl_path"],
        "requested": len(batch_products),
        "status": None,
        "output_file_id": None,
        "error_file_id": None,
        "results": [],
        "errors_file_raw": None,
    }

    if not POLL_UNTIL_DONE:
        result["status"] = "submitted"
        return result

    info = await asyncio.to_thread(poll_batch_until_done, client, batch_id)
    result["status"] = info.get("status")
    result["output_file_id"] = info.get("output_file_id")
    result["error_file_id"] = info.get("error_file_id")

    if result["output_file_id"]:
        text = await asyncio.to_thread(download_file_text, client, result["output_file_id"])
        result["results"] = parse_batch_output(text)

    if result["error_file_id"]:
        result["errors_file_raw"] = await asyncio.to_thread(download_file_text, client, result["error_file_id"])

    return result

async def run_all_batches(client: OpenAI, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    batches = chunk_list(products, BATCH_SIZE)
    total = len(batches)
    print(f"Total products={len(products)} -> batches={total} (BATCH_SIZE={BATCH_SIZE})")

    sem = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)
    results: List[Dict[str, Any]] = []

    async def guarded_run(i: int, batch_products: List[Dict[str, Any]]):
        async with sem:
            print(f"[submit] batch {i}/{total} size={len(batch_products)}")
            r = await run_one_batch(client, batch_products, i)
            results.append(r)

    tasks = [guarded_run(i + 1, b) for i, b in enumerate(batches)]
    await asyncio.gather(*tasks)

    results.sort(key=lambda x: x.get("batch_index", 0))
    return results


# =========================================================
# ============================== MAIN ======================
# =========================================================

def _load_step02_items() -> List[Dict[str, Any]]:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"No existe input de Step 02: {INPUT_JSON}")

    data = read_json(INPUT_JSON)
    items = data.get("items", [])
    if not isinstance(items, list):
        raise ValueError("El JSON de Step 02 no tiene 'items' como lista.")
    return [x for x in items if isinstance(x, dict)]

def _is_sales_volume_nonempty(p: Dict[str, Any]) -> bool:
    v = p.get("sales_volume_last_month")
    if v is None:
        return False
    if isinstance(v, str):
        return bool(v.strip())
    # si viene numérico u otro tipo, lo consideramos válido
    return True

def _load_existing_results_map() -> Dict[str, Dict[str, Any]]:
    if not OUTPUT_JSON.exists():
        return {}
    try:
        prev = read_json(OUTPUT_JSON)
        if not isinstance(prev, list):
            return {}
        m: Dict[str, Dict[str, Any]] = {}
        for it in prev:
            if not isinstance(it, dict):
                continue
            a = it.get("asin")
            if isinstance(a, str) and a.strip():
                m[a.strip()] = it
        return m
    except Exception:
        return {}

def main():
    items = _load_step02_items()

    # filtro business mínimo: sales_volume_last_month no vacío
    items = [p for p in items if _is_sales_volume_nonempty(p)]

    # resumible: si ya existe output, no re-procesar esos ASINs
    existing = _load_existing_results_map()
    pending: List[Dict[str, Any]] = []
    for p in items:
        asin = p.get("asin")
        if isinstance(asin, str) and asin.strip() and asin.strip() not in existing:
            pending.append(p)

    print(f"Execution date: {EXECUTION_DATE}")
    print(f"Input items (after sales_volume filter): {len(items)}")
    print(f"Already done (from output): {len(existing)}")
    print(f"Pending to evaluate: {len(pending)}")
    print(f"BATCH_SIZE={BATCH_SIZE} | MAX_CONCURRENT_BATCHES={MAX_CONCURRENT_BATCHES}")

    if not pending:
        print("No hay productos pendientes. Saliendo.")
        return

    client = OpenAI(api_key=OPENAI_API_KEY)
    batch_results = asyncio.run(run_all_batches(client, pending))

    # Construir mapa asin -> {pass, reason} desde batches
    interest_map: Dict[str, Dict[str, Any]] = {}
    for b in batch_results:
        for r in b.get("results", []) or []:
            asin = r.get("asin")
            parsed = r.get("llm_parsed")
            if not isinstance(asin, str) or not asin.strip():
                continue
            if not isinstance(parsed, dict):
                continue
            # esperamos {"pass": bool, "reason": str}
            pval = parsed.get("pass")
            reason = parsed.get("reason")
            if isinstance(pval, bool):
                interest_map[asin.strip()] = {
                    "interest_pass": pval,
                    "interest_reason": reason if isinstance(reason, str) else None,
                    "interest_model": MODEL,
                }

    # Merge final plano: producto completo + interest fields
    final_map = dict(existing)  # asin -> item completo ya guardado

    for p in items:
        asin = p.get("asin")
        if not isinstance(asin, str) or not asin.strip():
            continue
        a = asin.strip()

        merged_item = dict(p)  # trae todo lo del Step 02 (incluye category + product_url)
        extra = interest_map.get(a)
        if extra:
            merged_item.update(extra)
        else:
            # si no llegó respuesta parseable, lo dejamos marcado
            merged_item.setdefault("interest_pass", None)
            merged_item.setdefault("interest_reason", None)
            merged_item.setdefault("interest_model", MODEL)

        final_map[a] = merged_item

    # output plano (lista)
    out_list = list(final_map.values())
    write_json(OUTPUT_JSON, out_list)

    print(f"\nSaved: {OUTPUT_JSON}")
    print(f"Total items in output: {len(out_list)}")
    print("Done.")

if __name__ == "__main__":
    main()
