import asyncio
import html
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from supabase import create_client, Client as SupabaseClient
from telegram.constants import ParseMode
from telegram.ext import Application, ApplicationBuilder, ContextTypes

# ===================== Config & Env =====================

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY") or ""
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "instamart_products")
POLL_MINUTES = int(os.getenv("POLL_MINUTES", "10"))

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit(
        "Missing env. Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (or ANON)."
    )

CACHE_FILE = Path("notified_cache.json")   # stores last notified % per (product_id:var_id)
PAGE_SIZE = 1000                           # Supabase pagination

# ===================== Helpers =====================

def load_cache() -> Dict[str, int]:
    try:
        if CACHE_FILE.exists():
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def save_cache(cache: Dict[str, int]) -> None:
    try:
        CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")
    except Exception as e:
        print("[cache] write failed:", e)

def parse_percent(s: Any) -> Optional[int]:
    if s is None:
        return None
    import re
    m = re.search(r"(\d+)", str(s))
    return int(m.group(1)) if m else None

def compute_pct(mrp: Any, offer: Any) -> Optional[int]:
    try:
        mrp_f = float(mrp)
        offer_f = float(offer)
        if mrp_f <= 0 or offer_f > mrp_f:
            return None
        return round((mrp_f - offer_f) / mrp_f * 100)
    except Exception:
        return None

def normalize_discount(row: Dict[str, Any]) -> Optional[int]:
    # Prefer explicit "discount" (e.g., "72%") else compute from mrp/offer_price.
    pct = parse_percent(row.get("discount"))
    if pct is not None:
        return pct
    return compute_pct(row.get("mrp"), row.get("offer_price"))

def product_key(row: Dict[str, Any]) -> str:
    pid = row.get("product_id") or row.get("productId") or "?"
    vid = row.get("var_id") or "default"
    return f"{pid}:{vid}"

def fmt_money(v: Any) -> str:
    if v is None or v == "":
        return "-"
    try:
        n = float(v)
        s = f"₹{n:,.2f}"
        if s.endswith(".00"):
            s = s[:-3]
        return s
    except Exception:
        return str(v)

def now_ist_str() -> str:
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")

def format_message(row: Dict[str, Any], pct: int) -> str:
    name = row.get("name") or "(no name)"
    tile = row.get("tile_name") or row.get("category") or row.get("tile_id") or "—"
    mrp = fmt_money(row.get("mrp"))
    offer = fmt_money(row.get("offer_price") or row.get("store_price"))
    pid = row.get("product_id") or row.get("productId") or "—"
    vid = row.get("var_id") or "default"
    sku = row.get("sku") or "—"

    return (
        f"🔥 <b>{pct}% OFF</b>\n"
        f"<b>{html.escape(str(name))}</b>\n"
        f"Tile: <i>{html.escape(str(tile))}</i>\n"
        f"MRP: {mrp} | Offer: {offer}\n"
        f"SKU: {html.escape(str(sku))}\n"
        f"ID: {pid} / {vid}\n"
        f"⏱ {now_ist_str()}"
    )

# ===================== Supabase =====================

def supabase_client() -> SupabaseClient:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# >>> Make this SYNC (not async) <<<
def fetch_all_rows(sb: SupabaseClient) -> List[Dict[str, Any]]:
    # Select * so we don't break if some columns are missing (e.g., tile_id)
    out: List[Dict[str, Any]] = []
    start = 0
    while True:
        end = start + PAGE_SIZE - 1
        resp = sb.table(SUPABASE_TABLE).select("*", count="exact").range(start, end).execute()
        data = resp.data or []
        out.extend(data)
        if len(data) < PAGE_SIZE:
            break
        start += PAGE_SIZE
    return out

# ===================== Job: scan & notify =====================

# >>> Make this a JobQueue-style callback: it receives context, not application <<<
async def scan_and_notify(context: ContextTypes.DEFAULT_TYPE) -> None:
    app: Application = context.application
    print(f"[bot] scanning table '{SUPABASE_TABLE}' for >= 70% …")

    sb = supabase_client()
    # run blocking Supabase client in thread
    rows = await asyncio.to_thread(fetch_all_rows, sb)

    cache = load_cache()
    sent = 0

    for row in rows:
        if row.get("product_id") is None and row.get("productId") is not None:
            row["product_id"] = row["productId"]

        pct = normalize_discount(row)
        if pct is None or pct < 70:
            continue

        key = product_key(row)
        prev = cache.get(key)

        if prev is None or prev < 70 or pct > prev:
            text = format_message(row, pct)
            try:
                await app.bot.send_message(
                    chat_id=int(TELEGRAM_CHAT_ID),
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                cache[key] = pct
                sent += 1
            except Exception as e:
                print("[telegram] send failed:", e)

    save_cache(cache)
    print(f"[bot] scan done. Alerts sent: {sent}")

# ===================== Startup & Main =====================

async def on_startup(app: Application) -> None:
    await app.bot.send_message(
        chat_id=int(TELEGRAM_CHAT_ID),
        text=f"✅ Instamart discount bot up. Poll every {POLL_MINUTES}m.",
        disable_web_page_preview=True,
    )
    # kick off one scan immediately
    await scan_and_notify(ContextTypes.DEFAULT_TYPE(application=app))

def main() -> None:
    application: Application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # Requires installing the job-queue extra for PTB 21:
    # pip install "python-telegram-bot[job-queue]==21.6"
    application.job_queue.run_repeating(
        scan_and_notify,                 # <-- pass the async function directly
        interval=POLL_MINUTES * 60,
        first=0,
        name="scan_and_notify",
    )

    application.post_init = on_startup

    print("[bot] starting…")
    application.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
