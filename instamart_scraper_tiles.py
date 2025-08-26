import json
import os
import re
import time
import hashlib
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import sync_playwright

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from supabase import create_client
except ImportError:
    create_client = None

if TYPE_CHECKING:
    from supabase import Client as SupabaseClient
else:
    SupabaseClient = Any

HEADLESS = True
SLOWMO_MS = 0
REQ_GAP_SEC = 0.8
TILE_GAP_SEC = 4.0
SCROLL_PAUSE_MS = 250

OUT_DIR = os.path.abspath("out_tiles")
os.makedirs(OUT_DIR, exist_ok=True)

PARENTS: List[str] = [
    "https://www.swiggy.com/instamart/category-listing?categoryName=Dairy%2C+Bread+and+Eggs&custom_back=true&filterName=&offset=0&showAgeConsent=false&storeId=788745&taxonomyType=Speciality+taxonomy+1",
]

TABLE_NAME = os.getenv("SUPABASE_TABLE", "instamart_products")

def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")

def qs_val(qs: Dict[str, List[str]], key: str, default: str = "") -> str:
    v = qs.get(key, [default])
    return v[0] if v else default

def decode_plus(s: str) -> str:
    return (s or "").replace("+", " ")

def row_fingerprint(r: Dict[str, Any]) -> str:
    parts = [
        r.get("brand"),
        r.get("discount"),
        r.get("mrp"),
        r.get("name"),
        r.get("offer_price"),
        r.get("sku"),
        r.get("store_price"),
    ]
    s = "||".join("" if p is None else str(p) for p in parts)
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def parse_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    d = payload or {}
    items: List[Dict[str, Any]] = []
    if isinstance(d.get("products"), list):
        items = d["products"]
    elif isinstance(d.get("cards"), list):
        items = d["cards"]
    elif isinstance(d.get("items"), list):
        items = d["items"]
    if not items:
        widgets = d.get("data", {}).get("widgets", []) or d.get("pageWidgets", []) or []
        if isinstance(widgets, list):
            lists = [
                w for w in widgets
                if (w.get("widgetInfo", {}).get("widgetType") or w.get("type")) == "PRODUCT_LIST"
            ]
            for w in lists:
                x = w.get("data")
                if isinstance(x, list):
                    items.extend(x)
                elif isinstance(x, dict):
                    for k in ("products", "cards", "items"):
                        v = x.get(k)
                        if isinstance(v, list):
                            items.extend(v)
    if not items:
        cl = d.get("categoryListing", {})
        if isinstance(cl, dict) and isinstance(cl.get("products"), list):
            items = cl["products"]
        plp = d.get("plp", {})
        if not items and isinstance(plp, dict) and isinstance(plp.get("products"), list):
            items = plp["products"]
    return items or []

def explode_item(x: Dict[str, Any],
                tile_id: Optional[str] = None,
                tile_name: Optional[str] = None,
                category_name: Optional[str] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    tag = x.get("listing_description") or x.get("product_description") or ""
    m = re.search(r"(\d+%)", tag or "")
    discount = m.group(1) if m else None
    product_id = x.get("id") or x.get("product_id") or x.get("itemId") or x.get("info", {}).get("id")
    name = x.get("display_name") or x.get("title") or x.get("name") or x.get("info", {}).get("name")
    brand = x.get("brand") or x.get("info", {}).get("brand")
    variations = x.get("variations")
    if isinstance(variations, list) and variations:
        for v in variations:
            p = v.get("price") or {}
            row = {
                "brand": brand,
                "discount": discount,
                "mrp": p.get("mrp") or x.get("mrp") or x.get("price", {}).get("mrp"),
                "name": name,
                "offer_price": p.get("offer_price") or x.get("offer_price") or x.get("finalPrice"),
                "productId": product_id,
                "sku": v.get("sku") or v.get("code") or v.get("barcode"),
                "store_price": p.get("store_price") or p.get("price") or p.get("mrp") or x.get("store_price"),
                "var_id": v.get("id") or v.get("skuId") or v.get("sku_id") or v.get("variation_id"),
                "tile_id": tile_id,
                "tile_name": tile_name,
                "category": category_name,
            }
            rows.append(row)
    else:
        p = (x.get("variations") or [{}])[0] if x.get("variations") else {}
        price = p.get("price") or x.get("price") or {}
        row = {
            "brand": brand,
            "discount": discount,
            "mrp": price.get("mrp") or x.get("mrp"),
            "name": name,
            "offer_price": price.get("offer_price") or x.get("offer_price") or x.get("finalPrice"),
            "productId": product_id,
            "sku": p.get("sku") or p.get("code") or p.get("barcode"),
            "store_price": price.get("store_price") or price.get("price") or price.get("mrp") or x.get("store_price"),
            "var_id": p.get("id") or p.get("skuId") or p.get("sku_id") or p.get("variation_id"),
            "tile_id": tile_id,
            "tile_name": tile_name,
            "category": category_name,
        }
        rows.append(row)
    return rows

def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[Tuple[Optional[str], Optional[str]]] = set()
    out: List[Dict[str, Any]] = []
    for r in rows:
        key = (r.get("productId"), r.get("var_id"))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def get_has_more(payload: Dict[str, Any]) -> Optional[bool]:
    d = payload.get("data") or payload
    pag = d.get("pagination") or {}
    hm = d.get("hasMore")
    if isinstance(hm, bool):
        return hm
    hm = pag.get("hasMore")
    if isinstance(hm, bool):
        return hm
    return None

def fetch_parent_all(page, category_name: str, store_id: str, primary: str, secondary: str, taxonomy: str, gap: float=0.8) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    offset = 0
    step = 20
    max_retries = 3
    while True:
        for attempt in range(max_retries):
            try:
                params = {
                    "categoryName": category_name,
                    "storeId": store_id,
                    "offset": str(offset),
                    "filterName": "",
                    "primaryStoreId": primary,
                    "taxonomyType": taxonomy,
                }
                if secondary:
                    params["secondaryStoreId"] = secondary
                payload = page.evaluate(
                    """(params) => fetch('/api/instamart/category-listing?' + new URLSearchParams(params), {
                        credentials:'same-origin',
                        headers: {
                            'Accept': 'application/json',
                            'Content-Type': 'application/json'
                        }
                    }).then(r => r.json())""",
                    params,
                )
                items = parse_items(payload)
                print(f"[parent] offset {offset} -> items {len(items)}")
                if not items:
                    return collected
                for it in items:
                    collected.extend(explode_item(it, tile_id="parent", tile_name="Parent", category_name=category_name))
                hm = get_has_more(payload)
                offset += step
                if hm is False:
                    return collected
                time.sleep(gap)
                break
            except Exception as e:
                print(f"[parent] fetch failed at offset {offset}, attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    return collected
                time.sleep(1)
    return collected

def fetch_tile_get_all(page, category_name: str, store_id: str, primary: str, secondary: str, taxonomy: str, tile_id: str, tile_name: str, gap: float=0.6) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    step = 20
    max_retries = 3
    while True:
        for attempt in range(max_retries):
            try:
                params = {
                    "categoryName": category_name,
                    "storeId": store_id,
                    "offset": str(offset),
                    "filterName": "",
                    "primaryStoreId": primary,
                    "taxonomyType": taxonomy,
                }
                if secondary:
                    params["secondaryStoreId"] = secondary
                payload = page.evaluate(
                    """(params) => fetch('/api/instamart/category-listing?' + new URLSearchParams(params), {
                        credentials:'same-origin',
                        headers: {
                            'Accept': 'application/json',
                            'Content-Type': 'application/json'
                        }
                    }).then(r => r.json())""",
                    params,
                )
                got = parse_items(payload)
                print(f"[tile-GET:{tile_name}] offset {offset} -> items {len(got)}")
                if not got:
                    return out
                for it in got:
                    out.extend(explode_item(it, tile_id=tile_id, tile_name=tile_name, category_name=category_name))
                hm = get_has_more(payload)
                offset += step
                if hm is False:
                    return out
                time.sleep(gap)
                break
            except Exception as e:
                print(f"[tile-GET:{tile_name}] fetch failed at offset {offset}, attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    return out
                time.sleep(1)
    return out

def fetch_tile_post_all(page, filter_id: str, category_name: str, store_id: str, primary: str, secondary: str, taxonomy: str, tile_name: str, gap: float=0.6) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    page_no = 0
    limit = 40
    max_retries = 3
    while True:
        for attempt in range(max_retries):
            try:
                params = {
                    "filterId": filter_id,
                    "storeId": store_id,
                    "offset": "0",
                    "primaryStoreId": primary,
                    "type": taxonomy,
                    "pageNo": str(page_no),
                    "limit": str(limit),
                    "filterName": "",
                    "categoryName": category_name,
                }
                if secondary:
                    params["secondaryStoreId"] = secondary
                payload2 = page.evaluate(
                    """(params) => fetch('/api/instamart/category-listing/filter?' + new URLSearchParams(params), {
                        method: 'POST',
                        credentials: 'same-origin',
                        headers: {
                            'Content-Type': 'application/json',
                            'Accept': 'application/json'
                        },
                        body: '{}'
                    }).then(r => r.json())""",
                    params,
                )
                got = parse_items(payload2)
                print(f"[tile-POST:{tile_name}] pageNo {page_no} -> items {len(got)}")
                if not got:
                    return out
                for it in got:
                    out.extend(explode_item(it, tile_id=filter_id, tile_name=tile_name, category_name=category_name))
                hm = get_has_more(payload2)
                page_no += 1
                if hm is False:
                    return out
                time.sleep(gap)
                break
            except Exception as e:
                print(f"[tile-POST:{tile_name}] fetch failed at pageNo {page_no}, attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    return out
                time.sleep(1)
    return out

def init_supabase() -> Optional[SupabaseClient]:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        print("[supabase] Missing SUPABASE_URL or key (SUPABASE_SERVICE_ROLE_KEY / SUPABASE_ANON_KEY). Skipping DB writes.")
        return None
    if create_client is None:
        print("[supabase] 'supabase' package not installed. Run: pip install supabase")
        return None
    try:
        client: SupabaseClient = create_client(url, key)
        print("[supabase] Client initialized.")
        return client
    except Exception as e:
        print(f"[supabase] Failed to init client: {e}")
        return None

def compute_discount_str(mrp: Any, offer: Any, existing: Optional[str]) -> Optional[str]:
    if existing:
        return existing
    try:
        if mrp is not None and offer is not None:
            mrp_val = float(mrp)
            offer_val = float(offer)
            if mrp_val > 0 and offer_val <= mrp_val:
                pct = round((mrp_val - offer_val) / mrp_val * 100)
                return f"{pct}%"
    except (ValueError, TypeError):
        pass
    return existing

def rows_for_db(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        row = {
            "brand": r.get("brand"),
            "mrp": r.get("mrp"),
            "name": r.get("name"),
            "offer_price": r.get("offer_price"),
            "product_id": (str(r.get("productId")) if r.get("productId") is not None else None),
            "sku": r.get("sku"),
            "store_price": r.get("store_price"),
            "var_id": (str(r.get("var_id")) if r.get("var_id") is not None else None),
        }
        row["discount"] = compute_discount_str(row["mrp"], row["offer_price"], r.get("discount"))
        row["data_hash"] = row_fingerprint(row)
        out.append(row)
    return out

def _try_fetch_existing(sb: SupabaseClient, table: str, prod_ids: List[str], var_ids: List[str]) -> Tuple[Dict[Tuple[str, str], Optional[str]], bool]:
    existing: Dict[Tuple[str, str], Optional[str]] = {}
    hash_present = True
    if not prod_ids or not var_ids:
        return existing, hash_present
    try:
        resp = (
            sb.table(table)
              .select("product_id,var_id,data_hash")
              .in_("product_id", prod_ids)
              .in_("var_id", var_ids)
              .execute()
        )
        for row in resp.data or []:
            pid = row.get("product_id")
            vid = row.get("var_id")
            if pid is not None and vid is not None:
                existing[(pid, vid)] = row.get("data_hash")
        return existing, hash_present
    except Exception as e:
        print(f"[supabase] fetch existing with data_hash failed (will retry without hash): {e}")
        try:
            hash_present = False
            resp = (
                sb.table(table)
                  .select("product_id,var_id")
                  .in_("product_id", prod_ids)
                  .in_("var_id", var_ids)
                  .execute()
            )
            for row in resp.data or []:
                pid = row.get("product_id")
                vid = row.get("var_id")
                if pid is not None and vid is not None:
                    existing[(pid, vid)] = None
        except Exception as e2:
            print(f"[supabase] fetch existing keys failed: {e2}")
    return existing, hash_present

def upsert_batches(sb, table, rows, batch_size=400):
    if not sb or not rows:
        return
    total_new = total_changed = total_skipped = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        valid_batch = [r for r in batch if r.get("product_id") is not None and r.get("var_id") is not None]
        if not valid_batch:
            print(f"[supabase] no valid rows in batch {i//batch_size+1}")
            continue
        keys = [(r["product_id"], r["var_id"]) for r in valid_batch]
        prod_ids = [k[0] for k in keys]
        var_ids = [k[1] for k in keys]
        existing: Dict[Tuple[str, str], Optional[str]] = {}
        hash_present = True
        if prod_ids and var_ids:
            existing, hash_present = _try_fetch_existing(sb, table, prod_ids, var_ids)
        delta = []
        for r in valid_batch:
            key = (r["product_id"], r["var_id"])
            if key not in existing:
                delta.append(r)
                total_new += 1
                continue
            ex_hash = existing.get(key)
            if not hash_present:
                delta.append(r)
                total_changed += 1
                continue
            if ex_hash is None or ex_hash != r.get("data_hash"):
                delta.append(r)
                total_changed += 1
            else:
                total_skipped += 1
        if not delta:
            print(f"[supabase] nothing to upsert in batch {i//batch_size+1}")
            continue
        try:
            sb.table(table).upsert(delta, on_conflict="product_id,var_id").execute()
            print(f"[supabase] upserted {len(delta)} rows into {table}.")
        except Exception as e:
            print(f"[supabase] upsert failed for batch {i//batch_size+1}: {e}")
    print(f"[supabase] summary: new={total_new}, changed={total_changed}, skipped={total_skipped}")

def run() -> None:
    sb = init_supabase()
    while True:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=HEADLESS, slow_mo=SLOWMO_MS)
            try:
                ctx = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36"
                    ),
                    viewport={"width": 375, "height": 667}
                )
                page = ctx.new_page()
                for parent in PARENTS:
                    print(f"\n=== PARENT: {parent} ===")
                    try:
                        page.goto(parent, wait_until="networkidle", timeout=45000)
                    except Exception as e:
                        print(f"[fatal] {parent}: goto failed: {e}")
                        continue
                    time.sleep(2)
                    try:
                        page.wait_for_selector('li[data-itemid]', timeout=15000)
                    except PWTimeout:
                        print("[grid] tiles not visible yet, scrolling to trigger render…")
                        try:
                            for _ in range(5):
                                page.evaluate("window.scrollBy(0, window.innerHeight * 0.5)")
                                time.sleep(SCROLL_PAUSE_MS / 1000)
                            page.wait_for_selector('li[data-itemid]', timeout=8000)
                        except Exception as ex:
                            print(f"[warn] Could not find tiles after scrolling: {ex}")
                            continue
                    try:
                        tiles: List[Dict[str, str]] = page.evaluate(
                            """
                            () => {
                                const nodes = Array.from(document.querySelectorAll('li[data-itemid]'));
                                return nodes.map(n => {
                                    const filterId = n.getAttribute('data-itemid') || '';
                                    let labelEl = n.querySelector('[class*="aXZVg"]') || n.querySelector('div span') || n.querySelector('span') || n.querySelector('div');
                                    let name = '';
                                    if (labelEl) {
                                        name = (labelEl.textContent || '').trim();
                                    } else {
                                        name = (n.innerText || '').trim();
                                    }
                                    name = name.split('\\n').filter(line => line.trim()).pop() || '';
                                    return (filterId && name) ? { filterId, name } : null;
                                }).filter(Boolean);
                            }
                            """
                        )
                    except Exception as e:
                        print(f"[warn] Failed to collect tiles: {e}")
                        tiles = []
                    print(f"[tiles] discovered: {len(tiles)}")
                    u = urlparse(parent)
                    q = parse_qs(u.query)
                    category_name = decode_plus(qs_val(q, "categoryName"))
                    store_id = qs_val(q, "storeId")
                    primary = qs_val(q, "primaryStoreId", store_id)
                    secondary = qs_val(q, "secondaryStoreId", "")
                    taxonomy = decode_plus(qs_val(q, "taxonomyType", "Speciality taxonomy 1"))
                    collected: List[Dict[str, Any]] = []
                    try:
                        parent_items = fetch_parent_all(page, category_name, store_id, primary, secondary, taxonomy, gap=REQ_GAP_SEC)
                        collected.extend(parent_items)
                        print(f"[parent] collected {len(parent_items)} items")
                    except Exception as e:
                        print(f"[warn] parent fetch failed: {e}")
                    for idx, t in enumerate(tiles, start=1):
                        print(f"[tile {idx}/{len(tiles)}] {t['name']} ({t['filterId']})")
                        try:
                            loc = page.locator(f'li[data-itemid="{t["filterId"]}"]')
                            loc.scroll_into_view_if_needed(timeout=8000)
                            loc.click(timeout=8000)
                            try:
                                page.wait_for_load_state("networkidle", timeout=10000)
                            except Exception:
                                page.wait_for_selector('div[data-testid="product-card"]', timeout=10000)
                        except Exception as e:
                            print(f"[warn] Failed for tile {t['name']}: {e}")
                            continue
                        u2 = urlparse(page.url)
                        q2 = parse_qs(u2.query)
                        now_category = decode_plus(qs_val(q2, "categoryName", category_name))
                        now_primary = qs_val(q2, "primaryStoreId", primary)
                        now_secondary = qs_val(q2, "secondaryStoreId", secondary)
                        now_taxonomy = decode_plus(qs_val(q2, "taxonomyType", taxonomy))
                        print(f"   -> tile context: category='{now_category}', primary='{now_primary}', secondary='{now_secondary}', taxonomy='{now_taxonomy}'")
                        tile_total: List[Dict[str, Any]] = []
                        try:
                            tile_total.extend(
                                fetch_tile_post_all(
                                    page,
                                    t["filterId"],
                                    now_category,
                                    store_id,
                                    now_primary,
                                    now_secondary,
                                    now_taxonomy,
                                    tile_name=t["name"],
                                    gap=0.6,
                                )
                            )
                        except Exception as e:
                            print(f"[tile-POST] error: {e}")
                        if not tile_total:
                            try:
                                tile_total.extend(
                                    fetch_tile_get_all(
                                        page,
                                        now_category,
                                        store_id,
                                        now_primary,
                                        now_secondary,
                                        now_taxonomy,
                                        tile_id=t["filterId"],
                                        tile_name=t["name"],
                                        gap=0.6,
                                    )
                                )
                            except Exception as e:
                                print(f"[tile-GET] error: {e}")
                        if not tile_total:
                            try:
                                tile_total.extend(
                                    fetch_tile_get_all(
                                        page,
                                        t["name"],
                                        store_id,
                                        now_primary,
                                        now_secondary,
                                        now_taxonomy,
                                        tile_id=t["filterId"],
                                        tile_name=t["name"],
                                        gap=0.6,
                                    )
                                )
                            except Exception as e:
                                print(f"[tile-GET(label)] error: {e}")
                        print(f"   -> total products for tile {idx}: {len(tile_total)}")
                        collected.extend(tile_total)
                        time.sleep(TILE_GAP_SEC)
                    collected = dedupe_rows(collected)
                    print(f"[done] unique rows: {len(collected)}")
                    try:
                        db_rows = rows_for_db(collected)
                        upsert_batches(sb, TABLE_NAME, db_rows, batch_size=400)
                    except Exception as e:
                        print(f"[db error] {e}")
            finally:
                try:
                    browser.close()
                except Exception:
                    pass
        wait_secs = 5 * 60
        print(f"[scraper] Sleeping for {wait_secs//60} minutes before next cycle...")
        time.sleep(wait_secs)

if __name__ == "__main__":
    run()
