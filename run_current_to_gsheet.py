import os
import json
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Tuple, List

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()

os.makedirs(r"C:\temp", exist_ok=True)
os.environ["TEMP"] = r"C:\temp"
os.environ["TMP"] = r"C:\temp"
tempfile.tempdir = r"C:\temp"
SAFE_TEMP_DIR = r"C:\Temp"

KST = timezone(timedelta(hours=9))
SPREADSHEET_ID = "1DeSRVN4pWf6rnp1v_FeePUYe1ngjwyq_znXZUzl_kbM"
SLOT_TOLERANCE_MINUTES = 30
SLOTS = [
    ("10:00", 10, 0, "B"),
    ("12:00", 12, 0, "I"),
    ("14:00", 14, 0, "P"),
    ("16:00", 16, 0, "W"),
    ("18:00", 18, 0, "AD"),
    ("20:00", 20, 0, "AK"),
    ("22:00", 22, 0, "AR"),
]
FIELDS = [
    "meta_spend",
    "cafe24_sales",
    "cafe24_orders",
    "coupang_sales",
    "coupang_orders",
    "naver_sales",
    "naver_orders",
]


def load_brainology_products() -> List[str]:
    raw = os.getenv("BRAINOLOGY_PRODUCT_NAMES", "").strip()
    items = [x.strip() for x in raw.split(",") if x.strip()]
    if not items:
        raise RuntimeError("BRAINOLOGY_PRODUCT_NAMES 환경변수가 필요합니다. 예: 뉴턴젤리,오메가3")
    return items


def target_keys() -> List[str]:
    return ["burdenzero"] + load_brainology_products()


def brand_sheets() -> Dict[str, str]:
    sheets = {"burdenzero": "부담제로_지금"}
    for product in load_brainology_products():
        sheets[product] = f"{product}_지금"
    return sheets


def display_name(key: str) -> str:
    return "부담제로" if key == "burdenzero" else key


def now_kst() -> datetime:
    return datetime.now(KST)


def today_ymd_kst() -> str:
    return now_kst().date().strftime("%Y-%m-%d")


def pick_slot(dt: datetime) -> Optional[Tuple[str, str]]:
    tolerance_sec = SLOT_TOLERANCE_MINUTES * 60
    for label, hh, mm, col in SLOTS:
        center = dt.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if abs((dt - center).total_seconds()) <= tolerance_sec:
            return (label, col)
    return None


def col_to_index(col: str) -> int:
    n = 0
    for ch in col.strip().upper():
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n


def index_to_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(r + ord("A")) + s
    return s


def run_script_json(py_path: str, args: List[str]) -> Dict[str, Any]:
    os.makedirs(SAFE_TEMP_DIR, exist_ok=True)
    env = os.environ.copy()
    env["TEMP"] = SAFE_TEMP_DIR
    env["TMP"] = SAFE_TEMP_DIR
    env["TMPDIR"] = SAFE_TEMP_DIR
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    p = subprocess.run(["python", py_path] + args, capture_output=True, text=True, encoding="utf-8", env=env)
    if p.returncode != 0:
        raise RuntimeError(f"[SCRIPT FAIL] {py_path}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}\n")
    lines = [ln.strip() for ln in (p.stdout or "").splitlines() if ln.strip()]
    if not lines:
        raise RuntimeError(f"[SCRIPT NO OUTPUT] {py_path}")
    return json.loads(lines[-1])


def get_sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    json_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    if json_str:
        info = json.loads(json_str)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    elif json_file:
        creds = service_account.Credentials.from_service_account_file(json_file, scopes=scopes)
    else:
        raise RuntimeError("ENV 필요: GOOGLE_SERVICE_ACCOUNT_JSON 또는 GOOGLE_SERVICE_ACCOUNT_FILE")
    return build("sheets", "v4", credentials=creds)


def get_sheet_values(svc, sheet_name: str, a1: str) -> List[List[Any]]:
    return svc.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=f"{sheet_name}!{a1}").execute().get("values", [])


def update_sheet_values(svc, sheet_name: str, a1: str, values: List[List[Any]]):
    body = {"values": values}
    return svc.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID, range=f"{sheet_name}!{a1}", valueInputOption="USER_ENTERED", body=body).execute()


def append_sheet_values(svc, sheet_name: str, a1: str, values: List[List[Any]]):
    body = {"values": values}
    return svc.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=f"{sheet_name}!{a1}", valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body=body).execute()


def _normalize_ymd(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    if len(s) >= 10:
        head10 = s[:10]
        try:
            return datetime.strptime(head10, "%Y-%m-%d").date().strftime("%Y-%m-%d")
        except Exception:
            pass
    s2 = s.replace(".", "-").replace("/", "-")
    parts = s2.split()
    s2 = parts[0] if parts else s2
    try:
        return datetime.strptime(s2, "%Y-%m-%d").date().strftime("%Y-%m-%d")
    except Exception:
        try:
            y, m, d = s2.split("-")
            if y.isdigit() and m.isdigit() and d.isdigit():
                return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        except Exception:
            pass
    return s


def find_or_create_today_row(svc, sheet_name: str, ymd: str) -> int:
    colA = get_sheet_values(svc, sheet_name, "A:A")
    last_filled_row_idx = 0
    last_value = ""
    for i in range(len(colA), 0, -1):
        row = colA[i - 1]
        if row and str(row[0]).strip():
            last_filled_row_idx = i
            last_value = row[0]
            break
    if last_filled_row_idx > 0 and _normalize_ymd(last_value) == ymd:
        return last_filled_row_idx
    append_sheet_values(svc, sheet_name, "A:A", [[ymd]])
    colA2 = get_sheet_values(svc, sheet_name, "A:A")
    return len(colA2) if colA2 else 1


def build_row_payload(target: str, cafe24: Dict[str, Any], coupang: Dict[str, Any], naver: Dict[str, Any], meta: Dict[str, Any]) -> List[Any]:
    m = (meta.get("mapped") or {}).get(target) or {}
    c = (cafe24.get("mapped") or {}).get(target) or {}
    cp = (coupang.get("mapped") or {}).get(target) or {}
    nv = (naver.get("mapped") or {}).get(target) or {}
    return [
        float(m.get("spend") or 0.0),
        int(c.get("sales") or 0),
        int(c.get("orders") or 0),
        int(cp.get("sales") or 0),
        int(cp.get("orders") or 0),
        int(nv.get("sales") or 0),
        int(nv.get("orders") or 0),
    ]


def slack_post(text: str) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook:
        print("[SLACK SKIP] SLACK_WEBHOOK_URL 미설정")
        return
    r = requests.post(webhook, json={"text": text}, timeout=15)
    if r.status_code >= 300:
        raise RuntimeError(f"[SLACK WEBHOOK FAIL] {r.status_code} {r.text[:300]}")


def compute_roas_cpa_for_brand(target: str, cafe24_res, coupang_res, naver_res, meta_res) -> dict:
    m = (meta_res.get("mapped") or {}).get(target) or {}
    c = (cafe24_res.get("mapped") or {}).get(target) or {}
    cp = (coupang_res.get("mapped") or {}).get(target) or {}
    nv = (naver_res.get("mapped") or {}).get(target) or {}
    spend = float(m.get("spend") or 0.0)
    cafe24_sales = int(c.get("sales") or 0)
    cafe24_orders = int(c.get("orders") or 0)
    coupang_sales = int(cp.get("sales") or 0)
    coupang_orders = int(cp.get("orders") or 0)
    naver_sales = int(nv.get("sales") or 0)
    naver_orders = int(nv.get("orders") or 0)
    revenue = cafe24_sales + coupang_sales + naver_sales
    purchases = cafe24_orders + coupang_orders + naver_orders
    roas = (revenue / spend) if spend > 0 else 0.0
    cpa = (spend / purchases) if purchases > 0 else 0.0
    return {"spend": spend, "purchases": purchases, "revenue": revenue, "roas": roas, "cpa": cpa}


def main():
    now = now_kst()
    picked = pick_slot(now)
    if not picked:
        print(f"[SKIP] 현재시각(KST) {now.strftime('%H:%M')} 은 지정 슬롯(±{SLOT_TOLERANCE_MINUTES}분)에 해당 없음. 기록하지 않음.")
        return
    slot_label, start_col = picked
    ymd = today_ymd_kst()
    print(f"[INFO] slot={slot_label} start_col={start_col} date={ymd}")

    cafe24_res = run_script_json("connectors/sales/cafe24_current.py", ["--all", "--json"])
    coupang_res = run_script_json("connectors/sales/coupang_current.py", ["--json"])
    naver_res = run_script_json("connectors/sales/naver_current.py", ["--json"])
    meta_res = run_script_json("connectors/meta/meta_ads_current.py", ["--json"])

    svc = get_sheets_service()
    start_idx = col_to_index(start_col)
    end_col = index_to_col(start_idx + len(FIELDS) - 1)

    for target, sheet_name in brand_sheets().items():
        row_idx = find_or_create_today_row(svc, sheet_name, ymd)
        values = build_row_payload(target, cafe24_res, coupang_res, naver_res, meta_res)
        range_a1 = f"{start_col}{row_idx}:{end_col}{row_idx}"
        update_sheet_values(svc, sheet_name, range_a1, [values])
        print(f"[OK] {sheet_name} row={row_idx} range={range_a1} values={values}")

    msg_lines = ["*👀 현재 ROAS/CPA 알림*", f"- 날짜: {ymd} / 슬롯: {slot_label}"]
    for target in target_keys():
        s = compute_roas_cpa_for_brand(target, cafe24_res, coupang_res, naver_res, meta_res)
        msg_lines.extend([
            f"\n*✅ {display_name(target)}*",
            f"• 현재 매출 {s['revenue']:,} / {s['purchases']:,}",
            f"• 메타 광고비: {s['spend']:,.0f}",
            f"• ROAS {s['roas']:,.2f} / CPA {s['cpa']:,.0f}",
        ])
    slack_post("\n".join(msg_lines))
    print("[SLACK OK] sent")


if __name__ == "__main__":
    main()