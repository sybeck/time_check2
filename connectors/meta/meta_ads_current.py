import os
import json
import argparse
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import requests
from dotenv import load_dotenv

load_dotenv()

KST = timezone(timedelta(hours=9))
META_API_VERSION = os.getenv("META_API_VERSION", "v24.0").strip()
GRAPH_BASE = f"https://graph.facebook.com/{META_API_VERSION}"
GRAPH_BASE_NO_VER = "https://graph.facebook.com"
TIMEOUT = 30

PURCHASE_ACTION_KEYS = {
    "purchase",
    "omni_purchase",
    "offsite_conversion.purchase",
    "web_in_store_purchase",
    "onsite_conversion.purchase",
}

NEEDED_PERMS = {"ads_read", "read_insights"}
NICE_TO_HAVE_PERMS = {"ads_management"}


def must_env(key: str) -> str:
    v = os.getenv(key, "").strip()
    if not v:
        raise RuntimeError(f"[ENV ERROR] {key} 가 필요합니다. .env를 확인하세요.")
    return v


def ymd_today_kst() -> str:
    return datetime.now(KST).date().strftime("%Y-%m-%d")


def normalize_act_id(ad_account_id: str) -> str:
    ad_account_id = (ad_account_id or "").strip()
    if not ad_account_id:
        return ""
    return ad_account_id if ad_account_id.startswith("act_") else f"act_{ad_account_id}"


def safe_json(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        return None


def http_get(url: str, params: dict, label: str):
    r = requests.get(url, params=params, timeout=TIMEOUT)
    data = safe_json(r)
    if r.status_code != 200:
        raise RuntimeError(
            f"[HTTP ERROR] {label}\n"
            f"  url: {url}\n"
            f"  status: {r.status_code}\n"
            f"  body: {data or r.text[:300]}"
        )
    return data


def parse_purchases_from_actions(actions) -> int:
    if not actions:
        return 0
    total = 0
    for a in actions:
        at = (a.get("action_type") or "").strip()
        val = a.get("value")
        if val is None:
            continue
        is_purchase = (at in PURCHASE_ACTION_KEYS) or at.endswith(".purchase")
        if is_purchase:
            try:
                total += int(float(val))
            except Exception:
                pass
    return total


def load_brainology_products() -> List[str]:
    raw = os.getenv("BRAINOLOGY_PRODUCT_NAMES", "").strip()
    items = [x.strip() for x in raw.split(",") if x.strip()]
    if not items:
        raise RuntimeError("[ENV ERROR] BRAINOLOGY_PRODUCT_NAMES 가 필요합니다. 예: 뉴턴젤리,오메가3")
    return items


def normalize_text(s: str) -> str:
    return " ".join((s or "").lower().split())


def match_product_key(name: str, keywords: List[str]) -> bool:
    base = normalize_text(name)
    for kw in keywords:
        if normalize_text(kw) in base:
            return True
    return False


def match_campaign_to_product(campaign_name: str, product: str, keywords: List[str]) -> bool:
    base = normalize_text(campaign_name)

    # 요청사항:
    # 뉴턴젤리 광고비는 캠페인명에 "젤리" 단어가 포함된 캠페인을 집계
    if product == "뉴턴젤리":
        return "젤리" in base

    for kw in keywords:
        if normalize_text(kw) in base:
            return True
    return False


def product_keywords_map() -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    for product in load_brainology_products():
        kws = [product]
        alias_env = os.getenv(f"BRAINOLOGY_PRODUCT_ALIASES_{product}", "").strip()
        if alias_env:
            kws.extend([x.strip() for x in alias_env.split(",") if x.strip()])
        result[product] = kws
    return result


def debug_token(access_token: str) -> dict:
    url = f"{GRAPH_BASE_NO_VER}/debug_token"
    params = {"input_token": access_token, "access_token": access_token}
    return http_get(url, params, label="debug_token")


def get_permissions(access_token: str) -> dict:
    url = f"{GRAPH_BASE}/me/permissions"
    params = {"access_token": access_token}
    return http_get(url, params, label="me/permissions")


def list_my_adaccounts(access_token: str, limit: int = 200) -> dict:
    url = f"{GRAPH_BASE}/me/adaccounts"
    params = {"access_token": access_token, "fields": "account_id,name,account_status", "limit": limit}
    return http_get(url, params, label="me/adaccounts")


def summarize_permissions(perms_payload: dict):
    granted = set()
    declined = set()
    for row in perms_payload.get("data") or []:
        p = (row.get("permission") or "").strip()
        s = (row.get("status") or "").strip()
        if not p:
            continue
        if s == "granted":
            granted.add(p)
        elif s == "declined":
            declined.add(p)
    return granted, declined


def preflight(profile_name: str, token: str, target_act: str):
    print(f"\n[PRECHECK] {profile_name}")
    print(f"  target ad account: {target_act}")
    dbg = debug_token(token)
    dbg_data = dbg.get("data") or {}
    is_valid = dbg_data.get("is_valid")
    expires_at = dbg_data.get("expires_at")
    app_id = dbg_data.get("app_id")
    token_type = dbg_data.get("type")
    print(f"  token valid: {is_valid} | type: {token_type} | app_id: {app_id} | expires_at: {expires_at}")
    if not is_valid:
        raise RuntimeError(f"[TOKEN INVALID] {profile_name} 토큰이 유효하지 않습니다.")

    perms_payload = get_permissions(token)
    granted, _ = summarize_permissions(perms_payload)
    missing_needed = sorted(list(NEEDED_PERMS - granted))
    missing_nice = sorted(list(NICE_TO_HAVE_PERMS - granted))
    print(f"  granted perms: {', '.join(sorted(granted)) if granted else '(none?)'}")
    if missing_needed:
        raise RuntimeError(f"[PERMISSION MISSING] {profile_name}: {', '.join(missing_needed)}")
    if missing_nice:
        print(f"  ⚠️ missing optional perms: {', '.join(missing_nice)}")

    adacc_payload = list_my_adaccounts(token, limit=200)
    accessible = set()
    for a in adacc_payload.get("data") or []:
        acc_id = a.get("account_id")
        if acc_id:
            accessible.add(normalize_act_id(str(acc_id)))
        _id = a.get("id")
        if _id:
            accessible.add(normalize_act_id(str(_id).replace("act_", "")))
    print(f"  accessible ad accounts (sample up to 8): {sorted(list(accessible))[:8] or '(none)'}")
    if target_act not in accessible:
        raise RuntimeError(f"[AD ACCOUNT ACCESS] {profile_name}: {target_act} 접근 불가")
    print("  ✅ precheck OK")


def fetch_insights_current_by_campaign(access_token: str, ad_account_id: str, ymd: str) -> List[dict]:
    act_id = normalize_act_id(ad_account_id)
    url = f"{GRAPH_BASE}/{act_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "campaign_name,spend,actions,date_start,date_stop",
        "level": "campaign",
        "time_range": json.dumps({"since": ymd, "until": ymd}),
        "time_increment": 1,
        "limit": 500,
    }
    r = requests.get(url, params=params, timeout=TIMEOUT)
    data = safe_json(r)
    if r.status_code != 200:
        err = (data or {}).get("error") if isinstance(data, dict) else None
        raise RuntimeError(
            f"[INSIGHTS FAIL] {act_id}\n"
            f"  HTTP {r.status_code}\n"
            f"  error_message: {(err or {}).get('message') if isinstance(err, dict) else None}\n"
            f"  body: {data or r.text[:300]}"
        )
    return (data or {}).get("data") or []


def split_brainology_by_product(rows: List[dict], ymd: str) -> Dict[str, dict]:
    result = {name: {"date": ymd, "spend": 0.0, "purchases": 0} for name in load_brainology_products()}
    kw_map = product_keywords_map()

    for row in rows:
        ds = (row.get("date_start") or "").strip()
        if ds and ds != ymd:
            continue

        campaign_name = (row.get("campaign_name") or "").strip()
        if not campaign_name:
            continue

        for product, keywords in kw_map.items():
            if match_campaign_to_product(campaign_name, product, keywords):
                try:
                    result[product]["spend"] += float(row.get("spend") or 0.0)
                except Exception:
                    pass
                result[product]["purchases"] += parse_purchases_from_actions(row.get("actions"))
                break

    for product in result:
        result[product]["spend"] = float(result[product]["spend"])
        result[product]["purchases"] = int(result[product]["purchases"])
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="", help="YYYY-MM-DD (기본: 오늘 KST)")
    parser.add_argument("--json", action="store_true", help="마지막 줄에 JSON 결과만 출력")
    args = parser.parse_args()

    target_ymd = (args.date or "").strip() or ymd_today_kst()
    profiles = [
        {
            "name": "brainology",
            "token": must_env("META_BRAINOLOGY_ACCESS_TOKEN"),
            "ad_account": must_env("META_BRAINOLOGY_AD_ACCOUNT_ID"),
        },
        {
            "name": "burdenzero",
            "token": must_env("META_BURDENZERO_ACCESS_TOKEN"),
            "ad_account": must_env("META_BURDENZERO_AD_ACCOUNT_ID"),
        },
    ]

    print(f"[INFO] Meta Ads CURRENT (KST): {target_ymd} | API={META_API_VERSION}")
    print("=" * 70)

    mapped: Dict[str, dict] = {}
    total_spend = 0.0
    total_purchases = 0

    for p in profiles:
        target_act = normalize_act_id(p["ad_account"])
        preflight(p["name"], p["token"], target_act)

        if p["name"] == "brainology":
            rows = fetch_insights_current_by_campaign(p["token"], p["ad_account"], target_ymd)
            split_res = split_brainology_by_product(rows, target_ymd)
            mapped.update(split_res)
            for product, res in split_res.items():
                total_spend += res["spend"]
                total_purchases += res["purchases"]
                print(f"\n[RESULT] {product} ({target_act})")
                print(f"  spend(current): {res['spend']}")
                print(f"  purchases(current): {res['purchases']}")
                print("-" * 70)
        else:
            rows = fetch_insights_current_by_campaign(p["token"], p["ad_account"], target_ymd)
            spend_sum = 0.0
            purchases_sum = 0
            for row in rows:
                ds = (row.get("date_start") or "").strip()
                if ds and ds != target_ymd:
                    continue
                try:
                    spend_sum += float(row.get("spend") or 0.0)
                except Exception:
                    pass
                purchases_sum += parse_purchases_from_actions(row.get("actions"))
            mapped["burdenzero"] = {"date": target_ymd, "spend": float(spend_sum), "purchases": int(purchases_sum)}
            total_spend += spend_sum
            total_purchases += purchases_sum
            print(f"\n[RESULT] burdenzero ({target_act})")
            print(f"  spend(current): {spend_sum}")
            print(f"  purchases(current): {purchases_sum}")
            print("-" * 70)

    print("\n[TOTAL]")
    print(f"  total_spend(current): {total_spend}")
    print(f"  total_purchases(current): {total_purchases}")
    print("=" * 70)

    if args.json:
        out = {
            "date": target_ymd,
            "mapped": mapped,
            "total": {"spend": float(total_spend), "purchases": int(total_purchases)},
        }
        print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()