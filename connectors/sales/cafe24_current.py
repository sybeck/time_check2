import os
import re
import time
import json
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

load_dotenv()
KST = timezone(timedelta(hours=9))
TIMEOUT = 20_000


def must_env(key: str) -> str:
    v = os.getenv(key, "").strip()
    if not v:
        raise RuntimeError(f"{key} 환경변수가 필요합니다. .env를 확인하세요.")
    return v


def must_env_profile(profile: str, suffix: str) -> str:
    return must_env(f"CAFE24_{profile.strip().upper()}_{suffix}")


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def load_brainology_products() -> List[str]:
    raw = os.getenv("BRAINOLOGY_PRODUCT_NAMES", "").strip()
    items = [x.strip() for x in raw.split(",") if x.strip()]
    if not items:
        raise RuntimeError("BRAINOLOGY_PRODUCT_NAMES 환경변수가 필요합니다. 예: 뉴턴젤리,오메가3")
    return items


def product_keywords_map() -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    for product in load_brainology_products():
        kws = [product]
        alias_env = os.getenv(f"BRAINOLOGY_PRODUCT_ALIASES_{product}", "").strip()
        if alias_env:
            kws.extend([x.strip() for x in alias_env.split(",") if x.strip()])
        result[product] = kws
    return result


def match_product_key(name: str, keywords: List[str]) -> bool:
    base = normalize_text(name).lower()
    for kw in keywords:
        if normalize_text(kw).lower() in base:
            return True
    return False


def save_debug(page, prefix: str = "fail") -> None:
    os.makedirs("debug", exist_ok=True)
    page.screenshot(path=f"debug/{prefix}.png", full_page=True)
    with open(f"debug/{prefix}.html", "w", encoding="utf-8") as f:
        f.write(page.content())


def parse_two_numbers(raw: str) -> tuple[int, int]:
    raw = normalize_text(raw)
    nums = re.findall(r"\d[\d,]*", raw)
    if len(nums) < 2:
        raise ValueError(f"텍스트에서 숫자 2개(매출/주문수)를 파싱하지 못했습니다: {raw}")
    return int(nums[0].replace(",", "")), int(nums[1].replace(",", ""))


def login_cafe24(page, profile: str) -> None:
    url = must_env_profile(profile, "ADMIN_URL")
    user = must_env_profile(profile, "ADMIN_ID")
    pw = must_env_profile(profile, "ADMIN_PW")
    page.goto(url, wait_until="domcontentloaded")
    try:
        page.wait_for_load_state("load", timeout=5_000)
    except PwTimeoutError:
        pass
    time.sleep(1.0)
    scopes = [page] + list(page.frames)
    deadline = time.time() + 5.0
    submitted = False
    while time.time() < deadline and not submitted:
        for scope in scopes:
            try:
                id_loc = scope.locator("input[name='id'], input#id, input[name='mall_id'], input[type='text'], input[placeholder*='아이디'], input[placeholder*='ID']")
                pw_loc = scope.locator("input[name='passwd'], input#passwd, input[name='password'], input[type='password'], input[placeholder*='비밀번호'], input[placeholder*='Password']")
                if id_loc.count() == 0 or pw_loc.count() == 0:
                    continue
                try:
                    id_loc.first.wait_for(state="visible", timeout=1_000)
                    pw_loc.first.wait_for(state="visible", timeout=1_000)
                except Exception:
                    continue
                id_loc.first.fill(user)
                pw_loc.first.fill(pw)
                btn = scope.locator("button:has-text('로그인'), [role='button']:has-text('로그인'), input[value*='로그인']")
                if btn.count() > 0:
                    btn.first.click()
                else:
                    pw_loc.first.press("Enter")
                submitted = True
                break
            except Exception:
                continue
        if not submitted:
            time.sleep(0.25)
    if not submitted:
        save_debug(page, f"{profile}_login_form_not_found")
        raise RuntimeError(f"[{profile}] 로그인 폼/버튼을 찾지 못했습니다.")
    time.sleep(4)


def wait_after_login(page, profile: str) -> None:
    try:
        page.wait_for_load_state("domcontentloaded", timeout=10_000)
    except PwTimeoutError:
        pass
    wait_ms_key = f"CAFE24_{profile.strip().upper()}_POST_LOGIN_WAIT_MS"
    wait_ms = int(os.getenv(wait_ms_key, os.getenv("CAFE24_POST_LOGIN_WAIT_MS", "300")))
    if wait_ms > 0:
        time.sleep(wait_ms / 1000)


def get_dashboard_url(profile: str) -> str:
    key = f"CAFE24_{profile.strip().upper()}_DASHBOARD_URL"
    v = os.getenv(key, "").strip()
    if v:
        return v
    if profile.strip().lower() == "brainology":
        return "https://brainology.cafe24.com/disp/admin/shop1/order/DashboardMain"
    raise RuntimeError(f"[{profile}] {key} 를 .env에 추가하세요.")


def scrape_by_total_order_amount_right_cell(page) -> str:
    label = page.get_by_role("cell", name="총 주문 금액").first
    label.wait_for(state="visible", timeout=TIMEOUT)
    try:
        row = label.locator("xpath=ancestor::*[@role='row'][1]")
        cells = row.get_by_role("cell")
        if cells.count() == 0:
            cells = row.get_by_role("gridcell")
        idx = None
        for i in range(cells.count()):
            t = normalize_text(cells.nth(i).inner_text())
            if t == "총 주문 금액":
                idx = i
                break
        if idx is None or idx + 1 >= cells.count():
            raise RuntimeError("오른쪽 cell 못 찾음")
        text = normalize_text(cells.nth(idx + 1).inner_text())
        if text:
            return text
    except Exception:
        pass
    text = page.evaluate(
        """() => {
          const el = Array.from(document.querySelectorAll('td,th,div,[role="cell"]'))
            .find(x => (x.innerText || x.textContent || '').trim() === '총 주문 금액');
          if (!el) return '';
          const sib = el.nextElementSibling;
          return sib ? (sib.innerText || sib.textContent || '') : '';
        }"""
    )
    text = normalize_text(text)
    if text:
        return text
    raise RuntimeError("'총 주문 금액' 오른쪽 칸 텍스트를 찾지 못했습니다.")


def scrape_today_header_below_cell_text(page) -> str:
    header = page.get_by_role("columnheader", name="오늘").first
    header.wait_for(state="visible", timeout=TIMEOUT)
    try:
        header_row = header.locator("xpath=ancestor::*[@role='row'][1]")
        headers = header_row.get_by_role("columnheader")
        idx = None
        for i in range(headers.count()):
            if normalize_text(headers.nth(i).inner_text()) == "오늘":
                idx = i
                break
        if idx is None:
            raise RuntimeError("오늘 column index 못 찾음")
        body_row = header_row.locator("xpath=following-sibling::*[@role='row'][1]")
        cells = body_row.get_by_role("cell")
        if cells.count() == 0:
            cells = body_row.get_by_role("gridcell")
        text = normalize_text(cells.nth(idx).inner_text())
        if text:
            return text
    except Exception:
        pass
    raise RuntimeError("'오늘' 컬럼 바로 아래 칸 텍스트를 찾지 못했습니다.")


def get_current_metrics_burdenzero() -> dict:
    today = datetime.now(KST).date()
    headless = os.getenv("HEADLESS", "false").lower() == "true"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(TIMEOUT)
        try:
            login_cafe24(page, profile="burdenzero")
            wait_after_login(page, profile="burdenzero")
            page.goto(get_dashboard_url("burdenzero"), wait_until="domcontentloaded")
            time.sleep(4)
            try:
                page.get_by_role("cell", name="총 주문 금액").first.wait_for(state="visible", timeout=TIMEOUT)
            except Exception:
                pass
            try:
                raw = scrape_by_total_order_amount_right_cell(page)
            except Exception:
                raw = scrape_today_header_below_cell_text(page)
            sales, orders = parse_two_numbers(raw)
            return {
                "status": "ok",
                "date": today.isoformat(),
                "source": "cafe24",
                "profile": "burdenzero",
                "mapped": {"burdenzero": {"sales": int(sales), "orders": int(orders)}},
                "raw": raw,
            }
        finally:
            context.close()
            browser.close()


def _sleep(sec: float):
    time.sleep(sec)


def _wait_or_sleep(page, sec: float = 3.0):
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass
    _sleep(sec)


def _settle_after_login(page):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=20_000)
    except Exception:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass
    _sleep(3)


def _goto_with_retry(page, url: str, tries: int = 4):
    last_err = None
    for attempt in range(tries):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            _wait_or_sleep(page, 3)
            return
        except Exception as e:
            last_err = e
            _sleep(2 + attempt)
    raise last_err


def _select_analysis_template(page):
    target_value = os.getenv("CAFE24_BRAINOLOGY_ANALYSIS_TEMPLATE_VALUE", "49").strip() or "49"
    target_label = os.getenv("CAFE24_BRAINOLOGY_ANALYSIS_TEMPLATE_LABEL", "분석용").strip() or "분석용"
    sel = page.locator("#aManagesList")
    sel.wait_for(state="visible", timeout=15_000)
    for kwargs in ({"value": target_value}, {"label": target_label}):
        try:
            sel.select_option(**kwargs)
            _sleep(1)
            break
        except Exception:
            pass
    v = page.evaluate("""() => {
        const el = document.querySelector('#aManagesList');
        return el ? (el.value || '') : '';
    }""")
    if str(v or "") != target_value:
        page.evaluate(
            """(value) => {
                const el = document.querySelector('#aManagesList');
                if (!el) return;
                el.value = value;
                el.dispatchEvent(new Event('change', { bubbles: true }));
            }""",
            target_value,
        )
        _sleep(1)


def _read_sales_file(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv", ".txt"]:
        last_err = None
        for enc in ["utf-8-sig", "cp949", "euc-kr", "utf-8"]:
            try:
                return pd.read_csv(path, encoding=enc)
            except Exception as e:
                last_err = e
                try:
                    return pd.read_csv(path, encoding=enc, sep=None, engine="python")
                except Exception as e2:
                    last_err = e2
        raise last_err
    return pd.read_excel(path, engine="openpyxl")


def _col(df: pd.DataFrame, name: str, fallback_idx: int):
    if name in df.columns:
        return df[name]
    if df.shape[1] > fallback_idx:
        return df.iloc[:, fallback_idx]
    return None


def _num(x) -> float:
    if x is None:
        return 0.0
    try:
        if pd.isna(x):
            return 0.0
    except Exception:
        pass
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(x)
    except Exception:
        pass
    s = str(x).strip()
    if s == "" or s.lower() == "nan" or s == "-":
        return 0.0
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = s.replace("₩", "").replace("원", "").replace(",", "").strip()
    s = re.sub(r"[^0-9\.\-]", "", s)
    if s in ("", "-", "."):
        return 0.0
    try:
        v = float(s)
        return -v if neg else v
    except Exception:
        return 0.0


def aggregate_brainology_excel(excel_path: str, ymd: str) -> Dict[str, Dict[str, int]]:
    df = _read_sales_file(excel_path)
    colA = _col(df, "주문일시", 0)
    colB = _col(df, "결제일시(입금확인일)", 1)
    colE = _col(df, "상품명(한국어 쇼핑몰)", 4)
    colH = _col(df, "상품구매금액(KRW)", 7)
    colI = _col(df, "사용한 적립금액(최종)", 8)
    colJ = _col(df, "주문서 쿠폰 할인금액", 9)
    colM = _col(df, "실제 환불금액", 12)
    colN = _col(df, "품목별 주문번호", 13)
    if colA is None or colB is None or colE is None or colH is None or colN is None:
        raise RuntimeError(f"필수 컬럼을 찾지 못했습니다. columns={list(df.columns)}")
    result = {product: {"sales": 0, "orders": 0} for product in load_brainology_products()}
    kw_map = product_keywords_map()
    for idx in range(len(df)):
        a = "" if pd.isna(colA.iloc[idx]) else str(colA.iloc[idx])
        if a[:10] != ymd:
            continue
        b = colB.iloc[idx]
        if pd.isna(b) or str(b).strip() == "":
            continue
        m = 0.0 if colM is None else _num(colM.iloc[idx])
        if m > 0:
            continue
        product_name = "" if pd.isna(colE.iloc[idx]) else str(colE.iloc[idx]).strip()
        h = 0.0 if colH is None else _num(colH.iloc[idx])
        i = 0.0 if colI is None else _num(colI.iloc[idx])
        j = 0.0 if colJ is None else _num(colJ.iloc[idx])
        n_text = "" if colN is None or pd.isna(colN.iloc[idx]) else str(colN.iloc[idx]).strip()
        rev = h - i - j if n_text.endswith("1") else h
        for product, keywords in kw_map.items():
            if match_product_key(product_name, keywords):
                result[product]["sales"] += int(round(rev))
                result[product]["orders"] += 1
                break
    return result


def download_brainology_excel(page, download_dir: str) -> Path:
    Path(download_dir).mkdir(parents=True, exist_ok=True)
    excel_page_url = os.getenv(
        "CAFE24_BRAINOLOGY_EXCEL_PAGE_URL",
        "https://brainology.cafe24.com/admin/php/shop1/Excel/ExcelCreateRequest.php",
    ).strip()
    _goto_with_retry(page, excel_page_url, tries=5)
    _sleep(3)
    page.get_by_role("link", name="오늘").click()
    _sleep(3)
    page.locator("#aManagesList").click()
    _sleep(3)
    _select_analysis_template(page)
    _sleep(3)
    try:
        page.once("dialog", lambda d: d.accept())
    except Exception:
        pass
    page.get_by_role("link", name="엑셀파일 생성 요청").click()
    _sleep(3)
    _sleep(int(os.getenv("CAFE24_BRAINOLOGY_DOWNLOAD_WAIT_SEC", "10")))
    container = page.locator("#QA_download_list1 div").filter(has_text="요청파일 목록")
    max_loops = int(os.getenv("CAFE24_BRAINOLOGY_DOWNLOAD_RETRY_COUNT", "12"))
    for _ in range(max_loops):
        try:
            dl_link = container.get_by_role("link", name="다운로드").first
            if dl_link.count() > 0:
                with page.expect_download(timeout=30_000) as dl_info:
                    dl_link.click()
                download = dl_info.value
                save_to = Path(download_dir) / download.suggested_filename
                download.save_as(str(save_to))
                return save_to
        except PwTimeoutError:
            pass
        except Exception:
            pass
        _sleep(10)
    raise RuntimeError("카페24 엑셀 다운로드 링크를 찾지 못했습니다.")


def get_current_metrics_brainology() -> dict:
    today = datetime.now(KST).date()
    ymd = today.strftime("%Y-%m-%d")
    headless = os.getenv("HEADLESS", "false").lower() == "true"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(TIMEOUT)
        try:
            login_cafe24(page, profile="brainology")
            wait_after_login(page, profile="brainology")
            _settle_after_login(page)
            excel_path = download_brainology_excel(page, download_dir="downloads")
            mapped = aggregate_brainology_excel(str(excel_path), ymd)
            return {
                "status": "ok",
                "date": ymd,
                "source": "cafe24",
                "profile": "brainology",
                "excel_path": str(excel_path),
                "mapped": mapped,
            }
        finally:
            context.close()
            browser.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="brainology", choices=["brainology", "burdenzero"])
    ap.add_argument("--all", action="store_true", help="brainology + burdenzero 둘 다 조회")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if args.all:
        r_bz = get_current_metrics_burdenzero()
        r_bio = get_current_metrics_brainology()
        mapped = {"burdenzero": (r_bz.get("mapped") or {}).get("burdenzero", {"sales": 0, "orders": 0})}
        mapped.update(r_bio.get("mapped") or {})
        out = {
            "status": "ok",
            "source": "cafe24",
            "date": r_bio.get("date") or r_bz.get("date"),
            "mapped": mapped,
        }
        print(json.dumps(out, ensure_ascii=False) if args.json else out)
        return

    result = get_current_metrics_brainology() if args.profile == "brainology" else get_current_metrics_burdenzero()
    print(json.dumps(result, ensure_ascii=False) if args.json else result)


if __name__ == "__main__":
    main()