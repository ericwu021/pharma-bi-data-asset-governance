"""Token-based daily collection with explicit file-quality gate."""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import Dict, Tuple

import requests

from common_runtime import (
    apply_proxy,
    ensure_dir,
    get_logger,
    load_text,
    remove_yesterday_files,
    save_binary_file,
    validate_excel_nonempty,
)

LOGGER = get_logger("token_daily_collector")


def build_headers(token: str) -> Dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Authorization": token,
        "Content-Type": "application/json",
        "Referer": "https://scicore.bayer.cn/",
        "User-Agent": "Mozilla/5.0",
    }


def base_payload(report_date: str) -> Dict:
    return {
        "fromProvCityArray": None,
        "sellerLevel": None,
        "partnerArray": [],
        "buyerProvCityArray": None,
        "buyerType": None,
        "rawBuyerArray": [],
        "productArray": [],
        "reportDate": report_date,
        "zipType": "2",
    }


def download_and_gate(
    session: requests.Session,
    url: str,
    payload: Dict,
    headers: Dict[str, str],
    output_file: Path,
) -> bool:
    response = session.post(url, headers=headers, json=payload, timeout=120)
    if response.status_code != 200:
        LOGGER.error("Request failed (%s) for %s", response.status_code, output_file.name)
        return False

    save_binary_file(output_file, response.content)
    try:
        if not validate_excel_nonempty(output_file):
            output_file.unlink(missing_ok=True)
            LOGGER.warning("Downloaded empty file removed: %s", output_file.name)
            return False
    except Exception as exc:  # pragma: no cover - protective gate
        output_file.unlink(missing_ok=True)
        LOGGER.exception("Excel validation failed for %s: %s", output_file.name, exc)
        return False
    return True


def run_daily_batch(
    token: str,
    report_date: str,
    sellin_dir: Path,
    sellout_dir: Path,
    offtake_dir: Path,
) -> Dict[str, bool]:
    headers = build_headers(token)
    with requests.Session() as session:
        statuses: Dict[str, bool] = {}

        sellin_payload = base_payload(report_date) | {"remark": "Sell-in"}
        sellin_file = ensure_dir(sellin_dir) / f"Sellin_{report_date}.xlsx"
        statuses["sellin"] = download_and_gate(
            session=session,
            url="https://scicoredata.bayer.cn/pc/ddi/downloadDdiSaleInfo",
            payload=sellin_payload,
            headers=headers,
            output_file=sellin_file,
        )
        if statuses["sellin"]:
            remove_yesterday_files(sellin_dir, "Sellin_", LOGGER)

        sellout_payload = base_payload(report_date) | {"remark": "Sell-out"}
        sellout_file = ensure_dir(sellout_dir) / f"Sellout_{report_date}.xlsx"
        statuses["sellout"] = download_and_gate(
            session=session,
            url="https://scicoredata.bayer.cn/pc/ddi/downloadDdiSaleInfo",
            payload=sellout_payload,
            headers=headers,
            output_file=sellout_file,
        )
        if statuses["sellout"]:
            remove_yesterday_files(sellout_dir, "Sellout_", LOGGER)

        offtake_payload = base_payload(report_date)
        offtake_file = ensure_dir(offtake_dir) / f"Offtake_DDI_{report_date}.xlsx"
        statuses["offtake"] = download_and_gate(
            session=session,
            url="https://scicoredata.bayer.cn/pc/ddi/downloadDdiOfftakeInfo",
            payload=offtake_payload,
            headers=headers,
            output_file=offtake_file,
        )
        if statuses["offtake"]:
            remove_yesterday_files(offtake_dir, "Offtake_DDI_", LOGGER)

    return statuses


def main() -> None:
    parser = argparse.ArgumentParser(description="Token-based daily collection for sellin/sellout/offtake.")
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--report-date", default=(dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d"))
    parser.add_argument("--sellin-dir", required=True)
    parser.add_argument("--sellout-dir", required=True)
    parser.add_argument("--offtake-dir", required=True)
    parser.add_argument("--proxy", default="")
    args = parser.parse_args()

    apply_proxy(args.proxy)
    token = load_text(args.token_file)
    result = run_daily_batch(
        token=token,
        report_date=args.report_date,
        sellin_dir=Path(args.sellin_dir),
        sellout_dir=Path(args.sellout_dir),
        offtake_dir=Path(args.offtake_dir),
    )
    LOGGER.info("Daily batch result: %s", result)

    failed = [name for name, ok in result.items() if not ok]
    if failed:
        raise SystemExit(f"Failed report types: {failed}")


if __name__ == "__main__":
    main()