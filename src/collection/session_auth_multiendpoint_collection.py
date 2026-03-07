"""Session-auth multi-endpoint collection with quality gates."""

from __future__ import annotations

import argparse
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pandas as pd
import requests

from common_runtime import (
    apply_proxy,
    ensure_dir,
    get_logger,
    remove_yesterday_files,
    save_binary_file,
    validate_excel_nonempty,
)

LOGGER = get_logger("session_collector")


@dataclass(frozen=True)
class Endpoint:
    name: str
    url: str


ENDPOINTS: Dict[int, Endpoint] = {
    0: Endpoint("Sellin", "http://vendor.yfdyf.cn/sup/sup/client/report/exportPurchaseReport"),
    1: Endpoint("Endsales", "http://vendor.yfdyf.cn/sup/sup/client/report/exportLogisReport"),
    2: Endpoint("Offtake", "http://vendor.yfdyf.cn/sup/sup/client/report/exportDaysaleReport"),
    3: Endpoint("Inventory", "http://vendor.yfdyf.cn/sup/sup/client/report/exportQuantityReport"),
}


def login(session: requests.Session, username: str, password: str) -> None:
    url = "http://vendor.yfdyf.cn/sup/login"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "http://vendor.yfdyf.cn",
        "Referer": "http://vendor.yfdyf.cn/sup/login",
        "User-Agent": "Mozilla/5.0",
    }
    payload = {"username": username, "password": password}
    response = session.post(url, headers=headers, data=payload, timeout=30)
    response.raise_for_status()
    LOGGER.info("Login successful.")


def download_endpoint(
    session: requests.Session,
    endpoint: Endpoint,
    date_start: str,
    date_end: str,
    output_root: Path,
) -> bool:
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "http://vendor.yfdyf.cn",
        "User-Agent": "Mozilla/5.0",
    }
    payload = {
        "beginBudat": date_start,
        "endBudat": date_end,
        "beginDeliveryDate": date_start,
        "endDeliveryDate": date_end,
        "beginTima": date_start,
        "endTima": date_end,
    }
    response = session.post(endpoint.url, headers=headers, data=payload, timeout=120)
    if response.status_code != 200:
        LOGGER.error("%s download failed (status=%s)", endpoint.name, response.status_code)
        return False

    target_folder = ensure_dir(output_root / endpoint.name)
    output_file = target_folder / f"{endpoint.name}_{date_end}.xlsx"
    save_binary_file(output_file, response.content)

    try:
        if not validate_excel_nonempty(output_file, skiprows=1):
            LOGGER.warning("%s has no records. File removed.", output_file.name)
            output_file.unlink(missing_ok=True)
            return False
        # Normalize exported file format for downstream consistency.
        df = pd.read_excel(output_file, skiprows=1)
        df.to_excel(output_file, index=False)
    except Exception as exc:  # pragma: no cover - protective gate
        LOGGER.exception("Failed to validate %s: %s", output_file, exc)
        output_file.unlink(missing_ok=True)
        return False

    remove_yesterday_files(target_folder, endpoint.name, LOGGER)
    LOGGER.info("%s exported: %s", endpoint.name, output_file)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Session-based multi-endpoint collector")
    today = dt.date.today()
    parser.add_argument("--date-start", default=dt.date(today.year, today.month, 1).strftime("%Y-%m-%d"))
    parser.add_argument("--date-end", default=today.strftime("%Y-%m-%d"))
    parser.add_argument("--output-folder", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--proxy", default="")
    args = parser.parse_args()

    apply_proxy(args.proxy)
    output_root = ensure_dir(args.output_folder)

    with requests.Session() as session:
        login(session, args.username, args.password)
        status = {
            endpoint.name: download_endpoint(
                session=session,
                endpoint=endpoint,
                date_start=args.date_start,
                date_end=args.date_end,
                output_root=output_root,
            )
            for endpoint in ENDPOINTS.values()
        }
    failed = [name for name, ok in status.items() if not ok]
    if failed:
        raise SystemExit(f"Download failed for endpoints: {failed}")


if __name__ == "__main__":
    main()