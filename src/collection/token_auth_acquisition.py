"""Token acquisition script for token-driven collection pipeline."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from selenium import webdriver

from common_runtime import get_logger

LOGGER = get_logger("token_acquisition")


def extract_token_from_url(url: str) -> str:
    parsed = urlparse(url)
    token = parse_qs(parsed.query).get("tokenId", [""])[0]
    if not token:
        raise ValueError("tokenId not found in redirected URL.")
    return f"Bearer {token}"


def get_token(login_url: str, timeout_seconds: int, browser: str) -> str:
    if browser.lower() == "edge":
        driver = webdriver.Edge()
    elif browser.lower() == "firefox":
        driver = webdriver.Firefox()
    else:
        raise ValueError("Unsupported browser. Use 'edge' or 'firefox'.")

    try:
        driver.get(login_url)
        start_time = time.time()
        last_url = login_url
        while time.time() - start_time < timeout_seconds:
            last_url = driver.current_url
            if "tokenId=" in last_url:
                token = extract_token_from_url(last_url)
                LOGGER.info("Token captured successfully.")
                return token
            time.sleep(0.5)
        raise TimeoutError(f"Token was not captured within {timeout_seconds}s. Last URL: {last_url}")
    finally:
        driver.quit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Acquire auth token from redirected login URL.")
    parser.add_argument("--login-url", default="https://scicore.bayer.cn/web/passport/login")
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--browser", default="edge")
    parser.add_argument("--output-token-file", required=True)
    args = parser.parse_args()

    token = get_token(args.login_url, args.timeout_seconds, args.browser)
    token_file = Path(args.output_token_file)
    token_file.parent.mkdir(parents=True, exist_ok=True)
    token_file.write_text(token, encoding="utf-8")
    LOGGER.info("Token written to: %s", token_file)


if __name__ == "__main__":
    main()