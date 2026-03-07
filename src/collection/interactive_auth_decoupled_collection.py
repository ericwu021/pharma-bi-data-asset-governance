"""Interactive-auth decoupled collection script."""

from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path
from typing import Dict, Tuple

import cv2
import numpy as np
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from common_runtime import ensure_dir, get_logger

LOGGER = get_logger("interactive_auth_collector")


def keep_brightest_areas(image_path: Path, output_path: Path, brightness_threshold: int = 250) -> None:
    image = cv2.imread(str(image_path))
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    _, thresholded = cv2.threshold(gray, brightness_threshold, 255, cv2.THRESH_BINARY)
    mask = cv2.cvtColor(thresholded, cv2.COLOR_GRAY2BGR)
    result = cv2.bitwise_and(image, mask)
    cv2.imwrite(str(output_path), result)


def remove_white_text_keep_graphics(
    image_path: Path,
    output_path: Path,
    min_area: int = 500,
    aspect_ratio_threshold: float = 1.5,
) -> None:
    image = cv2.imread(str(image_path))
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(gray, 240, 255, cv2.THRESH_BINARY)
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    graphics_contours = []
    for cnt in contours:
        area = cv2.contourArea(cnt)
        x, y, w, h = cv2.boundingRect(cnt)
        aspect_ratio = max(w, h) / min(w, h) if min(w, h) > 0 else 0
        if area > min_area and aspect_ratio < aspect_ratio_threshold:
            graphics_contours.append(cnt)
    output = np.zeros_like(image)
    cv2.drawContours(output, graphics_contours, -1, (255, 255, 255), thickness=cv2.FILLED)
    cv2.imwrite(str(output_path), output)


def calculate_highlight_right_edge_distance(image_path: Path) -> int:
    image = cv2.imread(str(image_path), cv2.IMREAD_GRAYSCALE)
    _, thresh = cv2.threshold(image, 200, 255, cv2.THRESH_BINARY)
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_NONE)
    max_x = 0
    for contour in contours:
        x, y, w, h = cv2.boundingRect(contour)
        max_x = max(max_x, x + w)
    if max_x <= 0:
        raise RuntimeError("No captcha highlight detected.")
    return max_x


def collect_cookie_key(
    username: str,
    password: str,
    captcha_folder: Path,
    captcha_name: str,
    login_url: str,
) -> str:
    driver = webdriver.Firefox()
    try:
        driver.get(login_url)
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "tlxkh"))).clear()
        driver.find_element(By.ID, "tlxkh").send_keys(username)
        driver.find_element(By.ID, "tusername").clear()
        driver.find_element(By.ID, "tusername").send_keys(username)
        driver.find_element(By.ID, "tpass").clear()
        driver.find_element(By.ID, "tpass").send_keys(password)
        driver.find_element(By.ID, "yzm").click()

        image_src = ""
        for _ in range(6):
            image_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "backImg")))
            image_src = image_element.get_attribute("src")
            if image_src:
                break
            driver.find_element(By.CLASS_NAME, "verify-refresh").click()
        if not image_src:
            raise RuntimeError("Cannot fetch captcha image URL.")

        captcha_path = ensure_dir(captcha_folder) / captcha_name
        urllib.request.urlretrieve(image_src, str(captcha_path))
        keep_brightest_areas(captcha_path, captcha_path)
        remove_white_text_keep_graphics(captcha_path, captcha_path)
        distance = calculate_highlight_right_edge_distance(captcha_path)
        LOGGER.info("Captcha drag distance: %s", distance)

        block = driver.find_element(By.CLASS_NAME, "verify-move-block")
        ActionChains(driver).click_and_hold(block).move_by_offset(distance + 3, 0).release().perform()
        WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, "/html/body/form/div[3]/aside/section/ul/li[2]/a"))
        )
        cookie_key = ";".join(f"{c['name']}={c['value']}" for c in driver.get_cookies())
        return cookie_key
    finally:
        driver.quit()


def download_report(
    cookie_key: str,
    start_date: str,
    end_date: str,
    target_folder: Path,
    file_name: str,
    proxies: Dict[str, str] | None,
) -> Path:
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "gys.yndzyf.com",
        "Cookie": cookie_key,
        "Origin": "http://gys.yndzyf.com",
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
    }
    payload = {
        "action": "query",
        "pagename": "销售流向查询-分级",
        "edits": json.dumps(
            {
                "edit1": "当前数据",
                "edit2": "二级",
                "edit3": "云南白药大药房",
                "edit4": start_date,
                "edit5": end_date,
                "edit6": "",
                "edit7": "",
                "edit8": "",
                "edit9": "",
                "edit10": "",
                "edit11": "销售单位名称",
            }
        ),
    }
    response = requests.post(
        "http://gys.yndzyf.com/commonHandler.ashx",
        headers=headers,
        data=payload,
        proxies=proxies,
        timeout=60,
    )
    response.raise_for_status()
    query_data = response.json().get("queryData", "[]")
    df = pd.DataFrame(json.loads(query_data))
    output_path = ensure_dir(target_folder) / file_name
    df.to_excel(output_path, index=False)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Interactive-auth decoupled collector.")
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--target-folder", required=True)
    parser.add_argument("--file-name", default="interactive_auth_export.xlsx")
    parser.add_argument("--captcha-folder", default="./tmp")
    parser.add_argument("--captcha-name", default="captcha.png")
    parser.add_argument("--proxy", default="")
    parser.add_argument("--login-url", default="http://gys.yndzyf.com/")
    args = parser.parse_args()

    proxies = {"http": args.proxy, "https": args.proxy} if args.proxy else None
    cookie_key = collect_cookie_key(
        username=args.username,
        password=args.password,
        captcha_folder=Path(args.captcha_folder),
        captcha_name=args.captcha_name,
        login_url=args.login_url,
    )
    output_path = download_report(
        cookie_key=cookie_key,
        start_date=args.start_date,
        end_date=args.end_date,
        target_folder=Path(args.target_folder),
        file_name=args.file_name,
        proxies=proxies,
    )
    LOGGER.info("Interactive-auth export completed: %s", output_path)


if __name__ == "__main__":
    main()