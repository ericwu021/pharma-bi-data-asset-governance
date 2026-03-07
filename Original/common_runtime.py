"""Shared runtime utilities for chapter-5 automation scripts."""

from __future__ import annotations

import datetime as dt
import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd


LOGGER_NAME = "chapter4_automation"


def get_logger(name: str = LOGGER_NAME, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def apply_proxy(proxy: Optional[str]) -> None:
    if not proxy:
        return
    os.environ["http_proxy"] = proxy
    os.environ["HTTP_PROXY"] = proxy
    os.environ["https_proxy"] = proxy
    os.environ["HTTPS_PROXY"] = proxy


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def remove_yesterday_files(
    folder: str | Path,
    file_prefix: str,
    logger: Optional[logging.Logger] = None,
) -> int:
    target_date = dt.date.today() - dt.timedelta(days=1)
    root = Path(folder)
    removed = 0
    if not root.exists():
        return 0

    for file_path in root.rglob("*"):
        if not file_path.is_file():
            continue
        if file_prefix not in file_path.name:
            continue
        modified = dt.date.fromtimestamp(file_path.stat().st_mtime)
        if modified != target_date:
            continue
        file_path.unlink(missing_ok=True)
        removed += 1
        if logger:
            logger.info("Removed historical file: %s", file_path)
    return removed


def validate_excel_nonempty(path: str | Path, skiprows: int = 0) -> bool:
    df = pd.read_excel(path, skiprows=skiprows)
    return df.shape[0] > 0


def save_binary_file(path: str | Path, content: bytes) -> None:
    p = Path(path)
    ensure_dir(p.parent)
    p.write_bytes(content)


def load_text(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8").strip()

