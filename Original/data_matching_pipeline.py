"""Chapter-5 standardization/mapping pipeline.

核心目标：
1) 读取多客户异构原始数据（单文件/目录批量）；
2) 执行客户级预处理（可配置规则）；
3) 进行统一字段标准化与映射（分部/城市/SKU/价格）；
4) 输出客户明细与全量集成结果。

脚本采用“代码稳定 + 配置驱动”模式：
- 代码只承载通用机制；
- 客户差异通过 JSON 配置表达，便于论文交付与复核。
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from common_runtime import ensure_dir, get_logger

LOGGER = get_logger("data_matching_pipeline")


@dataclass
class CustomerJob:
    """Customer-specific pipeline job definition.

    `name` 用于输出文件命名；`ka_name` 用于映射口径。
    二者可不同（例如历史口径兼容场景）。
    """
    name: str  # output/customer label
    ka_name: str  # KA used in standardization/mapping
    loader: str  # single | folder | folder_tianji
    source: str  # filename (single) or folder-name (folder/folder_tianji)
    target_columns: List[str]
    date_mode: int = 0  # 0: read from source date column; 1: use manual start_date
    sheet_name: str = ""
    skip_rows: int = 0
    output_suffix: str = ""
    expand_by_day: bool = False
    preprocess_steps: List[Dict[str, Any]] = field(default_factory=list)
    postprocess_steps: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class PipelineConfig:
    """Top-level runtime configuration."""
    year: str
    sub_folder: str
    export_tag: str
    days_count: int
    start_date: str
    date_low_limit: str
    date_high_limit: str
    raw_data_root: str
    output_root: str
    mapping_root: str
    customers: List[CustomerJob]


def _require_columns(df: pd.DataFrame, columns: List[str], step_type: str) -> None:
    """Validate columns required by a transform step."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(f"Step `{step_type}` missing columns: {missing}")


def apply_transform_steps(df: pd.DataFrame, steps: List[Dict[str, Any]], mapping_root: Path) -> pd.DataFrame:
    """Apply config-driven transforms before/after standardization.

    This keeps customer-specific logic in JSON instead of hard-coded branches.
    """
    out = df.copy()
    for idx, step in enumerate(steps):
        step_type = step.get("type", "")
        if not step_type:
            raise ValueError(f"Invalid transform step at index {idx}: missing `type`.")

        if step_type == "rename_columns":
            out = out.rename(columns=step["columns"])
            continue

        if step_type == "filter_equal":
            col = step["column"]
            _require_columns(out, [col], step_type)
            out = out.loc[out[col] == step["value"]].copy()
            continue

        if step_type == "filter_not_equal":
            col = step["column"]
            _require_columns(out, [col], step_type)
            out = out.loc[out[col] != step["value"]].copy()
            continue

        if step_type == "set_constant":
            col = step["column"]
            out[col] = step["value"]
            continue

        if step_type == "map_values":
            col = step["column"]
            _require_columns(out, [col], step_type)
            out[col] = out[col].replace(step["mapping"])
            continue

        if step_type == "split_take_first":
            col = step["column"]
            delimiter = step.get("delimiter", " ")
            target = step.get("target_column", col)
            _require_columns(out, [col], step_type)
            out[target] = out[col].astype(str).str.split(delimiter, expand=True).iloc[:, 0]
            continue

        if step_type == "slice_str":
            col = step["column"]
            target = step.get("target_column", col)
            start = step.get("start", None)
            end = step.get("end", None)
            _require_columns(out, [col], step_type)
            out[target] = out[col].astype(str).str.slice(start, end)
            continue

        if step_type == "assign_by_contains":
            source = step["source_column"]
            target = step["target_column"]
            rules = step.get("rules", [])
            default_value = step.get("default", None)
            _require_columns(out, [source], step_type)
            if target not in out.columns:
                out[target] = ""
            if default_value is not None:
                out[target] = default_value
            source_text = out[source].astype(str)
            for rule in rules:
                contains = rule["contains"]
                value = rule["value"]
                mask = source_text.str.contains(contains, na=False)
                out.loc[mask, target] = value
            continue

        if step_type == "merge_mapping":
            file_name = step["file"]
            left_on = step["left_on"]
            right_on = step.get("right_on", left_on)
            select_columns = step.get("select_columns", right_on)
            how = step.get("how", "left")
            mapping_df = pd.read_excel(mapping_root / file_name)
            mapping_df = mapping_df[select_columns].copy()
            out = out.merge(mapping_df, how=how, left_on=left_on, right_on=right_on)
            continue

        raise ValueError(f"Unsupported transform step type: {step_type}")
    return out


def load_pipeline_config(path: Path) -> PipelineConfig:
    """Load and normalize JSON config."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    customers = []
    for job in raw["customers"]:
        normalized = dict(job)
        normalized.setdefault("ka_name", normalized.get("name", ""))
        customers.append(CustomerJob(**normalized))
    return PipelineConfig(
        year=raw["year"],
        sub_folder=raw["sub_folder"],
        export_tag=raw["export_tag"],
        days_count=raw["days_count"],
        start_date=raw["start_date"],
        date_low_limit=raw["date_low_limit"],
        date_high_limit=raw["date_high_limit"],
        raw_data_root=raw["raw_data_root"],
        output_root=raw["output_root"],
        mapping_root=raw["mapping_root"],
        customers=customers,
    )


def load_mapping_tables(mapping_root: Path, price_year: str = "2022") -> Dict[str, pd.DataFrame]:
    """Load base mapping tables used by all customers."""
    branch = pd.read_excel(mapping_root / "Branch_Mapping.xlsx").iloc[:, :10]
    branch = branch.drop_duplicates(subset=["Offtake_分部"]).dropna(subset=["Offtake_分部"]).reset_index(drop=True)

    sku = pd.read_excel(mapping_root / "SKU_Mapping.xlsx", sheet_name="SKU")
    sku = sku[sku.Platform == "Retail"]
    sku = sku.drop_duplicates(subset=["Offtake_SKU"]).dropna(subset=["Offtake_SKU"])
    sku = sku[["Offtake_SKU", "品规", "品类", "品类2"]].reset_index(drop=True)

    price_col = f"考核价{price_year}"
    price = pd.read_excel(mapping_root / "SKU_Mapping.xlsx", sheet_name="CAT")[["品规", price_col]]
    price = price.drop_duplicates(subset=["品规"]).dropna(subset=["品规"]).reset_index(drop=True)

    province = pd.read_excel(mapping_root / "Province_Mapping.xlsx")
    province = province.drop_duplicates(subset=["城市"]).dropna(subset=["城市"]).reset_index(drop=True)

    return {"branch": branch, "sku": sku, "price": price, "province": province}


def read_customer_file(file_path: Path, sheet_name: str, skip_rows: int) -> pd.DataFrame:
    """Read a single Excel file with optional sheet/skiprows."""
    if sheet_name:
        return pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skip_rows)
    return pd.read_excel(file_path, skiprows=skip_rows)


def files_integration(folder: Path, sheet_name: str, skip_rows: int) -> pd.DataFrame:
    """Batch-read Excel files from a folder and vertically concatenate."""
    file_list = [
        f for f in folder.iterdir()
        if f.is_file() and f.suffix.lower() in {".xlsx", ".xls"} and "$" not in f.name
    ]
    if not file_list:
        return pd.DataFrame()
    df_list = [read_customer_file(f, sheet_name, skip_rows) for f in sorted(file_list)]
    return pd.concat(df_list, ignore_index=True).dropna(how="all").reset_index(drop=True)


def files_integration_tianji(folder: Path, sheet_name: str, skip_rows: int) -> pd.DataFrame:
    """Special folder loader with branch tagging for Tianji source files."""
    file_list = [
        f for f in folder.iterdir()
        if f.is_file() and f.suffix.lower() in {".xlsx", ".xls"} and "$" not in f.name
    ]
    if not file_list:
        return pd.DataFrame()
    frames: List[pd.DataFrame] = []
    for file_path in sorted(file_list):
        df = read_customer_file(file_path, sheet_name, skip_rows)
        df["Offtake_分部"] = "益生天济" if "益生天济" in file_path.name else "天济大药房"
        frames.append(df)
    return pd.concat(frames, ignore_index=True).dropna(how="all").reset_index(drop=True)


def date_expansion_by_day(df: pd.DataFrame, start_date: str, days_count: int) -> pd.DataFrame:
    """Expand period totals to daily rows with equal allocation."""
    base = df.copy()
    base["数量"] = base["数量"] / days_count
    base["Offtake"] = base["Offtake"] / days_count
    dates = pd.date_range(start=pd.to_datetime(start_date), periods=days_count, freq="D")
    return pd.concat([base.assign(销售日期=d) for d in dates], ignore_index=True)


def format_standardization(
    df: pd.DataFrame,
    ka_name: str,
    date_mode: int,
    date_input: str,
    target_cols: List[str],
    branch: pd.DataFrame,
    sku: pd.DataFrame,
    province: pd.DataFrame,
    price: pd.DataFrame,
    date_low: pd.Timestamp,
    date_high: pd.Timestamp,
) -> pd.DataFrame:
    """Standardize schema and execute branch/city/SKU/price mappings."""
    df = df.copy()
    if date_mode == 0:
        df.loc[:, target_cols[0]] = pd.to_datetime(df.loc[:, target_cols[0]]).dt.date
    else:
        df[target_cols[0]] = pd.to_datetime(date_input)

    if len(target_cols) > 6:
        df["Offtake_SKU"] = df[target_cols[4]].astype(str) + df[target_cols[5]].astype(str)
    else:
        df["Offtake_SKU"] = df[target_cols[4]].astype(str)

    df["KA"] = ka_name
    df = df[[target_cols[0], target_cols[1], target_cols[2], target_cols[3], "Offtake_SKU", target_cols[-1]]]
    df.columns = ["销售日期", "KA", "Offtake_分部", "门店", "Offtake_SKU", "数量"]
    df = df[(df["销售日期"] >= date_low) & (df["销售日期"] <= date_high)]

    branch_map = branch.copy()
    if ka_name != "DDI新增":
        branch_map = branch_map.loc[branch_map.KA == ka_name]
    if "KA" in branch_map.columns:
        branch_map = branch_map.drop(columns=["KA"])

    df = df.merge(branch_map.reset_index(drop=True), how="left", on="Offtake_分部")
    LOGGER.info("[%s] Non-mapped branch count: %s", ka_name, df["连锁分部名称"].isna().sum())
    df.dropna(subset=["连锁分部名称"], inplace=True)

    df = df.merge(province, how="left", on="城市")
    LOGGER.info("[%s] Non-mapped city count: %s", ka_name, df["省份"].isna().sum())
    df.loc[df["省份"].isna(), "省份"] = "未知"

    df = df.merge(sku, how="left", on="Offtake_SKU")
    LOGGER.info("[%s] Non-mapped SKU count: %s", ka_name, df["品规"].isna().sum())
    df.dropna(subset=["品规"], inplace=True)
    df.pop("Offtake_SKU")

    df = df.merge(price, how="left", on="品规")
    price_col = price.columns[-1]
    df["Offtake"] = df["数量"] * df[price_col]
    df.rename(columns={price_col: "考核价"}, inplace=True)
    return df


def load_customer_raw_df(job: CustomerJob, raw_root: Path) -> pd.DataFrame:
    """Dispatch loader strategy by customer job config."""
    if job.loader == "single":
        return read_customer_file(raw_root / job.source, job.sheet_name, job.skip_rows)
    if job.loader == "folder":
        return files_integration(raw_root / job.source, job.sheet_name, job.skip_rows)
    if job.loader == "folder_tianji":
        return files_integration_tianji(raw_root / job.source, job.sheet_name, job.skip_rows)
    raise ValueError(f"Unknown loader type: {job.loader}")


def run_pipeline(config: PipelineConfig) -> None:
    """Run end-to-end pipeline and export customer/integrated outputs."""
    raw_root = Path(config.raw_data_root) / config.year / config.sub_folder
    outputs = ensure_dir(Path(config.output_root) / config.year / config.sub_folder)
    final_root = ensure_dir(Path(config.output_root) / "deliverables")
    mapping_root = Path(config.mapping_root)

    mappings = load_mapping_tables(mapping_root)
    processed_frames: List[pd.DataFrame] = []

    for job in config.customers:
        LOGGER.info("Processing customer: %s (KA=%s)", job.name, job.ka_name)
        raw_df = load_customer_raw_df(job, raw_root)
        if raw_df.empty:
            LOGGER.warning("[%s] no source rows loaded; skip.", job.name)
            continue
        if job.preprocess_steps:
            raw_df = apply_transform_steps(raw_df, job.preprocess_steps, mapping_root)

        out = format_standardization(
            df=raw_df,
            ka_name=job.ka_name,
            date_mode=job.date_mode,
            date_input=config.start_date,
            target_cols=job.target_columns,
            branch=mappings["branch"],
            sku=mappings["sku"],
            province=mappings["province"],
            price=mappings["price"],
            date_low=pd.to_datetime(config.date_low_limit),
            date_high=pd.to_datetime(config.date_high_limit),
        )
        if job.expand_by_day:
            out = date_expansion_by_day(out, config.start_date, config.days_count)
        if job.postprocess_steps:
            out = apply_transform_steps(out, job.postprocess_steps, mapping_root)

        suffix = f"_{job.output_suffix}" if job.output_suffix else ""
        out_file = outputs / f"{job.name}{suffix}_{config.export_tag}.csv"
        out.to_csv(out_file, index=False, encoding="utf-8_sig")
        LOGGER.info("[%s] exported: %s", job.name, out_file)
        processed_frames.append(out)

    if processed_frames:
        integrated = pd.concat(processed_frames, ignore_index=True)
        integrated_file = final_root / f"Offtake_Integration_{config.export_tag}_{config.year}.csv"
        integrated.to_csv(integrated_file, index=False, encoding="utf-8_sig")
        LOGGER.info("Integrated output exported: %s", integrated_file)
    else:
        LOGGER.warning("No processed customer outputs were generated.")

    date_range = pd.DataFrame({"销售日期": pd.date_range(start="2017-01-01", end=pd.Timestamp.today().date())})
    date_range_file = mapping_root / "Date_Range.csv"
    date_range.iloc[:-1, :].to_csv(date_range_file, index=False, encoding="utf-8_sig")
    LOGGER.info("Date range exported: %s", date_range_file)


def parse_args() -> argparse.Namespace:
    """Parse CLI args."""
    parser = argparse.ArgumentParser(description="Run chapter-5 data matching pipeline (.py version).")
    parser.add_argument("--config-file", required=True, help="Path to JSON config file.")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    config = load_pipeline_config(Path(args.config_file))
    run_pipeline(config)


if __name__ == "__main__":
    main()
