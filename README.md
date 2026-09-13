# Multi-Source Heterogeneous Data Asset Governance (Chapter 4)

This repository contains the implementation assets aligned with **Chapter 4** of the doctoral thesis on multi-source heterogeneous data asset governance.  
It organizes the codebase by governance mechanism rather than by script chronology, so the repository can support both academic review and engineering reuse.

## Research Context

The project addresses three governance challenges in multi-source data platforms:

1. **Availability instability** under heterogeneous authentication constraints.
2. **Comparability gaps** caused by inconsistent schemas, categories, and entity definitions.
3. **Usability uncertainty** in downstream analytics without quality gates and reproducible integration.

The implementation is organized into three layers:

- **Collection Layer**: session-based, interactive-auth decoupled, and token-driven acquisition.
- **Standardization & Mapping Layer**: rule-based normalization, mapping, and integrated export.
- **Application Layer**: modeling-ready notebooks for downstream analytics.

## Repository Structure

```text
.
├── 03_documentation/
│   └── chapter4_mapping.md
├── config/
│   └── data_matching_pipeline.config.template.json
├── docs/
│   └── RUNBOOK.md
├── notebooks/
│   ├── brand_forecast_modeling.ipynb
│   ├── data_matching_pipeline.ipynb
│   ├── global_category_mapping_ecommerce_data.ipynb
│   ├── global_category_mapping_panel_data.ipynb
│   └── sku_approximate_mapping.ipynb
├── src/
│   ├── collection/
│   │   ├── interactive_auth_decoupled_collection.py
│   │   ├── session_auth_multiendpoint_collection.py
│   │   ├── token_auth_acquisition.py
│   │   └── token_based_daily_collection.py
│   ├── processing/
│   │   └── data_matching_pipeline.py
│   └── runtime/
│       └── common_runtime.py
├── requirements.txt
└── README.md
```

## Module-to-Method Mapping (Chapter 4)

| Thesis mechanism | Section | Asset | File |
|---|---|---|---|
| Session reuse with on-write quality gate | 4.4.1 | A1 | `src/collection/session_auth_multiendpoint_collection.py` |
| Authentication / transfer decoupling | 4.4.2, Algorithm 4-1 | A2 | `src/collection/interactive_auth_decoupled_collection.py` |
| Token acquisition and token-driven daily batch | 4.4.3, Algorithm 4-2 | A3 | `src/collection/token_auth_acquisition.py`, `src/collection/token_based_daily_collection.py` |
| Config-driven standardization, day-level expansion, master-data mapping | 4.5, Algorithm 4-3 | A4 | `src/processing/data_matching_pipeline.py`, `config/data_matching_pipeline.config.template.json` |
| Approximate SKU matching and backfill | 4.5.3 | A5 | `notebooks/sku_approximate_mapping.ipynb` |
| Master-data enrichment and category alignment | 4.5.3 | A6 | `notebooks/global_category_mapping_*.ipynb` |
| Shared gate primitives (non-empty check, previous-day cleanup, logging, proxy) | 4.6, 4.7 | shared | `src/runtime/common_runtime.py` |

The statement-by-statement mapping between thesis text and code locations is in `03_documentation/chapter4_mapping.md`.

## Quick Start

### 1) Environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Collection Layer Examples

```bash
python src/collection/session_auth_multiendpoint_collection.py \
  --output-folder "./out/session" \
  --username "$VENDOR_USER" \
  --password "$VENDOR_PASS" \
  --date-start "2026-02-01" \
  --date-end "2026-02-28"
```

```bash
python src/collection/token_auth_acquisition.py \
  --output-token-file "./out/token/DDI_Token.txt"

python src/collection/token_based_daily_collection.py \
  --token-file "./out/token/DDI_Token.txt" \
  --sellin-dir "./out/ddi/sellin" \
  --sellout-dir "./out/ddi/sellout" \
  --offtake-dir "./out/ddi/offtake"
```

### 3) Standardization & Mapping

```bash
cp config/data_matching_pipeline.config.template.json config/data_matching_pipeline.config.json
python src/processing/data_matching_pipeline.py --config-file "./config/data_matching_pipeline.config.json"
```

For full execution order and reproducibility notes, see `docs/RUNBOOK.md`.
For chapter-level narrative-to-code traceability, see `03_documentation/chapter4_mapping.md`.

## Reproducibility and Governance Notes

- Avoid hard-coded credentials, proxy secrets, and internal network paths.
- Keep notebook outputs sanitized before publication.
- Preserve execution logs for batch-level success/failure traceability.
- Version tags should match thesis milestones (for example: `v1.0-paper`).

## License

Apache License 2.0 (`Apache-2.0`). See `LICENSE` for details.
