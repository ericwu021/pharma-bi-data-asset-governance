# Multi-Source Heterogeneous Data Asset Governance

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
├── Original/
│   └── (raw source archive)
├── requirements.txt
└── README.md
```

## Module-to-Method Mapping (Chapter 4)

- `src/collection/session_auth_multiendpoint_collection.py`  
  Session-reuse multi-endpoint collection (low-interaction branch of Algorithm 1).

- `src/collection/interactive_auth_decoupled_collection.py`  
  Interactive authentication and transmission decoupling (high-interaction branch of Algorithm 1).

- `src/collection/token_auth_acquisition.py` + `src/collection/token_based_daily_collection.py`  
  Two-stage token-driven scheduling and daily extraction (Algorithm 2).

- `src/processing/data_matching_pipeline.py`  
  Configuration-driven normalization, mapping, and cross-source integration (Algorithm 3 core workflow).

- `notebooks/*.ipynb`  
  Experimental and application-level artifacts for SKU/category mapping and forecasting.

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

## Reproducibility and Governance Notes

- Avoid hard-coded credentials, proxy secrets, and internal network paths.
- Keep notebook outputs sanitized before publication.
- Preserve execution logs for batch-level success/failure traceability.
- Version tags should match thesis milestones (for example: `v1.0-paper`).

## License

MIT License.
