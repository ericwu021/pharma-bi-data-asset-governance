# Chapter 4 Code Mapping

This document maps Chapter 4 narrative blocks to concrete implementation artifacts in this repository.

## 1) Mechanism Group A: Heterogeneous Authentication and Collection

**Chapter intent**
- Stabilize data availability under heterogeneous authentication constraints.
- Decouple authentication complexity from reusable extraction workflows.

**Primary artifacts**
- `src/collection/`
- `src/runtime/common_runtime.py`

**Representative implementation evidence**
- Session-reuse branch (Algorithm 1, low-interaction):
  - `src/collection/session_auth_multiendpoint_collection.py`
- Interactive-auth decoupled branch (Algorithm 1, high-interaction):
  - `src/collection/interactive_auth_decoupled_collection.py`
- Token-driven branch (Algorithm 2):
  - `src/collection/token_auth_acquisition.py`
  - `src/collection/token_based_daily_collection.py`
- Runtime support for logging/proxy/file gating:
  - `src/runtime/common_runtime.py`

## 2) Mechanism Group B: Standardization, Mapping, and Integration

**Chapter intent**
- Resolve schema and semantic comparability gaps across source systems.
- Construct parameterized and reproducible transformation workflows.

**Primary artifact**
- `src/processing/data_matching_pipeline.py`

**Representative implementation evidence**
- Config-driven pipeline entrypoint:
  - `src/processing/data_matching_pipeline.py`
  - `config/data_matching_pipeline.config.template.json`
- Customer-specific preprocess/postprocess transformation chain:
  - `rename_columns`, `filter_equal`, `filter_not_equal`
  - `set_constant`, `map_values`, `split_take_first`
  - `slice_str`, `assign_by_contains`, `merge_mapping`
- Integrated export and date-range support:
  - cross-customer merged output and date-range export logic in `src/processing/data_matching_pipeline.py`

## 3) Mechanism Group C: Mapping Knowledge Enrichment and Application Readiness

**Chapter intent**
- Build iterative mapping knowledge assets for long-term governance.
- Support downstream analytics and business modeling with governed outputs.

**Primary artifacts**
- `notebooks/`

**Representative implementation evidence**
- SKU approximate mapping experiments:
  - `notebooks/sku_approximate_mapping.ipynb`
- Global category mapping for panel/e-commerce streams:
  - `notebooks/global_category_mapping_panel_data.ipynb`
  - `notebooks/global_category_mapping_ecommerce_data.ipynb`
- Application-layer forecasting example:
  - `notebooks/brand_forecast_modeling.ipynb`

## 4) Mechanism-Level Traceability

### M1 Availability Stabilization
- Multi-branch authentication and extraction strategies:
  - `src/collection/session_auth_multiendpoint_collection.py`
  - `src/collection/interactive_auth_decoupled_collection.py`
  - `src/collection/token_auth_acquisition.py`
  - `src/collection/token_based_daily_collection.py`

### M2 Standardization Consistency
- Parameterized normalization and transformation control:
  - `src/processing/data_matching_pipeline.py`
  - `config/data_matching_pipeline.config.template.json`

### M3 Mapping Feedback Loop
- Mapping enrichment and unresolved-entity handling workflows:
  - `src/processing/data_matching_pipeline.py`
  - `notebooks/sku_approximate_mapping.ipynb`
  - `notebooks/global_category_mapping_*.ipynb`

### M4 Governance Reproducibility
- Runbook and execution order documentation:
  - `docs/RUNBOOK.md`
  - `README.md`

## 5) Engineering and Governance Discussion Support

The repository layout supports Chapter 4 discussions on:
- authentication strategy branching under heterogeneous data interfaces,
- configurable governance operators for cross-source comparability,
- traceable integration outputs for asset-grade data delivery,
- progressive knowledge accumulation via mapping and notebook evidence.

This mapping is suitable for chapter figures, evidence tables, and GitHub-based reproducibility appendices.
