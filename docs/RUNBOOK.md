# Chapter 4 Runbook (Execution Order)

This runbook aligns the thesis mechanisms with the practical execution sequence for reproducibility.

## 0. Environment Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Use environment variables for credentials and paths whenever possible.

## 1. Collection Layer (Algorithm 1 / Algorithm 2)

### 1.1 Session-Reuse Collection

```bash
python src/collection/session_auth_multiendpoint_collection.py \
  --output-folder "./out/session" \
  --username "$VENDOR_USER" \
  --password "$VENDOR_PASS"
```

### 1.2 Interactive-Auth Decoupled Collection

```bash
python src/collection/interactive_auth_decoupled_collection.py \
  --username "$VENDOR_USER" \
  --password "$VENDOR_PASS" \
  --start-date "2026-02-01" \
  --end-date "2026-02-28" \
  --target-folder "./out/interactive"
```

### 1.3 Token-Driven Collection

```bash
python src/collection/token_auth_acquisition.py \
  --output-token-file "./out/token/DDI_Token.txt"

python src/collection/token_based_daily_collection.py \
  --token-file "./out/token/DDI_Token.txt" \
  --sellin-dir "./out/ddi/sellin" \
  --sellout-dir "./out/ddi/sellout" \
  --offtake-dir "./out/ddi/offtake"
```

## 2. Standardization and Mapping Layer (Algorithm 3)

```bash
cp config/data_matching_pipeline.config.template.json config/data_matching_pipeline.config.json
python src/processing/data_matching_pipeline.py --config-file "./config/data_matching_pipeline.config.json"
```

Recommended supplementary notebooks:

- `notebooks/sku_approximate_mapping.ipynb`
- `notebooks/global_category_mapping_panel_data.ipynb`
- `notebooks/global_category_mapping_ecommerce_data.ipynb`

## 3. Application Layer (Optional)

- `notebooks/brand_forecast_modeling.ipynb`

## 4. Pre-Submission Checklist

- [ ] No plaintext credentials or internal URLs in scripts.
- [ ] Notebook outputs are sanitized.
- [ ] Runtime logs preserve batch-level traceability.
- [ ] Repository tag is aligned with thesis versioning.
