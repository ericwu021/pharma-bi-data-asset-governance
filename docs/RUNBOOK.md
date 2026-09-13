# 运行手册（第 4 章复现顺序）

## 0. 环境

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

凭据、代理与路径一律通过命令行参数或环境变量传入，仓库中不含任何真实凭据。

## 1. 采集层

### 1.1 会话复用采集（A1）

```bash
python src/collection/session_auth_multiendpoint_collection.py \
  --output-folder "./out/session" \
  --username "$VENDOR_USER" \
  --password "$VENDOR_PASS" \
  --date-start "2026-02-01" \
  --date-end "2026-02-28"
```

### 1.2 认证与传输解耦采集（A2，算法 4-1）

```bash
python src/collection/interactive_auth_decoupled_collection.py \
  --username "$VENDOR_USER" \
  --password "$VENDOR_PASS" \
  --start-date "2026-02-01" \
  --end-date "2026-02-28" \
  --target-folder "./out/interactive"
```

### 1.3 令牌驱动日批采集（A3，算法 4-2）

```bash
python src/collection/token_auth_acquisition.py \
  --output-token-file "./out/token/DDI_Token.txt"

python src/collection/token_based_daily_collection.py \
  --token-file "./out/token/DDI_Token.txt" \
  --sellin-dir "./out/ddi/sellin" \
  --sellout-dir "./out/ddi/sellout" \
  --offtake-dir "./out/ddi/offtake"
```

## 2. 标准化、映射与门控（A4，算法 4-3）

```bash
cp config/data_matching_pipeline.config.template.json config/data_matching_pipeline.config.json
python src/processing/data_matching_pipeline.py --config-file "./config/data_matching_pipeline.config.json"
```

映射表（`Branch_Mapping.xlsx`、`SKU_Mapping.xlsx`、`Province_Mapping.xlsx`）放在配置中的 `mapping_root` 目录；运行日志中每一步映射的未命中数量即论文 4.6 节覆盖率指标的来源。

## 3. 近似匹配与主数据富化（A5、A6）

- `notebooks/sku_approximate_mapping.ipynb`
- `notebooks/global_category_mapping_panel_data.ipynb`
- `notebooks/global_category_mapping_ecommerce_data.ipynb`

## 4. 应用层（可选）

- `notebooks/brand_forecast_modeling.ipynb`

## 5. 送审前检查

- [ ] 脚本与配置中无明文凭据、内部地址。
- [ ] Notebook 输出已清理。
- [ ] 运行日志保留批次级成功与失败记录。
- [ ] 仓库标签与论文版本一致。
