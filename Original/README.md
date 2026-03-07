# Chapter 4 Automation Apps

本目录对应第4章机制化代码资产，按“认证机制—下载执行—质量门控”组织。

## Files
- `common_runtime.py`：通用运行层（日志、代理、文件校验、历史文件清理）
- `session_auth_multiendpoint_collection.py`：会话复用多端点采集（算法1低交互分支）
- `interactive_auth_decoupled_collection.py`：高交互认证解耦采集（算法1高交互分支）
- `token_auth_acquisition.py`：令牌获取与持久化（算法2阶段1）
- `token_based_daily_collection.py`：令牌驱动日批下载与文件门控（算法2阶段2）
- `data_matching_pipeline.py`：标准化/映射/集成主流程（算法3核心入口）
- `data_matching_pipeline.config.template.json`：`data_matching_pipeline.py` 的配置模板
- `sku_approximate_mapping.ipynb`：SKU 近似匹配与映射策略实验
- `global_category_mapping_panel_data.ipynb`：面板数据品类映射
- `global_category_mapping_ecommerce_data.ipynb`：电商数据品类映射
- `brand_forecast_modeling.ipynb`：应用层建模示例
- `data_matching_pipeline.ipynb`：历史实现留档（归档用途，默认不作为主执行入口）

## Minimal CLI Examples
```bash
python session_auth_multiendpoint_collection.py \
  --output-folder "./out/session" \
  --username "$VENDOR_USER" \
  --password "$VENDOR_PASS" \
  --date-start "2026-02-01" \
  --date-end "2026-02-28"
```

```bash
python token_auth_acquisition.py \
  --output-token-file "./out/token/DDI_Token.txt"
python token_based_daily_collection.py \
  --token-file "./out/token/DDI_Token.txt" \
  --sellin-dir "./out/ddi/sellin" \
  --sellout-dir "./out/ddi/sellout" \
  --offtake-dir "./out/ddi/offtake"
```

```bash
cp data_matching_pipeline.config.template.json data_matching_pipeline.config.json
python data_matching_pipeline.py --config-file "./data_matching_pipeline.config.json"
```

## Pipeline Meaning (for paper review)
`data_matching_pipeline.py` 的处理语义是“先归一、再映射、后汇总”：
1. 按客户读取原始数据（支持单文件与目录批处理）；
2. 执行客户预处理规则（字段修正、筛选、门店/分部修补等）；
3. 统一标准字段并完成分部、城市、SKU、价格映射；
4. 输出客户级结果，并合并为跨客户集成数据集。

## Customer-Specific Transform Steps
`data_matching_pipeline.py` 支持在每个客户配置下声明：
- `preprocess_steps`：标准化前清洗
- `postprocess_steps`：标准化后补充处理

当前支持的 step type：
- `rename_columns`
- `filter_equal` / `filter_not_equal`
- `set_constant`
- `map_values`
- `split_take_first`
- `slice_str`
- `assign_by_contains`
- `merge_mapping`

## Security Notes
- 不要在代码中硬编码用户名、密码、代理凭据或内部路径。
- 提交到 GitHub 前，请确认脱敏并清理 notebook 输出中的敏感信息。
