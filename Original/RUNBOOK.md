# Chapter 4 Apps Runbook

本 runbook 将论文机制与代码执行顺序对齐，便于论文提交时复核。

## 0. Environment
- 安装依赖：`pip install -r requirements.txt`
- 推荐使用环境变量配置路径与凭据，避免硬编码。

## 1. 采集层（算法1/算法2）
1. 会话复用采集  
   - `session_auth_multiendpoint_collection.py`
2. 高交互认证解耦采集  
   - `interactive_auth_decoupled_collection.py`
3. 令牌驱动采集  
   - `token_auth_acquisition.py` -> `token_based_daily_collection.py`

## 2. 标准化与映射层（算法3）
1. `data_matching_pipeline.py`（主入口，配置驱动）
   - 先复制配置模板：`cp data_matching_pipeline.config.template.json data_matching_pipeline.config.json`
   - 再执行：`python data_matching_pipeline.py --config-file "./data_matching_pipeline.config.json"`
2. `sku_approximate_mapping.ipynb`
3. `global_category_mapping_panel_data.ipynb`
4. `global_category_mapping_ecommerce_data.ipynb`

## 3. 应用层（可选）
- `brand_forecast_modeling.ipynb`

## 4. 提交前检查
- [ ] 所有脚本不包含明文凭据和内部地址
- [ ] notebook 顶部保留“提交说明”和“参数区”单元
- [ ] 运行日志可复核（至少保留批次成功/失败汇总）
- [ ] 提交版本与论文版本标签一致（如 `v1.0-paper`）
