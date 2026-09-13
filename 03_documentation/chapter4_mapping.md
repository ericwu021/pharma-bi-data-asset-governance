# 第 4 章《数据平台建设与多源资产治理》与代码的对应

本文档把论文第 4 章的机制、算法与表格逐项对应到本仓库的实现文件，供评审复核与复现。章节编号以论文 2026 年 9 月版为准。

## 1. 机制与文件

| 论文机制 | 章节 | 资产编号 | 实现文件 |
|---|---|---|---|
| 会话复用与落盘门控 | 4.4.1 | A1 | `src/collection/session_auth_multiendpoint_collection.py` |
| 认证与传输解耦 | 4.4.2，算法 4-1 | A2 | `src/collection/interactive_auth_decoupled_collection.py` |
| 令牌资产化与日批调度 | 4.4.3，算法 4-2 | A3 | `src/collection/token_auth_acquisition.py`、`src/collection/token_based_daily_collection.py` |
| 配置驱动的标准化算子、时间粒度一致化、主数据映射 | 4.5，算法 4-3 | A4 | `src/processing/data_matching_pipeline.py`、`config/data_matching_pipeline.config.template.json`、`notebooks/data_matching_pipeline.ipynb` |
| 增量近似匹配与回填 | 4.5.3 | A5 | `notebooks/sku_approximate_mapping.ipynb` |
| 主数据富化与类别对齐 | 4.5.3 | A6 | `notebooks/global_category_mapping_panel_data.ipynb`、`notebooks/global_category_mapping_ecommerce_data.ipynb` |
| 门控原语（非空校验、前日清理、目录保障、日志、代理） | 4.6、4.7 | 共用 | `src/runtime/common_runtime.py` |

## 2. 论文中的参数与代码位置

| 论文陈述 | 代码位置 |
|---|---|
| 登录请求 30 秒超时，下载请求 120 秒超时 | `session_auth_multiendpoint_collection.py`：`login()`、`download_endpoint()` |
| 四个报表端点共用一个会话 | `session_auth_multiendpoint_collection.py`：`ENDPOINTS`、`main()` |
| 落盘后非空校验，失败即删除并告警；通过后重写为统一格式；清理前一日同名文件 | `download_endpoint()` 调用 `validate_excel_nonempty()`、`remove_yesterday_files()` |
| 验证码图像经亮度阈值过滤与前景保留后估计滑块位移；认证后提取全部 Cookie | `interactive_auth_decoupled_collection.py`：`keep_brightest_areas()`、`remove_white_text_keep_graphics()`、`calculate_highlight_right_edge_distance()`、`collect_cookie_key()` |
| 20 秒内从跳转地址捕获令牌并持久化 | `token_auth_acquisition.py`：`get_token()`（`--timeout-seconds` 默认 20） |
| 进货、销售、纯销三类日报；空文件删除；前日清理 | `token_based_daily_collection.py`：`run_daily_batch()`、`download_and_gate()` |
| 作业项字段：数据源、加载方式、目标字段、日期模式、工作表、表头偏移、前后处理步骤 | `data_matching_pipeline.py`：`CustomerJob` |
| 表 4-1 的转换步骤类型 | `apply_transform_steps()`：`rename_columns`、`filter_equal`、`filter_not_equal`、`set_constant`、`map_values`、`split_take_first`、`slice_str`、`assign_by_contains`、`merge_mapping` |
| 周期数据按天均匀展开 | `date_expansion_by_day()` |
| 分部、城市、SKU、价格四步映射，每步记录未命中数量 | `format_standardization()`、`load_mapping_tables()` |
| 客户明细与全量集成结果输出 | `run_pipeline()` |

## 3. 评估指标的数据来源

- 文件有效率 $r_f$：采集脚本日志中的下载数与非空校验通过数。
- 映射覆盖率 $Cov_{sku}$：`format_standardization()` 记录的分部、城市、SKU 未命中数量。
- 治理前后对照（表 4-5）：来自平台运行记录，不由本仓库生成。

## 4. 版本冻结

论文送审前以标签 `v1.0-paper` 冻结本仓库，标签对应的提交哈希登记在论文附录。
