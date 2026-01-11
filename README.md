# Datasource Migrate - 架构迁移与统计优化项目

## 项目背景
本项目主要解决网络审计系统中海量日志的统计性能问题。通过将原有的 ES 定时任务聚合模式迁移至 ClickHouse，利用其物化视图技术实现数据的实时预聚合。

## 核心改进：从“定时任务”到“物化视图”
在旧版本中，系统通过 Java 定时任务从 ElasticSearch 抽取数据并在应用层或 ES 层进行聚合计算。
在新架构中：
1. **实时计算**：通过 ClickHouse 物化视图（如 `mv_web_behavior_stats`），在基础日志写入时自动触发预聚合。
2. **存储优化**：使用 `AggregatingMergeTree` 引擎存储聚合状态（AggregateFunction），大幅节省存储空间。
3. **极速查询**：查询时通过 `sumMerge`, `minMerge` 等函数直接从聚合表中获取结果，响应时间从秒级降至毫秒级。

## 技术栈
- **核心框架**: Spring Boot 3.x, MyBatis Plus
- **数据库**: ClickHouse (主库), MySQL (配置库)
- **分布式中间件**: Apache Dubbo (服务间调用)
- **数据转换**: ModelMapper, Fastjson2

## 模块说明
- `mapper/clickhouse`: 封装 ClickHouse 特有的 SQL 操作，支持动态聚合字段选择。
- `dubboImpl`: 提供对外的 Dubbo 服务，内部实现了双写校验逻辑（ES 与 ClickHouse 对比验证）。
- `sql/`: 包含所有物化视图、目标汇总表及 Mock 数据的 ClickHouse DDL 脚本。

## 统计维度
项目实现了以下 7 个核心维度的物化统计：
1. **上网行为统计** (`web_behavior_stats`): 网页访问、邮件、搜索次数。
2. **网站请求统计** (`web_site_req_stats`): 基于 URL 的访问频率。
3. **IPS 攻击类型统计** (`ips_type_stats`): 各类安全攻击维度的计数。
4. **恶意网站统计** (`malicious_stats`): 针对黑名单域名的访问分析。
5. **病毒传输统计** (`virus_stats`): 病毒名称及传输频率。
6. **应用流量统计** (`app_flow_stats`): 按应用协议分类的 Byte 级流量。
7. **IP 流量统计** (`ip_flow_stats`): 按资产 IP 分类的上行/下行流量。

## 部署注意
ClickHouse 需开启 `parallel_view_processing` 以优化物化视图处理性能。
