# VNPY + DolphinDB 高频数据处理系统

## 项目概述

这是一个基于 **VNPY** 和 **DolphinDB** 的高频金融数据处理学习项目，主要演示如何将实时交易数据高效采集、处理并持久化存储。

### 核心功能

- **实时数据采集**：通过 VNPY 的 CTP 网关获取期货 tick 数据
- **流式数据处理**：将数据转换为 DolphinDB 兼容格式并写入流表
- **多重订阅处理**：同时支持数据持久化和实时 K 线聚合
- **高性能存储**：利用 DolphinDB 的分区表实现海量数据高效存储
- **实时分析**：通过时间序列聚合器生成实时 K 线数据


## 功能特性

### 数据采集层
- ✅ 基于 VNPY CTP 网关的实时数据获取
- ✅ 智能数据过滤（按合约、交易所筛选）
- ✅ 纳秒级时间戳记录（接收时间戳）

### 数据处理层
- ✅ 自动数据格式转换（TickData → DolphinDB 格式）
- ✅ 时区处理（本地时间保持，不转换为 UTC）
- ✅ 数据验证和错误处理

### 数据存储层
- ✅ DolphinDB 流表实时写入
- ✅ 分区表自动持久化
- ✅ 高可用数据存储

### 数据分析层
- ✅ 实时 K 线聚合（可配置时间窗口）
- ✅ 延迟统计分析（接收延迟、队列延迟、写入延迟）
- ✅ 多维度性能监控

## 系统架构

```
VNPY CTP Gateway → TickCollectorEngine → TickDataConverter → StreamTableWriter → DolphinDB
     ↓                        ↓                        ↓                        ↓
  实时tick数据           采集+过滤               数据转换               流表写入
     ↓                        ↓                        ↓                        ↓
  CTP接口              事件驱动                 格式转换                同步写入
     ↓                        ↓                        ↓                        ↓
  期货交易所             内存处理                 类型映射                内存表
                                                                          ↓
                                                                  订阅处理
                                                                          ↓
                                               ┌─────────────────────┬─────────────────────┐
                                               │                     │                     │
                                               ▼                     ▼                     ▼
                                        分区表持久化              K线表聚合              延迟统计
                                        (历史数据)               (实时分析)              (性能监控)
```

### 核心组件

1. **TickCollectorEngine**: 数据采集引擎，监听 tick 事件
2. **TickDataConverter**: 数据转换器，处理格式转换
3. **StreamTableWriter**: 流表写入器，负责数据写入
4. **DolphinDB 会话管理**: 连接池和重连机制

## 技术栈

- **Python 3.7+**
- **VNPY**: 2.1.7
- **DolphinDB**: 高性能时序数据库
- **pandas**: 数据处理
- **CTP**: 期货交易接口

## 快速开始

### 环境准备

1. **安装依赖**
   ```bash
   pip install vnpy
   pip install dolphindb
   ```

2. **启动 DolphinDB**
   ```bash
   # 启动 DolphinDB 服务
   ./dolphindb -home /path/to/dolphindb
   ```

### 系统初始化

1. **创建分区表**
   ```bash
   # 在 DolphinDB 中执行
   exec("script/dos/creat_table.dos")
   ```

2. **创建流表和持久化订阅**
   ```bash
   # 在 DolphinDB 中执行
   exec("script/dos/creat_stream_table.dos")
   ```

3. **（可选）添加 K 线聚合功能**
   ```bash
   # 在 DolphinDB 中执行
   exec("script/dos/get_kline.dos")
   ```

### 启动数据采集

```bash
# 启动 VNPY 数据采集程序
python run.py
```

## 使用指南

### 基本操作流程

1. **数据采集启动**
   ```bash
   python run.py
   ```
   连接 CTP 网关（开发过程使用snownow），开始采集 tick 数据

2. **实时监控**
   ```bash
   # 检查流表状态
   select count(*) from tick_stream

   # 检查分区表数据
   select count(*) from loadTable("dfs://vnpy_futures", "tick_data")
   ```

3. **K线数据查询**
   ```bash
   # 查看实时K线
   select * from OHLC order by datetime desc limit 10
   ```

### 配置说明

#### 数据过滤配置
在 `run.py` 中可以配置：
```python
filter_config = FilterConfig(
    symbols={"IM2603"},  # 监控的合约
    exchanges={Exchange.CFFEX}  # 监控的交易所
)
```

#### 性能参数
- **流表批处理大小**: `batchSize=10`
- **持久化间隔**: `throttle=0.05`
- **K线时间窗口**: `windowSize=60*1000` (毫秒)

### 性能监控

#### 延迟统计查询
```sql
-- 详细延迟分析
select
    symbol,
    datetime,
    (long(receive_timestamp / 1000000) - long(nanotimestamp(datetime) / 1000000)) as receive_delay_ms,
    (long(write_start_timestamp / 1000000) - long(receive_timestamp / 1000000)) as queue_delay_ms,
    (long(write_end_timestamp / 1000000) - long(write_start_timestamp / 1000000)) as write_delay_ms,
    (long(write_end_timestamp / 1000000) - long(receive_timestamp / 1000000)) as total_delay_ms
from tick_stream
where symbol='IM2603'
order by datetime desc
limit 20
```

#### 聚合性能统计
```sql
-- 性能统计汇总
select
    symbol,
    count(*) as total_count,
    avg(long(receive_timestamp / 1000000) - long(nanotimestamp(datetime) / 1000000)) as avg_receive_delay_ms,
    avg(long(write_start_timestamp / 1000000) - long(receive_timestamp / 1000000)) as avg_queue_delay_ms,
    avg(long(write_end_timestamp / 1000000) - long(write_start_timestamp / 1000000)) as avg_write_delay_ms,
    avg(long(write_end_timestamp / 1000000) - long(receive_timestamp / 1000000)) as avg_total_delay_ms
from tick_stream
where symbol='IM2603'
group by symbol
```

## 项目结构

```
imba-vnpy/
├── app/
│   └── dolphindb_recorder.py      # 旧版实现（已废弃）
├── myvnpy/
│   └── app/
│       └── data_recorder/
│           ├── tick_collector_engine.py    # 数据采集引擎
│           ├── tick_data_converter.py      # 数据转换器
│           ├── stream_table_writer.py      # 流表写入器
│           └── dolphindb_session.py        # DDB会话管理
├── script/
│   ├── dos/                           # DolphinDB 脚本
│   │   ├── creat_table.dos           # 分区表创建
│   │   ├── creat_stream_table.dos    # 流表和持久化订阅创建
│   │   ├── get_kline.dos             # K线聚合功能
│   │   ├── cacul_lag.dos             # 延迟详细查询
│   │   ├── cacul_lag_simple.dos      # 延迟简化查询
│   │   └── cacul_lag_stats.dos       # 延迟统计聚合
│   └── python/                       # Python 辅助脚本
├── run.py                            # 主程序入口
├── test.py                           # 测试脚本
└── README.md                         # 项目文档
```

## 核心实现原理

### 时间戳处理
- **接收时间戳**: 数据到达系统时的纳秒级时间戳
- **写入开始时间戳**: 开始写入流表时的纳秒级时间戳
- **写入结束时间戳**: 完成写入后的纳秒级时间戳
- **业务时间戳**: tick 数据本身的 datetime 字段

### 数据流时序
```
Tick到达 → 记录接收时间戳 → 数据转换 → 记录写入开始时间戳 → 写入流表 → 记录写入结束时间戳 → 触发订阅处理
```

### 解耦设计
- **采集层**: 只负责数据获取和过滤
- **转换层**: 只负责数据格式转换
- **写入层**: 只负责数据持久化
- **分析层**: 只负责数据分析和聚合

## 注意事项

### 性能优化
- 流表内存预分配 100 万行
- 批处理大小设置为 10（平衡实时性和性能）
- 持久化节流时间 0.05 秒

### 数据一致性
- 使用事务确保数据完整性
- 异常处理防止数据丢失
- 重连机制保证连接稳定性

### 监控告警
- 内置延迟监控
- 错误日志记录
- 性能指标统计

## 常见问题

### Q: 如何修改监控的合约？
A: 在 `run.py` 中的 `FilterConfig` 中修改 `symbols` 参数

### Q: 如何调整 K 线时间窗口？
A: 在 `get_kline.dos` 中修改 `windowSize` 参数（毫秒单位）

### Q: 如何查看系统状态？
A: 使用 DolphinDB 的 `getStreamingStat()` 函数

### Q: 数据存储在哪里？
A: 分区表存储在 `dfs://vnpy_futures/tick_data`

## 下一步计划

基于当前系统架构，我们规划了三个重要的发展方向：

### 1. 实时K线图表动态输出 🎯
**目标**: 构建实时可视化界面，动态展示K线图表
- **技术方案**: Vue.js + WebSocket + Chart.js 或 ECharts
- **数据源**: 订阅 DolphinDB 的 OHLC 表实时更新
- **功能特性**:
  - 多时间周期K线切换 (1min, 5min, 15min, 1hour)
  - 实时价格更新和成交量显示
  - 技术指标叠加 (MA, RSI, MACD等)
  - 多合约同时监控

### 2. 流批一体因子计算 🧮
**目标**: 实现实时和离线因子计算的无缝集成
- **技术方案**: DolphinDB 流批一体计算引擎
- **核心功能**:
  - **实时因子**: 基于流数据计算动量、波动率、量价关系等指标
  - **历史因子**: 批量计算技术指标和基本面因子
  - **因子存储**: 结构化存储和管理计算结果
  - **因子验证**: 实时验证因子有效性和稳定性

### 3. 交易信号MQ推送 📡
**目标**: 构建高可靠的交易信号分发系统
- **技术方案**: RabbitMQ / Kafka + 规则引擎
- **系统架构**:
  - **信号生成**: 基于因子计算结果生成交易信号
  - **信号过滤**: 多层过滤机制确保信号质量
  - **消息队列**: 异步可靠的分发机制
  - **接收端**: 支持多客户端同时接收信号
- **安全特性**:
  - 信号延迟监控
  - 异常信号拦截
  - 风控规则集成

### 实施路线图

```
Phase 1: 实时K线图表 (1-2周)
├── 设计前端界面架构
├── 实现WebSocket数据推送
├── 集成图表组件
└── 性能优化和测试

Phase 2: 流批一体因子 (1-2周)
├── 设计因子计算框架
├── 实现实时因子计算
├── 构建因子库管理
└── 因子回测验证

Phase 3: 交易信号MQ (1-2周)
├── 搭建消息队列架构
├── 实现信号生成引擎
├── 构建信号分发系统
└── 集成风控规则
```

### 预期收益

- **可视化能力**: 直观的行情监控和分析界面
- **量化能力**: 强大的因子计算和信号生成能力
- **系统扩展性**: 支持更多交易策略和风险管理功能

## 许可证

本项目仅用于学习和研究目的。