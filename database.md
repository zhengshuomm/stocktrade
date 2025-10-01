# 股票交易模拟系统数据库设计

## 数据库连接信息
**PostgreSQL Endpoint:**
```
psql 'postgresql://neondb_owner:npg_actGluWDr3d1@ep-raspy-river-af178kn5-pooler.c-2.us-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
```

## 程序要求
创建一个名为 `trade_stock.py` 的程序，放置在 `program/` 目录下，实现基于异常检测数据的股票交易模拟系统。

## 数据库表设计

### 1. User 表
存储用户账户信息：
- `cash` (DECIMAL): 现金余额，初始值 100,000
- `stock` (DECIMAL): 持有股票总价值，初始值 0
- `total_value` (DECIMAL): 总资产 = cash + stock

### 2. Transaction_History 表
存储所有交易记录（数据永不删除）：
- `transaction_id` (SERIAL PRIMARY KEY): 交易ID
- `symbol` (VARCHAR): 股票代码
- `buy_price` (DECIMAL): 买入价格
- `sell_price` (DECIMAL): 卖出价格，初始值 0
- `current_price` (DECIMAL): 当前价格
- `number_shares` (INTEGER): 持有股数
- `amount` (DECIMAL): 当前价值 = current_price × number_shares
- `gain` (DECIMAL): 盈亏 = (current_price - buy_price) × number_shares (持有中) 或 (sell_price - buy_price) × number_shares (已卖出)
- `is_hold` (BOOLEAN): 是否持有，true=持有，false=已卖出
- `buy_date` (TIMESTAMP): 买入时间
- `sell_date` (TIMESTAMP): 卖出时间，初始值 NULL

**约束条件：**
- 每个 symbol 在 is_hold=true 时只能有一条记录（UNIQUE约束）
- 卖出后 current_price 不再更新
- 只有 is_hold=false 时 sell_price 才不为 0

## 交易逻辑

### 买入条件
1. **数据源**: 读取 `data/outlier/` 和 `data/volume_outlier/` 中最新的异常检测文件
2. **时效性**: 文件时间戳与当前时间差 ≤ 5分钟
3. **信号要求**: 
   - 按 symbol 分组统计看涨/看跌数量
   - 必须有 ≥ 2个看涨信号
   - 不能有任何看跌信号
4. **买入金额**: total_value / 10（前提：cash ≥ total_value / 10）

### 卖出条件
对持有的每只股票，根据最新异常检测数据：

1. **同时包含看涨和看跌**:
   - 看涨数量 > 看跌数量 → 继续持有
   - 看跌数量 ≥ 3 → 卖出

2. **只包含看涨** → 继续持有

3. **只包含看跌** → 卖出

4. **不包含该股票**:
   - 持有时间 > 24小时 → 卖出
   - 持有时间 ≤ 24小时 → 继续持有

### 交易规则
- **全量交易**: 每次卖出都是全部股数，不部分卖出
- **实时更新**: 持有期间持续更新 current_price、amount、gain
- **数据一致性**: user.stock = SUM(transaction_history.amount WHERE is_hold=true)

## 程序架构
- **类型**: Cron Job 定时任务
- **频率**: 根据异常检测文件更新频率执行
- **错误处理**: 数据库连接失败、文件读取错误等异常情况
- **日志记录**: 所有交易操作和决策过程