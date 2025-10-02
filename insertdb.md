# 数据库插入程序规范

## 数据库连接信息

**PostgreSQL Endpoint:**
```bash
psql 'postgresql://neondb_owner:npg_actGluWDr3d1@ep-raspy-river-af178kn5-pooler.c-2.us-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
```

## 数据库表结构

### 1. volume_outlier 表
**用途**: 存储成交量异常数据

**字段列表**:
```sql
contractSymbol, strike, signal_type, folder_name, option_type, 
volume_old, volume_new, amount_threshold, amount_to_market_cap, 
openInterest_new, expiry_date, lastPrice_new, lastPrice_old, 
volume, symbol, 
股票价格(new), 股票价格(old), 股票价格(new open), 股票价格(new high), 股票价格(new low), 
create_time
```

### 2. oi_outlier 表
**用途**: 存储持仓量异常数据

**字段列表**:
```sql
contractSymbol, strike, oi_change, signal_type, folder_name, 
option_type, openInterest_new, openInterest_old, amount_threshold, 
amount_to_market_cap, expiry_date, lastPrice_new, lastPrice_old, 
volume, symbol, 
股票价格(new), 股票价格(old), 股票价格(new open), 股票价格(new high), 股票价格(new low), 
create_time
```

## 程序功能要求

### 数据源
- **Volume异常**: `{folder}/volume_outlier/` 目录下最新的CSV文件
- **OI异常**: `{folder}/outlier/` 目录下最新的CSV文件
- **支持文件夹**: `data` 或 `priority_data`

### 数据去重机制
- **唯一性约束**: 同一CSV文件中的同一条数据在数据库中只能存在一次
- **实现方式**: 使用 `contractSymbol + folder_name + create_time` 作为唯一标识
- **重复插入**: 如果尝试插入重复数据，程序应跳过或更新

### 数据格式要求
- **浮点数精度**: 所有float类型字段保留2位有效数字
  - 示例: `0.00001234` → `0.000012`
- **时间字段**: `create_time` 使用当前时间戳
- **文件夹标识**: `folder_name` 记录数据来源文件夹名称

### 程序接口
```python
def insert_outliers_to_db(folder_name: str):
    """
    将指定文件夹中的异常数据插入到数据库
    
    Args:
        folder_name: 数据文件夹名称 ('data' 或 'priority_data')
    """
    pass
```

## 实现注意事项

1. **错误处理**: 数据库连接失败、CSV文件不存在等异常情况
2. **事务管理**: 确保数据插入的原子性
3. **日志记录**: 记录插入成功/失败的详细信息
4. **性能优化**: 批量插入提高效率
5. **数据验证**: 确保字段类型和格式正确
