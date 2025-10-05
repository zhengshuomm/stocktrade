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
last_day_close_price, create_time
```

**新增字段说明**:
- `last_day_close_price` (DECIMAL): 前一天最后一个时间戳的股票收盘价，用于计算价格变化

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

### 3. processed_files 表
**用途**: 记录所有已处理过的文件，避免重复处理

**字段列表**:
```sql
id (主键), folder_name, csv_filename, file_type, processed_time, 
file_size, row_count, status
```

**字段说明**:
- `id`: 自增主键
- `folder_name`: 数据文件夹名称 ('data' 或 'priority_data')
- `csv_filename`: CSV文件名 (如 'volume_outlier_20251002-1543.csv')
- `file_type`: 文件类型 ('volume_outlier' 或 'oi_outlier')
- `processed_time`: 处理时间戳
- `file_size`: 文件大小 (字节)
- `row_count`: 处理的行数
- `status`: 处理状态 ('success', 'failed', 'partial')

**建表SQL**:
```sql
CREATE TABLE processed_files (
    id SERIAL PRIMARY KEY,
    folder_name VARCHAR(50) NOT NULL,
    csv_filename VARCHAR(255) NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_size BIGINT,
    row_count INTEGER,
    status VARCHAR(20) DEFAULT 'success',
    UNIQUE(folder_name, csv_filename)
);
```

## 程序功能要求

### 数据源
- **Volume异常**: `{folder}/volume_outlier/` 目录下最新的CSV文件
- **OI异常**: `{folder}/outlier/` 目录下最新的CSV文件
- **支持文件夹**: `data` 或 `priority_data`

### 数据去重机制
- **文件级去重**: 通过 `processed_files` 表记录已处理的文件，避免重复处理同一文件
- **数据级去重**: 同一CSV文件中的同一条数据在数据库中只能存在一次
- **实现方式**: 
  - 文件去重：检查 `processed_files` 表中是否存在 `folder_name + csv_filename` 组合
  - 数据去重：使用 `contractSymbol + folder_name + create_time` 作为唯一标识
- **重复处理**: 如果文件已处理过，程序应跳过该文件

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
    
    处理流程:
    1. 检查 processed_files 表，跳过已处理的文件
    2. 读取最新的 volume_outlier 和 oi_outlier CSV文件
    3. 验证数据格式和完整性
    4. 批量插入到对应的数据库表
    5. 记录处理结果到 processed_files 表
    """
    pass
```

## 实现注意事项

1. **错误处理**: 数据库连接失败、CSV文件不存在等异常情况
2. **事务管理**: 确保数据插入的原子性
3. **日志记录**: 记录插入成功/失败的详细信息
4. **性能优化**: 批量插入提高效率
5. **数据验证**: 确保字段类型和格式正确
6. **文件去重**: 实现前先检查 `processed_files` 表，避免重复处理
7. **状态跟踪**: 记录文件处理状态，支持部分成功的情况
8. **文件完整性**: 验证文件大小和行数，确保文件未被截断
