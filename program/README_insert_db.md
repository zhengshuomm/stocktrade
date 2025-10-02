# 数据库插入程序使用说明

## 功能概述

`insert_outliers_to_db.py` 是一个用于将异常检测结果插入到PostgreSQL数据库的程序，支持文件去重、批量插入和错误处理。

## 主要功能

1. **文件去重**: 通过 `processed_files` 表避免重复处理同一文件
2. **批量插入**: 高效插入大量数据到数据库
3. **数据验证**: 自动格式化浮点数精度和数据类型
4. **错误处理**: 完整的异常处理和日志记录
5. **状态跟踪**: 记录文件处理状态和历史

## 使用方法

### 基本用法

```bash
# 处理 data 文件夹
python3 program/insert_outliers_to_db.py --folder data

# 处理 priority_data 文件夹
python3 program/insert_outliers_to_db.py --folder priority_data

# 显示详细日志
python3 program/insert_outliers_to_db.py --folder data --verbose
```

### 测试连接

```bash
# 测试数据库连接和文件检测
python3 program/test_insert_db.py --folder data
python3 program/test_insert_db.py --folder priority_data
```

## 支持的数据类型

### Volume异常数据
- **源文件**: `{folder}/volume_outlier/volume_outlier_*.csv`
- **目标表**: `volume_outlier`
- **字段**: contractSymbol, strike, signal_type, volume_old, volume_new, amount_threshold 等

### OI异常数据
- **源文件**: `{folder}/outlier/*.csv`
- **目标表**: `oi_outlier`
- **字段**: contractSymbol, strike, oi_change, signal_type, openInterest_new, openInterest_old 等

## 数据库表结构

### processed_files 表
记录所有已处理的文件，避免重复处理：

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

## 程序特性

### 1. 智能去重
- **文件级去重**: 检查 `processed_files` 表，跳过已处理文件
- **数据级去重**: 使用 `contractSymbol + folder_name + create_time` 作为唯一标识

### 2. 数据格式化
- **浮点数精度**: 自动保留2位有效数字
- **时间戳**: 使用当前时间作为 `create_time`
- **空值处理**: 自动处理缺失数据

### 3. 错误处理
- **数据库连接**: 自动重试和错误恢复
- **文件读取**: 处理文件不存在或格式错误
- **数据插入**: 事务回滚和部分成功处理

### 4. 日志记录
- **详细日志**: 记录所有操作步骤和结果
- **错误日志**: 记录失败原因和堆栈信息
- **性能日志**: 记录处理时间和数据量

## 配置信息

### 数据库连接
```python
DB_CONFIG = {
    'host': 'ep-raspy-river-af178kn5-pooler.c-2.us-west-2.aws.neon.tech',
    'database': 'neondb',
    'user': 'neondb_owner',
    'password': 'npg_actGluWDr3d1',
    'port': 5432,
    'sslmode': 'require'
}
```

### 日志配置
- **日志文件**: `insert_outliers.log`
- **日志级别**: INFO (使用 --verbose 显示DEBUG)
- **输出**: 同时输出到文件和控制台

## 处理流程

1. **连接数据库**: 建立PostgreSQL连接
2. **检查文件**: 查找最新的CSV文件
3. **去重检查**: 查询 `processed_files` 表
4. **读取数据**: 解析CSV文件内容
5. **格式化数据**: 处理数据类型和精度
6. **批量插入**: 高效插入到数据库
7. **记录状态**: 更新 `processed_files` 表
8. **关闭连接**: 清理资源

## 错误处理

### 常见错误
1. **数据库连接失败**: 检查网络和凭据
2. **文件不存在**: 检查文件夹路径和文件模式
3. **数据格式错误**: 检查CSV文件格式
4. **插入失败**: 检查数据库表结构和约束

### 状态码
- `success`: 处理成功
- `failed`: 处理失败
- `partial`: 部分成功

## 性能优化

1. **批量插入**: 使用 `execute_values` 提高插入效率
2. **事务管理**: 确保数据一致性
3. **内存管理**: 分批处理大量数据
4. **连接池**: 复用数据库连接

## 依赖包

```bash
pip install pandas psycopg2-binary
```

## 注意事项

1. **数据库权限**: 确保有创建表和插入数据的权限
2. **文件权限**: 确保有读取CSV文件的权限
3. **磁盘空间**: 确保有足够的日志文件空间
4. **网络连接**: 确保数据库服务器可访问

## 故障排除

### 1. 连接失败
```bash
# 测试数据库连接
python3 -c "import psycopg2; print('PostgreSQL驱动正常')"
```

### 2. 文件未找到
```bash
# 检查文件结构
ls -la data/volume_outlier/
ls -la data/outlier/
```

### 3. 权限问题
```bash
# 检查文件权限
chmod 644 data/volume_outlier/*.csv
chmod 644 data/outlier/*.csv
```

## 更新日志

- **v1.0.0**: 初始版本，支持基本的文件插入功能
- **v1.1.0**: 添加文件去重机制
- **v1.2.0**: 优化数据格式化和错误处理
