# 股票期权异常检测系统

这是一个基于 Python 的股票期权异常检测系统，能够自动扫描股票期权数据并检测异常交易模式。

## 功能特性

### 数据扫描
- **多线程股票期权数据扫描** (`scan_stock_30min.py`)
  - 支持双线程并发下载
  - 自动获取期权链和股票价格数据
  - 支持执行价格偏差过滤

### 异常检测
- **成交量异常检测** (`program/find_outliers_by_volume.py`)
  - 基于成交量变化检测异常
  - 支持跨日数据处理
  - 市值比例过滤
  - 8种异常信号类型判断

- **持仓量异常检测** (`program/find_outliers_by_oi.py`)
  - 基于持仓量变化检测异常
  - 支持大额交易特殊处理
  - 金额门槛过滤

### 通知系统
- **Discord 通知** (`program/discord_outlier_sender.py`)
  - 自动发送异常检测结果到 Discord
  - 支持嵌入消息和简单文本格式
  - 按金额分档着色显示

## 自动化运行

系统通过 GitHub Actions 实现自动化运行：

### 数据扫描时间表（美国西部时间）
- **周一到周五**：5:30, 6:30, 7:30, 8:30, 9:30, 10:30, 11:30, 13:30
- 运行 `scan_stock_30min.py`

### 异常检测时间表（美国西部时间）
- **持仓量异常检测**：每天 6:00, 7:00
- **成交量异常检测**：每天 7:00, 8:00, 9:00, 10:00, 11:00, 12:00, 14:00

## 安装和使用

### 环境要求
- Python 3.9+
- 依赖包见 `requirements.txt`

### 安装依赖
```bash
pip install -r requirements.txt
```

### 手动运行

#### 扫描股票期权数据
```bash
python scan_stock_30min.py --max-stocks 100 --delay 1.0
```

#### 检测成交量异常
```bash
cd program
python find_outliers_by_volume.py
```

#### 检测持仓量异常
```bash
cd program
python find_outliers_by_oi.py
```

#### 发送 Discord 通知
```bash
cd program
python discord_outlier_sender.py
```

## 配置说明

### 参数配置
- `MIN_VOLUME`: 最小成交量阈值 (默认: 3000)
- `MIN_VOLUME_INCREASE_PCT`: 最小成交量增幅 (默认: 30%)
- `MIN_AMOUNT_THRESHOLD`: 最小金额门槛 (默认: 200万)
- `STOCK_CHANGE_THRESHOLD`: 股票价格变化阈值 (默认: 1%)
- `OPTION_CHANGE_THRESHOLD`: 期权价格变化阈值 (默认: 5%)

### 文件结构
```
stocktrade/
├── option_data/          # 期权数据文件
├── stock_price/          # 股票价格数据文件
├── stock_symbol/         # 股票代码和市值数据
├── volume_outlier/       # 成交量异常检测结果
├── outlier/              # 持仓量异常检测结果
├── program/              # 核心程序文件
├── .github/workflows/    # GitHub Actions 配置
└── requirements.txt      # 依赖包列表
```

## 注意事项

1. **时区调整**：GitHub Actions 使用 UTC 时间，需要根据夏令时调整 cron 表达式
2. **API 限制**：注意 Yahoo Finance API 的请求频率限制
3. **数据存储**：系统会自动提交新数据到 Git 仓库
4. **错误处理**：所有脚本都包含完整的错误处理机制

## 贡献

欢迎提交 Issue 和 Pull Request 来改进这个项目。

## 许可证

MIT License
