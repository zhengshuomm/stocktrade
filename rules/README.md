# 期权异常信号分类规则

这个目录包含了所有用于分类期权异常信号的规则定义。

## 文件结构

- `signal_classification_rules.py` - 主要的信号分类规则模块
- `__init__.py` - Python包初始化文件
- `README.md` - 本说明文件

## 主要功能

### 信号分类规则

定义了以下信号类型：

#### 排除信号（不参与统计）
- `空头平仓Put，回补，看跌信号减弱`
- `买Call平仓/做波动率交易`
- `买Put平仓/做波动率交易`

#### 看涨Call信号
- `多头买 Call，看涨`
- `空头平仓 Call，回补信号，看涨`
- `买 Call，看涨`

#### 看跌Call信号
- `空头卖 Call，看跌/看不涨`
- `多头平仓 Call，减仓，看涨减弱`
- `卖 Call，看空/价差对冲`
- `卖 Call，看跌`

#### 看涨Put信号
- `空头卖 Put，看涨/看不跌`
- `多头平仓 Put，减仓，看跌减弱`
- `卖 Put，看涨/对冲`
- `卖 Put，看涨`

#### 看跌Put信号
- `多头买 Put，看跌`
- `买 Put，看跌`

## 使用方法

### 基本用法

```python
from rules.signal_classification_rules import classify_signal

# 分类信号
result = classify_signal("多头买 Call，看涨", "CALL")
print(result)
# 输出: {'is_bullish': True, 'is_bearish': False, 'is_call': True, 'is_put': False, 'should_count': True}
```

### 获取所有信号类型

```python
from rules.signal_classification_rules import get_all_signal_types

all_signals = get_all_signal_types()
print(f"总共有 {len(all_signals)} 个信号类型")
```

### 获取信号统计

```python
from rules.signal_classification_rules import get_signal_counts

counts = get_signal_counts()
print(counts)
# 输出: {'exclude_signals': 3, 'bullish_call_signals': 3, ...}
```

## 测试

运行测试脚本：

```bash
python3 rules/signal_classification_rules.py
```

## 维护

当需要添加新的信号类型时：

1. 在 `signal_classification_rules.py` 中添加新的信号到相应的列表中
2. 更新 `classify_signal` 函数的分类逻辑（如果需要）
3. 运行测试确保新规则工作正常
4. 更新文档

## 集成

这个规则模块已经被以下文件使用：

- `program/discord_outlier_sender_module.py` - Discord消息发送模块
- `program/find_outliers_by_oi.py` - OI异常检测
- `program/find_outliers_by_volume.py` - Volume异常检测

所有使用信号分类的地方都会自动使用这个统一的规则模块。
