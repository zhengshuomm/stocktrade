"""
期权异常信号分类规则

这个模块包含了所有用于分类期权异常信号的规则定义。
用于判断信号类型是否属于看涨、看跌、Call、Put等类别。

作者: AI Assistant
创建时间: 2025-01-02
"""

# 需要排除的信号类型（不参与统计）
EXCLUDE_SIGNALS = [
    "空头平仓Put，回补，看跌信号减弱",
    "买Call平仓/做波动率交易", 
    "买Put平仓/做波动率交易"
]

# 看涨Call信号
BULLISH_CALL_SIGNALS = [
    "多头买 Call，看涨",
    "空头平仓 Call，回补信号，看涨",
    "买 Call，看涨"
]

# 看跌Call信号
BEARISH_CALL_SIGNALS = [
    "空头卖 Call，看跌/看不涨",
    "多头平仓 Call，减仓，看涨减弱",
    "卖 Call，看空/价差对冲",
    "卖 Call，看跌"
]

# 看涨Put信号
BULLISH_PUT_SIGNALS = [
    "空头卖 Put，看涨/看不跌",
    "多头平仓 Put，减仓，看跌减弱", 
    "卖 Put，看涨/对冲",
    "卖 Put，看涨"
]

# 看跌Put信号
BEARISH_PUT_SIGNALS = [
    "多头买 Put，看跌",
    "买 Put，看跌"
]


def classify_signal(signal_type, option_type):
    """
    根据信号类型和期权类型进行分类
    
    Args:
        signal_type (str): 信号类型字符串
        option_type (str): 期权类型 ('CALL' 或 'PUT')
    
    Returns:
        dict: 包含分类结果的字典
            - is_bullish: bool, 是否为看涨信号
            - is_bearish: bool, 是否为看跌信号  
            - is_call: bool, 是否为Call期权
            - is_put: bool, 是否为Put期权
            - should_count: bool, 是否应该参与统计
    """
    # 检查是否为Call或Put
    is_call = option_type.upper() == 'CALL' if option_type else False
    is_put = option_type.upper() == 'PUT' if option_type else False
    
    # 检查是否需要排除
    if signal_type in EXCLUDE_SIGNALS:
        return {
            "is_bullish": False,
            "is_bearish": False,
            "is_call": False,
            "is_put": False,
            "should_count": False
        }
    
    # 分类逻辑
    if signal_type in BULLISH_CALL_SIGNALS and is_call:
        return {"is_bullish": True, "is_bearish": False, "is_call": True, "is_put": False, "should_count": True}
    elif signal_type in BEARISH_CALL_SIGNALS and is_call:
        return {"is_bullish": False, "is_bearish": True, "is_call": True, "is_put": False, "should_count": True}
    elif signal_type in BULLISH_PUT_SIGNALS and is_put:
        return {"is_bullish": True, "is_bearish": False, "is_call": False, "is_put": True, "should_count": True}
    elif signal_type in BEARISH_PUT_SIGNALS and is_put:
        return {"is_bullish": False, "is_bearish": True, "is_call": False, "is_put": True, "should_count": True}
    else:
        return {"is_bullish": False, "is_bearish": False, "is_call": False, "is_put": False, "should_count": False}


def get_all_signal_types():
    """
    获取所有定义的信号类型
    
    Returns:
        list: 所有信号类型的列表
    """
    all_signals = []
    all_signals.extend(EXCLUDE_SIGNALS)
    all_signals.extend(BULLISH_CALL_SIGNALS)
    all_signals.extend(BEARISH_CALL_SIGNALS)
    all_signals.extend(BULLISH_PUT_SIGNALS)
    all_signals.extend(BEARISH_PUT_SIGNALS)
    return all_signals


def get_signal_counts():
    """
    获取各类信号的数量统计
    
    Returns:
        dict: 各类信号的数量统计
    """
    return {
        "exclude_signals": len(EXCLUDE_SIGNALS),
        "bullish_call_signals": len(BULLISH_CALL_SIGNALS),
        "bearish_call_signals": len(BEARISH_CALL_SIGNALS),
        "bullish_put_signals": len(BULLISH_PUT_SIGNALS),
        "bearish_put_signals": len(BEARISH_PUT_SIGNALS),
        "total_signals": len(get_all_signal_types())
    }


if __name__ == "__main__":
    # 测试代码
    print("信号分类规则测试:")
    print(f"排除信号数量: {len(EXCLUDE_SIGNALS)}")
    print(f"看涨Call信号数量: {len(BULLISH_CALL_SIGNALS)}")
    print(f"看跌Call信号数量: {len(BEARISH_CALL_SIGNALS)}")
    print(f"看涨Put信号数量: {len(BULLISH_PUT_SIGNALS)}")
    print(f"看跌Put信号数量: {len(BEARISH_PUT_SIGNALS)}")
    print(f"总信号数量: {len(get_all_signal_types())}")
    
    # 测试分类函数
    test_cases = [
        ("多头买 Call，看涨", "CALL"),
        ("空头卖 Put，看涨/看不跌", "PUT"),
        ("买Call平仓/做波动率交易", "CALL"),
        ("卖 Call，看跌", "CALL")
    ]
    
    print("\n分类测试:")
    for signal_type, option_type in test_cases:
        result = classify_signal(signal_type, option_type)
        print(f"{signal_type} ({option_type}) -> {result}")
