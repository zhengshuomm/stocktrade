"""
期权异常信号分类规则包

这个包包含了所有用于分类期权异常信号的规则定义。
"""

from .signal_classification_rules import (
    classify_signal,
    get_all_signal_types,
    get_signal_counts,
    EXCLUDE_SIGNALS,
    BULLISH_CALL_SIGNALS,
    BEARISH_CALL_SIGNALS,
    BULLISH_PUT_SIGNALS,
    BEARISH_PUT_SIGNALS
)

__all__ = [
    'classify_signal',
    'get_all_signal_types', 
    'get_signal_counts',
    'EXCLUDE_SIGNALS',
    'BULLISH_CALL_SIGNALS',
    'BEARISH_CALL_SIGNALS',
    'BULLISH_PUT_SIGNALS',
    'BEARISH_PUT_SIGNALS'
]
