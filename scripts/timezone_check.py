#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
时区检查工具
用于验证 GitHub Actions 的时间配置是否正确
"""

import datetime
import pytz

def check_timezone():
    """检查当前时区和夏令时状态"""
    print("=== 时区信息检查 ===")
    
    # 获取美国西部时区
    pacific = pytz.timezone('America/Los_Angeles')
    now_pacific = datetime.datetime.now(pacific)
    now_utc = datetime.datetime.now(pytz.UTC)
    
    print(f"当前 UTC 时间: {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"美国西部时间: {now_pacific.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"是否为夏令时: {now_pacific.dst() != datetime.timedelta(0)}")
    print(f"UTC 偏移: {now_pacific.utcoffset()}")
    
    # 计算目标时间
    target_times = [
        "05:30", "06:30", "07:30", "08:30", "09:30", "10:30", "11:30", "13:30"
    ]
    
    print("\n=== 目标运行时间 (美国西部时间) ===")
    for time_str in target_times:
        hour, minute = map(int, time_str.split(':'))
        target_time = now_pacific.replace(hour=hour, minute=minute, second=0, microsecond=0)
        utc_time = target_time.astimezone(pytz.UTC)
        print(f"{time_str} PST/PDT -> {utc_time.strftime('%H:%M UTC')}")
    
    # 检查当前是否在夏令时期间
    year = now_pacific.year
    
    # 计算3月第二个周日（夏令时开始）
    march_8 = datetime.datetime(year, 3, 8, tzinfo=pacific)
    march_second_sunday = march_8
    while march_second_sunday.weekday() != 6:  # 找到周日
        march_second_sunday += datetime.timedelta(days=1)
    march_second_sunday += datetime.timedelta(days=7)  # 第二个周日
    
    # 计算11月第一个周日（夏令时结束）
    november_1 = datetime.datetime(year, 11, 1, tzinfo=pacific)
    november_first_sunday = november_1
    while november_first_sunday.weekday() != 6:  # 找到周日
        november_first_sunday += datetime.timedelta(days=1)
    
    print(f"\n=== 夏令时变化时间 ===")
    print(f"夏令时开始: {march_second_sunday.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"夏令时结束: {november_first_sunday.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # 检查当前是否在夏令时期间
    is_dst = march_second_sunday <= now_pacific < november_first_sunday
    print(f"当前是否在夏令时期间: {is_dst}")
    
    return is_dst

if __name__ == "__main__":
    check_timezone()
