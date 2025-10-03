#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于成交量变化检测期权异常程序
比较 option_data 和 stock_price 目录下最新两份 all-*.csv 文件，根据成交量变化判断异常情况

核心逻辑说明：
=============

1. 数据获取与预处理：
   - 获取 option_data 和 stock_price 目录下最新的两份 all-*.csv 文件
   - 解析文件名中的时间戳 (YYYYMMDD-HHMM 格式)
   - 加载期权数据 (contractSymbol, volume, lastPrice, option_type, symbol)
   - 加载股票价格数据 (symbol, Close)
   - 加载市值数据 (Symbol, Market Cap)

2. 跨日处理逻辑：
   - 比较最新两份期权文件的日期 (YYYYMMDD)
   - 如果日期不同，将前一份快照的 volume 全部置为 0
   - 对于 volume_old = 0 的情况，将 volume_change_pct 设为 100%

3. 异常检测条件：
   基础过滤条件：
   - volume_new > 3000 (最新成交量必须大于3000)
   - volume_change_pct > 30% (成交量增幅必须大于30%)
   - amount_threshold > 200万 (金额门槛必须大于200万)
   
   跨日额外过滤条件 (当 volume_old = 0 时)：
   - amount_threshold / market_cap > MIN_MARKET_CAP_RATIO (相对于市值的比例过滤)

4. 异常信号判断：
   根据股票价格变化、期权价格变化和成交量变化的方向组合判断异常类型：
   
   CALL 期权：
   - 股票↑ + 期权↑ + 成交量↑ → 买Call，看涨
   - 股票↑ + 期权↓ + 成交量↑ → 卖Call，看空/价差对冲
   - 股票↓ + 期权↑ + 成交量↑ → 买Call平仓/做波动率交易
   - 股票↓ + 期权↓ + 成交量↑ → 卖Call，看跌
   
   PUT 期权：
   - 股票↓ + 期权↑ + 成交量↑ → 买Put，看跌
   - 股票↓ + 期权↓ + 成交量↑ → 卖Put，看涨/对冲
   - 股票↑ + 期权↑ + 成交量↑ → 买Put平仓/做波动率交易
   - 股票↑ + 期权↓ + 成交量↑ → 卖Put，看涨

5. 阈值参数：
   - 股票价格变化阈值：1%
   - 期权价格变化阈值：5%
   - 最小成交量：3000
   - 最小成交量增幅：30%
   - 最小金额门槛：200万
   - 跨日市值比例阈值：MIN_MARKET_CAP_RATIO

6. 输出排序：
   - 按 amount_threshold (金额门槛) 从大到小排序
   - 金额分档：<=5M, 5M-10M, 10M-50M, >50M

7. 文件输出：
   - CSV格式：volume_outlier_YYYYMMDD-HHMM.csv
"""

import os
import glob
import pandas as pd
from datetime import datetime, timedelta
import re
from pytz import timezone
from discord_outlier_sender_module import send_volume_outliers

# 默认数据路径，可以通过 --folder 参数覆盖
DEFAULT_DATA_FOLDER = "data"
OPTION_DIR = f"{DEFAULT_DATA_FOLDER}/option_data"
STOCK_PRICE_DIR = f"{DEFAULT_DATA_FOLDER}/stock_price"
VOLUME_OUTLIER_DIR = f"{DEFAULT_DATA_FOLDER}/volume_outlier"
MARKET_CAP_FILE = f"{DEFAULT_DATA_FOLDER}/stock_symbol/symbol_market.csv"

# 成交量异常检测参数
MIN_VOLUME = 3000
MIN_VOLUME_INCREASE_PCT = 30
MIN_AMOUNT_THRESHOLD = 2_000_000  # 200万
MIN_MARKET_CAP_RATIO = 0.00001  # 0.001%

# 金额门槛全局变量
THRESHOLD_5M = 5_000_000    # 500万
THRESHOLD_10M = 10_000_000  # 1000万
THRESHOLD_50M = 50_000_000  # 5000万


def parse_ts_from_filename(path: str) -> datetime:
    # 期望文件名形如 all-YYYYMMDD-HHMM.csv
    name = os.path.basename(path)
    m = re.match(r"all-(\d{8})-(\d{4})\.csv$", name)
    if not m:
        # 退化：若不匹配则用mtime
        return datetime.fromtimestamp(os.path.getmtime(path))
    ymd, hm = m.groups()
    return datetime.strptime(ymd + hm, "%Y%m%d%H%M")


def find_previous_day_last_file(option_dir: str, stock_price_dir: str, current_date: str):
    """查找前一天最后一个文件"""
    # 计算前一天日期
    current_dt = datetime.strptime(current_date, '%Y%m%d')
    prev_dt = current_dt - timedelta(days=1)
    prev_date = prev_dt.strftime('%Y%m%d')
    
    # 查找前一天的所有文件
    prev_files = []
    for filename in os.listdir(option_dir):
        if filename.startswith('all-') and filename.endswith('.csv'):
            file_date = filename.split('-')[1][:8]  # 提取日期部分
            if file_date == prev_date:
                prev_files.append(filename)
    
    if not prev_files:
        return None, None, None
    
    # 按时间排序，找到最后一个文件
    prev_files.sort()
    last_prev_file = prev_files[-1]
    
    # 构建完整路径
    prev_option = os.path.join(option_dir, last_prev_file)
    prev_stock = os.path.join(stock_price_dir, last_prev_file)
    
    if not os.path.exists(prev_stock):
        return None, None, None
    
    prev_ts = parse_ts_from_filename(prev_option)
    return prev_option, prev_stock, prev_ts


def find_latest_two_all_csv(option_dir: str, stock_price_dir: str, specified_files: list = None):
    """查找最新的两份期权数据和对应的股票价格数据"""
    if specified_files and len(specified_files) >= 2:
        # 使用指定的文件，但按时间顺序排列
        file1 = os.path.join(option_dir, specified_files[0])
        file2 = os.path.join(option_dir, specified_files[1])
        
        if not os.path.exists(file1):
            raise FileNotFoundError(f"指定的期权文件不存在: {file1}")
        if not os.path.exists(file2):
            raise FileNotFoundError(f"指定的期权文件不存在: {file2}")
        
        # 提取时间戳并按时间排序
        ts1 = parse_ts_from_filename(file1)
        ts2 = parse_ts_from_filename(file2)
        
        if ts1 > ts2:
            # file1 时间更晚，作为最新文件
            latest_option = file1
            previous_option = file2
            latest_ts = ts1
            previous_ts = ts2
        else:
            # file2 时间更晚，作为最新文件
            latest_option = file2
            previous_option = file1
            latest_ts = ts2
            previous_ts = ts1
        
        # 查找对应的股票价格文件
        latest_stock = os.path.join(stock_price_dir, f"all-{latest_ts.strftime('%Y%m%d-%H%M')}.csv")
        previous_stock = os.path.join(stock_price_dir, f"all-{previous_ts.strftime('%Y%m%d-%H%M')}.csv")
        
        if not os.path.exists(latest_stock):
            raise FileNotFoundError(f"未找到对应的股票价格文件: {latest_stock}")
        if not os.path.exists(previous_stock):
            raise FileNotFoundError(f"未找到对应的股票价格文件: {previous_stock}")
    else:
        # 自动查找最新的文件
        option_pattern = os.path.join(option_dir, "all-*.csv")
        option_files = glob.glob(option_pattern)
        if not option_files or len(option_files) < 2:
            raise FileNotFoundError("未找到至少两份期权数据文件用于对比")
        
        option_files_sorted = sorted(option_files, key=lambda p: parse_ts_from_filename(p), reverse=True)
        latest_option = option_files_sorted[0]
        previous_option = option_files_sorted[1]
        
        # 提取时间戳
        latest_ts = parse_ts_from_filename(latest_option)
        previous_ts = parse_ts_from_filename(previous_option)
        
        # 查找对应的股票价格文件
        latest_stock = os.path.join(stock_price_dir, f"all-{latest_ts.strftime('%Y%m%d-%H%M')}.csv")
        previous_stock = os.path.join(stock_price_dir, f"all-{previous_ts.strftime('%Y%m%d-%H%M')}.csv")
        
        if not os.path.exists(latest_stock):
            raise FileNotFoundError(f"未找到对应的股票价格文件: {latest_stock}")
        if not os.path.exists(previous_stock):
            raise FileNotFoundError(f"未找到对应的股票价格文件: {previous_stock}")
    
    return latest_option, previous_option, latest_stock, previous_stock, latest_ts, previous_ts


def load_option_csv(path: str) -> pd.DataFrame:
    """加载期权数据CSV文件"""
    df = pd.read_csv(path)
    required_cols = ["contractSymbol", "volume", "lastPrice", "option_type", "symbol"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"期权文件 {path} 缺少必要列: {missing}")
    
    return df

def load_stock_csv(path: str) -> pd.DataFrame:
    """加载股票价格数据CSV文件"""
    df = pd.read_csv(path)
    required_cols = ["symbol", "Close"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"股票价格文件 {path} 缺少必要列: {missing}")
    return df


def load_market_cap_csv(path: str) -> pd.DataFrame:
    """加载市值数据CSV文件"""
    if not os.path.exists(path):
        print(f"警告: 市值文件 {path} 不存在，将跳过市值过滤")
        return pd.DataFrame()
    
    df = pd.read_csv(path)
    required_cols = ["Symbol", "Market Cap"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"警告: 市值文件 {path} 缺少必要列: {missing}，将跳过市值过滤")
        return pd.DataFrame()
    
    # 确保数值列的类型正确
    df["Market Cap"] = pd.to_numeric(df["Market Cap"], errors="coerce").fillna(0)
    return df


def compute_volume_outliers(latest_option_df: pd.DataFrame, prev_option_df: pd.DataFrame, 
                           latest_stock_df: pd.DataFrame, prev_stock_df: pd.DataFrame, 
                           market_cap_df: pd.DataFrame = None, market_cap_ratio: float = MIN_MARKET_CAP_RATIO, 
                           is_cross_day: bool = False, prev_day_stock_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    根据成交量变化判断异常情况
    """
    # 数据预处理
    latest_option_df = latest_option_df.copy()
    prev_option_df = prev_option_df.copy()
    latest_stock_df = latest_stock_df.copy()
    prev_stock_df = prev_stock_df.copy()
    
    # 确保数值列的类型正确
    numeric_cols = ["volume", "lastPrice", "Close", "openInterest"]
    for col in numeric_cols:
        if col in latest_option_df.columns:
            latest_option_df[col] = pd.to_numeric(latest_option_df[col], errors="coerce").fillna(0)
        if col in prev_option_df.columns:
            prev_option_df[col] = pd.to_numeric(prev_option_df[col], errors="coerce").fillna(0)
        if col in latest_stock_df.columns:
            latest_stock_df[col] = pd.to_numeric(latest_stock_df[col], errors="coerce").fillna(0)
        if col in prev_stock_df.columns:
            prev_stock_df[col] = pd.to_numeric(prev_stock_df[col], errors="coerce").fillna(0)
    
    # 创建股票价格变化映射和价格映射
    stock_price_changes = {}
    stock_prices = {}  # 存储最新和之前的股票价格
    for _, row in latest_stock_df.iterrows():
        symbol = row['symbol']
        latest_close = row['Close']
        prev_row = prev_stock_df[prev_stock_df['symbol'] == symbol]
        if not prev_row.empty:
            prev_close = prev_row.iloc[0]['Close']
            price_change = (latest_close - prev_close) / prev_close if prev_close != 0 else 0
            stock_price_changes[symbol] = price_change
            
            # 在跨日情况下，使用前一天最后一个文件的open价格
            if is_cross_day and prev_day_stock_df is not None:
                prev_day_row = prev_day_stock_df[prev_day_stock_df['symbol'] == symbol]
                if not prev_day_row.empty:
                    prev_open = prev_day_row.iloc[0]['Open']
                else:
                    prev_open = prev_row.iloc[0]['Open']
            else:
                prev_open = prev_row.iloc[0]['Open']
            
            stock_prices[symbol] = {
                'new': latest_close,
                'old': prev_close,
                'old_open': prev_open,
                'new_open': row['Open'],
                'new_high': row['High'],
                'new_low': row['Low']
            }
    
    # 合并期权数据
    prev_option_subset = prev_option_df[["contractSymbol", "volume", "lastPrice", "openInterest"]].copy()
    merged = latest_option_df.merge(prev_option_subset, on="contractSymbol", how="left", suffixes=("_new", "_old"))
    
    # 只处理同时存在于两份文件的合约
    merged = merged[merged["volume_old"].notna()].copy()
    
    if merged.empty:
        return pd.DataFrame()
    
    # 跨日数据特殊处理：如果volume完全一样，则跳过该行
    if is_cross_day:
        # 过滤掉volume完全相同的行
        merged = merged[merged["volume_new"] != merged["volume_old"]].copy()
        
        if merged.empty:
            return pd.DataFrame()
    
    # 计算变化
    if is_cross_day:
        # 跨日数据：计算差值
        merged["volume_change"] = merged["volume_new"] - merged["volume_old"]
        # 跨日数据的百分比计算：如果volume_old为0，则设为100%，否则正常计算
        merged["volume_change_pct"] = merged.apply(
            lambda row: 100.0 if row["volume_old"] == 0 and row["volume_new"] > 0
            else (row["volume_change"] / row["volume_old"] * 100 if row["volume_old"] > 0 else 0), axis=1
        )
    else:
        # 同日数据：正常计算
        merged["volume_change"] = merged["volume_new"] - merged["volume_old"]
        merged["volume_change_pct"] = (merged["volume_change"] / merged["volume_old"] * 100).fillna(0)
    merged["option_price_change"] = (merged["lastPrice_new"] - merged["lastPrice_old"]) / merged["lastPrice_old"]
    merged["option_price_change"] = merged["option_price_change"].fillna(0)
    
    # 添加股票价格变化
    merged["stock_price_change"] = merged["symbol"].map(stock_price_changes).fillna(0)
    
    # 添加市值数据
    if market_cap_df is not None and not market_cap_df.empty:
        market_cap_map = dict(zip(market_cap_df["Symbol"], market_cap_df["Market Cap"]))
        merged["market_cap"] = merged["symbol"].map(market_cap_map).fillna(0)
    else:
        merged["market_cap"] = 0
    
    # 判断异常情况
    outliers = []
    
    for _, row in merged.iterrows():
        symbol = row["symbol"]
        option_type = row["option_type"]
        stock_change = row["stock_price_change"]
        option_change = row["option_price_change"]
        volume_change = row["volume_change"]
        volume_change_pct = row["volume_change_pct"]
        volume_new = row["volume_new"]
        volume_old = row["volume_old"]
        
        # 基础过滤条件
        if volume_new <= MIN_VOLUME:
            continue  # 成交量太小
        
        if volume_change_pct <= MIN_VOLUME_INCREASE_PCT:
            continue  # 成交量增幅不够
        
        # 计算金额门槛：成交量变化 * 期权lastPrice * 100
        volume_change_abs = abs(volume_change)
        last_price = row["lastPrice_new"]
        amount_threshold = volume_change_abs * last_price * 100
        
        if amount_threshold <= MIN_AMOUNT_THRESHOLD:
            continue  # 金额门槛不够
        
        # 跨日额外过滤条件 (当 volume_old = 0 时)
        if volume_old == 0:
            market_cap = row.get("market_cap", 0)
            if market_cap > 0:
                calculated_ratio = amount_threshold / market_cap
                if calculated_ratio <= market_cap_ratio:
                    continue  # 相对于市值的比例不够
        
        # 判断变化方向
        stock_up = stock_change > 0.01  # 股票上涨超过1%
        stock_down = stock_change < -0.01  # 股票下跌超过1%
        option_up = option_change > 0.05  # 期权价格上涨超过5%
        option_down = option_change < -0.05  # 期权价格下跌超过5%
        volume_up = volume_change > 0  # 成交量增加
        
        # 判断是否满足异常条件
        is_outlier = False
        signal_type = ""
        
        if option_type == "CALL":
            if stock_up and option_up and volume_up:
                is_outlier = True
                signal_type = "买 Call，看涨"
            elif stock_up and option_down and volume_up:
                is_outlier = True
                signal_type = "卖 Call，看空/价差对冲"
            elif stock_down and option_up and volume_up:
                is_outlier = True
                signal_type = "买 Call平仓/做波动率交易"
            elif stock_down and option_down and volume_up:
                is_outlier = True
                signal_type = "卖 Call，看跌"
        elif option_type == "PUT":
            if stock_down and option_up and volume_up:
                is_outlier = True
                signal_type = "买 Put，看跌"
            elif stock_down and option_down and volume_up:
                is_outlier = True
                signal_type = "卖 Put，看涨/对冲"
            elif stock_up and option_up and volume_up:
                is_outlier = True
                signal_type = "买 Put平仓/做波动率交易"
            elif stock_up and option_down and volume_up:
                is_outlier = True
                signal_type = "卖 Put，看涨"
        
        if is_outlier:
            # 添加异常信息
            outlier_row = row.copy()
            outlier_row["signal_type"] = signal_type
            outlier_row["stock_price_change_pct"] = stock_change * 100
            outlier_row["option_price_change_pct"] = option_change * 100
            outlier_row["volume_change_abs"] = volume_change_abs
            outlier_row["amount_threshold"] = amount_threshold
            # 添加金额/市值比值
            market_cap = row.get("market_cap", 0)
            outlier_row["amount_to_market_cap"] = (amount_threshold / market_cap) if market_cap and market_cap > 0 else 0
            outlier_row["amount_to_market_cap_pct"] = outlier_row["amount_to_market_cap"] * 100
            # 添加最新持仓量
            outlier_row["openInterest_new"] = row.get("openInterest_new", None)
            
            # 添加股票价格字段
            if symbol in stock_prices:
                outlier_row["股票价格(new)"] = stock_prices[symbol]['new']
                outlier_row["股票价格(old)"] = stock_prices[symbol]['old']
                outlier_row["股票价格(new open)"] = stock_prices[symbol].get('new_open', None)
                outlier_row["股票价格(new high)"] = stock_prices[symbol].get('new_high', None)
                outlier_row["股票价格(new low)"] = stock_prices[symbol].get('new_low', None)
            else:
                outlier_row["股票价格(new)"] = None
                outlier_row["股票价格(old)"] = None
                outlier_row["股票价格(new open)"] = None
                outlier_row["股票价格(new high)"] = None
                outlier_row["股票价格(new low)"] = None
            
            outliers.append(outlier_row)
    
    if not outliers:
        return pd.DataFrame()
    
    # 转换为DataFrame
    outliers_df = pd.DataFrame(outliers)

    # 标注金额分档（四档）：
    #  <=5,000,000："<=5M"； 5,000,000-10,000,000："5M-10M"； 10,000,000-50,000,000："10M-50M"； >50,000,000：">50M"
    def _amount_tier(x: float) -> str:
        if x <= THRESHOLD_5M:
            return "<=5M"
        if x <= THRESHOLD_10M:
            return "5M-10M"
        if x <= THRESHOLD_50M:
            return "10M-50M"
        return ">50M"
    outliers_df["amount_tier"] = outliers_df["amount_threshold"].apply(_amount_tier)
    
    # 按金额门槛排序
    outliers_df = outliers_df.sort_values(by="amount_threshold", ascending=False)
    
    return outliers_df


def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def save_volume_outliers(df: pd.DataFrame, out_dir: str) -> str:
    ensure_dir(out_dir)
    # 使用美国西部时区（PST/PDT）时间作为文件名时间戳
    ts = datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y%m%d-%H%M")
    
    # 定义列顺序：前面几个重要列
    priority_columns = [
        "contractSymbol", "strike", "lastPrice_new", "signal_type", "stock_price_change_pct",
        "option_type", "volume_change_abs", "volume_new", "amount_threshold", 
        "amount_to_market_cap_pct", "openInterest_new", "amount_tier", "expiry_date",
        "股票价格(new)", "股票价格(old)", "股票价格(new open)", "股票价格(new high)", "股票价格(new low)"
    ]
    
    # 重新排列列顺序
    available_priority_cols = [col for col in priority_columns if col in df.columns]
    other_cols = [col for col in df.columns if col not in priority_columns]
    reordered_columns = available_priority_cols + other_cols
    df_reordered = df[reordered_columns]
    
    out_path = os.path.join(out_dir, f"volume_outlier_{ts}.csv")
    df_reordered.to_csv(out_path, index=False, encoding="utf-8-sig")

    return out_path


def main():
    import argparse
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='成交量异常检测程序')
    parser.add_argument('--folder', type=str, default=DEFAULT_DATA_FOLDER,
                       help=f'数据文件夹路径 (默认: {DEFAULT_DATA_FOLDER})')
    parser.add_argument('--files', '-f', type=str, nargs=2, metavar=('LATEST', 'PREVIOUS'),
                       help='指定要对比的期权文件名，例如: --files all-20250930-0923.csv all-20250930-1150.csv')
    parser.add_argument('--discord', '-d', action='store_true',
                       help='发送结果到 Discord (默认: 不发送)')
    parser.add_argument('--market-cap-ratio', type=float, default=MIN_MARKET_CAP_RATIO,
                       help=f'最小市值比例要求 (默认: {MIN_MARKET_CAP_RATIO})')
    
    args = parser.parse_args()
    
    # 根据folder参数更新路径
    global OPTION_DIR, STOCK_PRICE_DIR, VOLUME_OUTLIER_DIR, MARKET_CAP_FILE
    OPTION_DIR = f"{args.folder}/option_data"
    STOCK_PRICE_DIR = f"{args.folder}/stock_price"
    VOLUME_OUTLIER_DIR = f"{args.folder}/volume_outlier"
    MARKET_CAP_FILE = f"{args.folder}/stock_symbol/symbol_market.csv"
    
    try:
        if args.files:
            print(f"使用指定的文件进行对比:")
            print(f"  指定文件1: {args.files[0]}")
            print(f"  指定文件2: {args.files[1]}")
            print("  (将按时间顺序自动排列)")
        else:
            print("自动查找最新的两个文件进行对比")
        
        latest_option, previous_option, latest_stock, previous_stock, latest_ts, previous_ts = find_latest_two_all_csv(
            OPTION_DIR, STOCK_PRICE_DIR, args.files
        )
        print(f"最新期权文件: {latest_option}")
        print(f"上一份期权文件: {previous_option}")
        print(f"最新股票价格文件: {latest_stock}")
        print(f"上一份股票价格文件: {previous_stock}")

        latest_option_df = load_option_csv(latest_option)
        prev_option_df = load_option_csv(previous_option)
        latest_stock_df = load_stock_csv(latest_stock)
        prev_stock_df = load_stock_csv(previous_stock)
        market_cap_df = load_market_cap_csv(MARKET_CAP_FILE)

        # 跨日处理：如果最新两份文件的日期不同，需要特殊处理volume变化计算
        is_cross_day = latest_ts.strftime('%Y%m%d') != previous_ts.strftime('%Y%m%d')
        if is_cross_day:
            print("检测到跨日数据，将使用智能volume变化计算")
            # 查找前一天最后一个文件用于股票趋势判断
            current_date = latest_ts.strftime('%Y%m%d')
            prev_day_option, prev_day_stock, prev_day_ts = find_previous_day_last_file(OPTION_DIR, STOCK_PRICE_DIR, current_date)
            if prev_day_option and prev_day_stock:
                print(f"前一天最后一个期权文件: {prev_day_option}")
                print(f"前一天最后一个股票价格文件: {prev_day_stock}")
                # 使用前一天最后一个文件的数据来构建stock_prices
                prev_day_stock_df = load_stock_csv(prev_day_stock)
            else:
                print("未找到前一天最后一个文件，使用当前previous文件")
                prev_day_stock_df = prev_stock_df
        else:
            prev_day_stock_df = prev_stock_df

        out_df = compute_volume_outliers(latest_option_df, prev_option_df, latest_stock_df, prev_stock_df, market_cap_df, args.market_cap_ratio, is_cross_day, prev_day_stock_df)
        if out_df.empty:
            print("未发现符合异常条件的期权合约。")
            return
        
        # 先保存原始数据
        out_path = save_volume_outliers(out_df, VOLUME_OUTLIER_DIR)
        print(f"已保存异常结果: {out_path}")
        print(f"异常条数: {len(out_df)}")
        
        # 为Discord发送准备包含should_count字段的数据
        out_df_copy = None
        if args.discord:
            # 使用与统计部分相同的分类逻辑
            def classify_signal(row):
                signal_type = str(row["signal_type"])
                option_type = str(row["option_type"]).upper()
                
                # 不统计的信号类型
                exclude_signals = [
                    "空头平仓 Put，回补，看跌信号减弱",
                    "买 Call平仓/做波动率交易", 
                    "买 Put平仓/做波动率交易"
                ]
                
                if signal_type in exclude_signals:
                    return {
                        "is_bullish": False,
                        "is_bearish": False,
                        "is_call": False,
                        "is_put": False,
                        "should_count": False
                    }
                
                # 看涨Call信号
                bullish_call_signals = [
                    "多头买 Call，看涨",
                    "空头平仓 Call，回补信号，看涨",
                    "买 Call，看涨"
                ]
                
                # 看跌Call信号
                bearish_call_signals = [
                    "空头卖 Call，看跌/看不涨",
                    "多头平仓 Call，减仓，看涨减弱",
                    "卖 Call，看空/价差对冲",
                    "卖 Call，看跌"
                ]
                
                # 看涨Put信号
                bullish_put_signals = [
                    "空头卖 Put，看涨/看不跌",
                    "多头平仓 Put，减仓，看跌减弱", 
                    "卖 Put，看涨/对冲",
                    "卖 Put，看涨"
                ]
                
                # 看跌Put信号
                bearish_put_signals = [
                    "多头买 Put，看跌",
                    "买 Put，看跌"
                ]
                
                is_call = option_type == "CALL"
                is_put = option_type == "PUT"
                
                if signal_type in bullish_call_signals and is_call:
                    return {"is_bullish": True, "is_bearish": False, "is_call": True, "is_put": False, "should_count": True}
                elif signal_type in bearish_call_signals and is_call:
                    return {"is_bullish": False, "is_bearish": True, "is_call": True, "is_put": False, "should_count": True}
                elif signal_type in bullish_put_signals and is_put:
                    return {"is_bullish": True, "is_bearish": False, "is_call": False, "is_put": True, "should_count": True}
                elif signal_type in bearish_put_signals and is_put:
                    return {"is_bullish": False, "is_bearish": True, "is_call": False, "is_put": True, "should_count": True}
                else:
                    return {"is_bullish": False, "is_bearish": False, "is_call": False, "is_put": False, "should_count": False}
            
            # 应用分类
            out_df_copy = out_df.copy()
            classification = out_df_copy.apply(classify_signal, axis=1, result_type='expand')
            out_df_copy["is_bullish"] = classification['is_bullish']
            out_df_copy["is_bearish"] = classification['is_bearish'] 
            out_df_copy["is_call"] = classification['is_call']
            out_df_copy["is_put"] = classification['is_put']
            out_df_copy["should_count"] = classification['should_count']
        
        # 发送到 Discord (如果启用)
        if args.discord:
            print("\n开始发送到 Discord...")
            try:
                # 计算时间范围
                time_range = None
                if latest_ts and previous_ts:
                    time_range = f"{previous_ts.strftime('%Y%m%d-%H%M')} to {latest_ts.strftime('%Y%m%d-%H%M')}"
                
                # 计算股票价格数据
                stock_prices = {}
                if latest_stock_df is not None and prev_stock_df is not None:
                    for _, row in latest_stock_df.iterrows():
                        symbol = row['symbol']
                        latest_close = row['Close']
                        latest_open = row.get('Open', latest_close)  # 如果没有Open列，使用Close
                        latest_high = row.get('High', latest_close)  # 如果没有High列，使用Close
                        latest_low = row.get('Low', latest_close)    # 如果没有Low列，使用Close
                        prev_row = prev_stock_df[prev_stock_df['symbol'] == symbol]
                        if not prev_row.empty:
                            prev_close = prev_row.iloc[0]['Close']
                            
                            # 在跨日情况下，使用前一天最后一个文件的open价格
                            if is_cross_day and prev_day_stock_df is not None:
                                prev_day_row = prev_day_stock_df[prev_day_stock_df['symbol'] == symbol]
                                if not prev_day_row.empty:
                                    prev_open = prev_day_row.iloc[0]['Open']
                                else:
                                    prev_open = prev_row.iloc[0]['Open']
                            else:
                                prev_open = prev_row.iloc[0]['Open']
                            
                            stock_prices[symbol] = {
                                'new': latest_close,
                                'old': prev_close,
                                'old_open': prev_open,
                                'new_open': latest_open,
                                'new_high': latest_high,
                                'new_low': latest_low
                            }
                
                # 准备异常类型统计
                signal_type_stats = None
                if "signal_type" in out_df.columns:
                    signal_counts = out_df["signal_type"].value_counts()
                    signal_type_stats = signal_counts.to_dict()
                
                # 使用新的模块化Discord发送器
                import asyncio
                # 判断是否为跨日比较
                is_cross_day = False
                if args.files:
                    # 从文件名提取日期
                    import re
                    file1_date = re.search(r'all-(\d{8})-\d{4}\.csv', args.files[0])
                    file2_date = re.search(r'all-(\d{8})-\d{4}\.csv', args.files[1])
                    if file1_date and file2_date:
                        is_cross_day = file1_date.group(1) != file2_date.group(1)
                
                asyncio.run(send_volume_outliers(out_df_copy, args.folder, time_range, stock_prices, None, signal_type_stats, out_path, is_cross_day))
            except Exception as e:
                print(f"❌ Discord发送失败: {e}")
        
        # 显示异常类型统计
        if "signal_type" in out_df.columns:
            print("\n异常类型统计:")
            signal_counts = out_df["signal_type"].value_counts()
            for signal_type, count in signal_counts.items():
                print(f"  {signal_type}: {count} 个")
        
        # 针对最终写入文件的记录，按 symbol 统计 看涨/看跌 数量
        try:
            if "symbol" in out_df.columns and "signal_type" in out_df.columns and not out_df.empty:
                print("\n按股票统计（最终结果集）：")
                
                # 使用与Discord发送模块相同的精确分类逻辑
                def classify_signal(row):
                    signal_type = str(row["signal_type"])
                    option_type = str(row["option_type"]).upper()
                    
                    # 不统计的信号类型
                    exclude_signals = [
                        "空头平仓 Put，回补，看跌信号减弱",
                        "买 Call平仓/做波动率交易", 
                        "买 Put平仓/做波动率交易"
                    ]
                    
                    if signal_type in exclude_signals:
                        return {
                            "is_bullish": False,
                            "is_bearish": False,
                            "is_call": False,
                            "is_put": False,
                            "should_count": False
                        }
                    
                    # 看涨Call
                    bullish_call_signals = [
                        "多头买 Call，看涨",
                        "空头平仓 Call，回补信号，看涨",
                        "买 Call，看涨"
                    ]
                    
                    # 看跌Call  
                    bearish_call_signals = [
                        "空头卖 Call，看跌/看不涨",
                        "多头平仓 Call，减仓，看涨减弱",
                        "卖 Call，看空/价差对冲",
                        "卖 Call，看跌"
                    ]
                    
                    # 看涨Put
                    bullish_put_signals = [
                        "空头卖 Put，看涨/看不跌",
                        "多头平仓 Put，减仓，看跌减弱", 
                        "卖 Put，看涨/对冲",
                        "卖 Put，看涨"
                    ]
                    
                    # 看跌Put
                    bearish_put_signals = [
                        "多头买 Put，看跌",
                        "买 Put，看跌"
                    ]
                    
                    is_call = "CALL" in option_type
                    is_put = "PUT" in option_type
                    
                    if signal_type in bullish_call_signals and is_call:
                        return {"is_bullish": True, "is_bearish": False, "is_call": True, "is_put": False, "should_count": True}
                    elif signal_type in bearish_call_signals and is_call:
                        return {"is_bullish": False, "is_bearish": True, "is_call": True, "is_put": False, "should_count": True}
                    elif signal_type in bullish_put_signals and is_put:
                        return {"is_bullish": True, "is_bearish": False, "is_call": False, "is_put": True, "should_count": True}
                    elif signal_type in bearish_put_signals and is_put:
                        return {"is_bullish": False, "is_bearish": True, "is_call": False, "is_put": True, "should_count": True}
                    else:
                        return {"is_bullish": False, "is_bearish": False, "is_call": False, "is_put": False, "should_count": False}
                
                # 应用分类
                out_df_copy = out_df.copy()
                classification = out_df_copy.apply(classify_signal, axis=1, result_type='expand')
                out_df_copy["is_bullish"] = classification['is_bullish']
                out_df_copy["is_bearish"] = classification['is_bearish'] 
                out_df_copy["is_call"] = classification['is_call']
                out_df_copy["is_put"] = classification['is_put']
                out_df_copy["should_count"] = classification['should_count']
                
                # 只统计should_count=True的记录
                countable_df = out_df_copy[out_df_copy['should_count']]
                
                if not countable_df.empty:
                    grouped = countable_df.groupby("symbol").agg(
                        bullish_count=("is_bullish", "sum"),
                        bearish_count=("is_bearish", "sum"),
                        total=("symbol", "count")
                    ).reset_index()

                    # 按总数与bullish优先显示
                    grouped = grouped.sort_values(by=["total", "bullish_count"], ascending=[False, False])

                    for _, row in grouped.iterrows():
                        sym = row["symbol"]
                        bull = int(row["bullish_count"])  
                        bear = int(row["bearish_count"])  
                        tot = int(row["total"])
                        print(f"  {sym}: 看涨 {bull} 个, 看跌 {bear} 个, 合计 {tot}")
                else:
                    print("  没有可统计的记录")
        except Exception as e:
            print(f"按symbol统计打印失败: {e}")
        
    except Exception as e:
        print(f"执行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
