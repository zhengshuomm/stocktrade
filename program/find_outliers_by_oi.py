#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于持仓量变化检测期权异常程序
比较 option_data 和 stock_price 目录下最新两份 all-*.csv 文件，根据股票价格变化、期权价格变化和持仓量变化判断异常情况

核心逻辑说明：
=============

1. 数据获取与预处理：
   - 获取 option_data 和 stock_price 目录下最新的两份 all-*.csv 文件
   - 解析文件名中的时间戳 (YYYYMMDD-HHMM 格式)
   - 加载期权数据 (contractSymbol, openInterest, lastPrice, option_type, symbol)
   - 加载股票价格数据 (symbol, Close)

2. 异常检测条件：
   基础过滤条件：
   - amount_threshold > 500万 (金额门槛必须大于500万)
   - 金额门槛计算：|OI变化| × 期权lastPrice × 100
   
   变化方向判断：
   - 股票价格变化阈值：1% (上涨/下跌)
   - 期权价格变化阈值：5% (上涨/下跌)
   - 持仓量变化：增加/减少 (OI变化 > 0 或 < 0)
   
   特殊处理：
   - 当持仓量变化特别大时 (|OI变化| > 1000)，放宽期权价格变化要求
   - 期权价格不变或上涨 → option_up = True
   - 期权价格不变或下跌 → option_down = True

3. 异常信号判断：
   根据股票价格变化、期权价格变化和持仓量变化的方向组合判断异常类型：
   
   CALL 期权：
   - 股票↑ + 期权↑ + OI↑ → 多头买Call，看涨
   - 股票↓ + 期权↓ + OI↑ → 空头卖Call，看跌/看不涨
   - 股票↑ + 期权↑ + OI↓ → 空头平仓Call，回补信号，看涨
   - 股票↓ + 期权↓ + OI↓ → 多头平仓Call，减仓，看涨减弱
   
   PUT 期权：
   - 股票↓ + 期权↑ + OI↑ → 多头买Put，看跌
   - 股票↑ + 期权↓ + OI↑ → 空头卖Put，看涨/看不跌
   - 股票↓ + 期权↑ + OI↓ → 空头平仓Put，回补，看跌信号减弱
   - 股票↑ + 期权↓ + OI↓ → 多头平仓Put，减仓，看跌减弱

4. 阈值参数：
   - 股票价格变化阈值：1%
   - 期权价格变化阈值：5%
   - 最小金额门槛：500万
   - 持仓量变化显著阈值：1000
   - 金额分档阈值：5M, 10M, 50M

5. 输出排序：
   - 按 amount_threshold (金额门槛) 从大到小排序
   - 金额分档：<=5M, 5M-10M, 10M-50M, >50M

6. 文件输出：
   - CSV格式：outlier/YYYYMMDD-HHMM.csv
   - Excel格式：带颜色分档的 xlsx 文件

7. 统计信息：
   - 处理的合约总数
   - 各方向变化的合约数量统计
   - 各金额档位的合约数量统计
   - 检测到的异常合约数量
"""

import os
import glob
import pandas as pd
from datetime import datetime
import re

OPTION_DIR = "option_data"
STOCK_PRICE_DIR = "stock_price"
OUTLIER_DIR = "outlier"
MARKET_CAP_FILE = "stock_symbol/symbol_market.csv"

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


def find_latest_two_all_csv(option_dir: str, stock_price_dir: str):
    """查找最新的两份期权数据和对应的股票价格数据"""
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
    
    return latest_option, previous_option, latest_stock, previous_stock


def load_option_csv(path: str) -> pd.DataFrame:
    """加载期权数据CSV文件"""
    df = pd.read_csv(path)
    required_cols = ["contractSymbol", "openInterest", "lastPrice", "option_type", "symbol"]
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


def compute_outliers(latest_option_df: pd.DataFrame, prev_option_df: pd.DataFrame, 
                    latest_stock_df: pd.DataFrame, prev_stock_df: pd.DataFrame, 
                    market_cap_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    根据股票价格变化、期权价格变化和持仓量变化判断异常情况
    """
    # 数据预处理
    latest_option_df = latest_option_df.copy()
    prev_option_df = prev_option_df.copy()
    latest_stock_df = latest_stock_df.copy()
    prev_stock_df = prev_stock_df.copy()
    
    # 确保数值列的类型正确
    numeric_cols = ["openInterest", "lastPrice", "Close"]
    for col in numeric_cols:
        if col in latest_option_df.columns:
            latest_option_df[col] = pd.to_numeric(latest_option_df[col], errors="coerce").fillna(0)
        if col in prev_option_df.columns:
            prev_option_df[col] = pd.to_numeric(prev_option_df[col], errors="coerce").fillna(0)
        if col in latest_stock_df.columns:
            latest_stock_df[col] = pd.to_numeric(latest_stock_df[col], errors="coerce").fillna(0)
        if col in prev_stock_df.columns:
            prev_stock_df[col] = pd.to_numeric(prev_stock_df[col], errors="coerce").fillna(0)
    
    # 创建股票价格变化映射
    stock_price_changes = {}
    for _, row in latest_stock_df.iterrows():
        symbol = row['symbol']
        latest_close = row['Close']
        prev_row = prev_stock_df[prev_stock_df['symbol'] == symbol]
        if not prev_row.empty:
            prev_close = prev_row.iloc[0]['Close']
            price_change = (latest_close - prev_close) / prev_close if prev_close != 0 else 0
            stock_price_changes[symbol] = price_change
    
    # 合并期权数据
    prev_option_subset = prev_option_df[["contractSymbol", "openInterest", "lastPrice"]].copy()
    merged = latest_option_df.merge(prev_option_subset, on="contractSymbol", how="left", suffixes=("_new", "_old"))
    
    # 只处理同时存在于两份文件的合约
    merged = merged[merged["openInterest_old"].notna()].copy()
    
    if merged.empty:
        return pd.DataFrame()
    
    # 计算变化
    merged["oi_change"] = merged["openInterest_new"] - merged["openInterest_old"]
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
        oi_change = row["oi_change"]
        
        # 判断变化方向
        stock_up = stock_change > 0.01  # 股票上涨超过1%
        stock_down = stock_change < -0.01  # 股票下跌超过1%
        option_up = option_change > 0.05  # 期权价格上涨超过5%
        option_down = option_change < -0.05  # 期权价格下跌超过5%
        oi_up = oi_change > 0  # 持仓量增加
        oi_down = oi_change < 0  # 持仓量减少
        
        # 对于持仓量变化特别大的情况，放宽期权价格变化的要求
        oi_change_significant = abs(oi_change) > 1000  # 持仓量变化超过1000
        if oi_change_significant:
            option_up = option_change >= 0  # 期权价格不变或上涨
            option_down = option_change <= 0  # 期权价格不变或下跌
        
        # 计算金额门槛：OI差值 * 期权lastPrice * 100
        oi_change_abs = abs(oi_change)
        last_price = row["lastPrice_new"]
        amount_threshold = oi_change_abs * last_price * 100
        
        # 金额门槛检查：必须大于500万
        if amount_threshold <= THRESHOLD_5M:
            continue  # 跳过不满足金额门槛的合约
        
        # 判断是否满足异常条件
        is_outlier = False
        signal_type = ""
        
        if option_type == "CALL":
            if stock_up and option_up and oi_up:
                is_outlier = True
                signal_type = "多头买 Call，看涨"
            elif stock_down and option_down and oi_up:
                is_outlier = True
                signal_type = "空头卖 Call，看跌/看不涨"
            elif stock_up and option_up and oi_down:
                is_outlier = True
                signal_type = "空头平仓 Call，回补信号，看涨"
            elif stock_down and option_down and oi_down:
                is_outlier = True
                signal_type = "多头平仓 Call，减仓，看涨减弱"
        elif option_type == "PUT":
            if stock_down and option_up and oi_up:
                is_outlier = True
                signal_type = "多头买 Put，看跌"
            elif stock_up and option_down and oi_up:
                is_outlier = True
                signal_type = "空头卖 Put，看涨/看不跌"
            elif stock_down and option_up and oi_down:
                is_outlier = True
                signal_type = "空头平仓 Put，回补，看跌信号减弱"
            elif stock_up and option_down and oi_down:
                is_outlier = True
                signal_type = "多头平仓 Put，减仓，看跌减弱"
        
        if is_outlier:
            # 添加异常信息
            outlier_row = row.copy()
            outlier_row["signal_type"] = signal_type
            outlier_row["stock_price_change_pct"] = stock_change * 100
            outlier_row["option_price_change_pct"] = option_change * 100
            # 输出持仓量变化为带符号的值（不取绝对值）
            outlier_row["oi_change_abs"] = oi_change
            outlier_row["amount_threshold"] = amount_threshold
            # 添加金额/市值比值
            market_cap = row.get("market_cap", 0)
            outlier_row["amount_to_market_cap"] = (amount_threshold / market_cap) if market_cap and market_cap > 0 else 0
            outliers.append(outlier_row)
    
    # 添加统计信息
    print(f"\n数据统计:")
    print(f"处理的合约总数: {len(merged)}")
    print(f"股票价格上涨的合约: {len(merged[merged['stock_price_change'] > 0.01])}")
    print(f"股票价格下跌的合约: {len(merged[merged['stock_price_change'] < -0.01])}")
    print(f"期权价格上涨的合约: {len(merged[merged['option_price_change'] > 0.05])}")
    print(f"期权价格下跌的合约: {len(merged[merged['option_price_change'] < -0.05])}")
    print(f"持仓量增加的合约: {len(merged[merged['oi_change'] > 0])}")
    print(f"持仓量减少的合约: {len(merged[merged['oi_change'] < 0])}")
    
    # 计算金额门槛统计（>=500万，分档计数）
    merged["amount_threshold"] = merged["oi_change"].abs() * merged["lastPrice_new"] * 100
    tier_5m = merged[merged["amount_threshold"] > THRESHOLD_5M]
    tier_10m = merged[merged["amount_threshold"] > THRESHOLD_10M]
    tier_50m = merged[merged["amount_threshold"] > THRESHOLD_50M]
    
    print(f"金额门槛超过500万的合约: {len(tier_5m)}")
    if not tier_5m.empty:
        print("  合约代码:", ", ".join(tier_5m["contractSymbol"].tolist()))
    
    print(f"金额门槛超过1000万的合约: {len(tier_10m)}")
    if not tier_10m.empty:
        print("  合约代码:", ", ".join(tier_10m["contractSymbol"].tolist()))
    
    print(f"金额门槛超过5000万的合约: {len(tier_50m)}")
    if not tier_50m.empty:
        print("  合约代码:", ", ".join(tier_50m["contractSymbol"].tolist()))
    
    print(f"检测到的异常合约: {len(outliers)}")
    
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


def save_outliers(df: pd.DataFrame, out_dir: str) -> str:
    ensure_dir(out_dir)
    ts = datetime.now().strftime("%Y%m%d-%H%M")
    out_path = os.path.join(out_dir, f"{ts}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    # 另存为Excel并按金额分档着色
    try:
        xlsx_path = os.path.join(out_dir, f"{ts}.xlsx")
        # 颜色方案（可按需调整）：
        #  "<=5M": 蓝色; "5M-10M": 橙色; "10M-50M": 红色; ">50M": 紫色
        color_map = {
            "<=5M": "#2F80ED",   # 蓝
            "5M-10M": "#F2994A", # 橙
            "10M-50M": "#EB5757",# 大红
            ">50M": "#9B51E0"    # 特殊色（紫）
        }

        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="outliers")
            workbook  = writer.book
            worksheet = writer.sheets["outliers"]

            # 找到 amount_tier 列索引
            header = list(df.columns)
            tier_col_idx = header.index("amount_tier") if "amount_tier" in header else None
            if tier_col_idx is not None:
                # 从第二行开始（第一行是表头），对整行应用条件格式
                n_rows = len(df)
                n_cols = len(header)
                excel_range = 1, 0, n_rows, n_cols - 1  # (first_row, first_col, last_row, last_col)

                for tier, color in color_map.items():
                    format_obj = workbook.add_format({"font_color": color})
                    worksheet.conditional_format(
                        excel_range[0], excel_range[1], excel_range[2], excel_range[3],
                        {
                            "type": "formula",
                            # Excel列号从A=0开始，需要把 tier_col_idx 转成列字母
                            "criteria": f'=${chr(ord("A") + tier_col_idx)}2="{tier}"',
                            "format": format_obj
                        }
                    )
        # 忽略上色失败等非致命错误
    except Exception:
        pass

    return out_path


def main():
    try:
        latest_option, previous_option, latest_stock, previous_stock = find_latest_two_all_csv(OPTION_DIR, STOCK_PRICE_DIR)
        print(f"最新期权文件: {latest_option}")
        print(f"上一份期权文件: {previous_option}")
        print(f"最新股票价格文件: {latest_stock}")
        print(f"上一份股票价格文件: {previous_stock}")

        latest_option_df = load_option_csv(latest_option)
        prev_option_df = load_option_csv(previous_option)
        latest_stock_df = load_stock_csv(latest_stock)
        prev_stock_df = load_stock_csv(previous_stock)
        market_cap_df = load_market_cap_csv(MARKET_CAP_FILE)

        out_df = compute_outliers(latest_option_df, prev_option_df, latest_stock_df, prev_stock_df, market_cap_df)
        if out_df.empty:
            print("未发现符合异常条件的期权合约。")
            return
        
        out_path = save_outliers(out_df, OUTLIER_DIR)
        print(f"已保存异常结果: {out_path}")
        print(f"异常条数: {len(out_df)}")
        
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
                # 定义情绪判断
                st = out_df["signal_type"].astype(str)
                out_df = out_df.copy()
                out_df["is_bullish"] = st.str.contains("看涨", na=False)
                out_df["is_bearish"] = st.str.contains("看跌", na=False)

                grouped = out_df.groupby("symbol").agg(
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
        except Exception as e:
            print(f"按symbol统计打印失败: {e}")
        
    except Exception as e:
        print(f"执行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
