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
   - amount_threshold / market_cap > 0.001% (相对于市值的比例过滤)

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
   - 跨日市值比例阈值：0.001%

6. 输出排序：
   - 按 amount_threshold (金额门槛) 从大到小排序
   - 金额分档：<=5M, 5M-10M, 10M-50M, >50M

7. 文件输出：
   - CSV格式：volume_outlier_YYYYMMDD-HHMM.csv
   - Excel格式：带颜色分档的 xlsx 文件
"""

import os
import glob
import pandas as pd
from datetime import datetime
import re

OPTION_DIR = "option_data"
STOCK_PRICE_DIR = "stock_price"
VOLUME_OUTLIER_DIR = "volume_outlier"
MARKET_CAP_FILE = "stock_symbol/symbol_market.csv"

# 成交量异常检测参数
MIN_VOLUME = 3000  # 最新成交量必须大于3000
MIN_VOLUME_INCREASE_PCT = 0.30  # 成交量增幅必须大于30%
MIN_AMOUNT_THRESHOLD = 2_000_000  # 金额门槛必须大于200万

# 股票和期权价格变化阈值
STOCK_CHANGE_THRESHOLD = 0.01  # 股票价格变化阈值1%
OPTION_CHANGE_THRESHOLD = 0.05  # 期权价格变化阈值5%


def parse_ts_from_filename(path: str) -> datetime:
    """从文件名解析时间戳"""
    name = os.path.basename(path)
    m = re.match(r"all-(\d{8})-(\d{4})\.csv$", name)
    if not m:
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
                          market_cap_df: pd.DataFrame = None) -> pd.DataFrame:
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
    prev_option_subset = prev_option_df[["contractSymbol", "volume", "lastPrice"]].copy()
    merged = latest_option_df.merge(prev_option_subset, on="contractSymbol", how="left", suffixes=("_new", "_old"))
    
    # 只处理同时存在于两份文件的合约
    merged = merged[merged["volume_old"].notna()].copy()
    
    if merged.empty:
        return pd.DataFrame()
    
    # 计算变化
    merged["volume_change"] = merged["volume_new"] - merged["volume_old"]
    merged["volume_change_pct"] = merged["volume_change"] / merged["volume_old"]
    merged["volume_change_pct"] = merged["volume_change_pct"].fillna(0)
    # 当上一份快照的 volume 为 0（例如跨日时被设为0），将增幅百分比视为100%
    merged.loc[merged["volume_old"] == 0, "volume_change_pct"] = 1.0
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
        volume_new = row["volume_new"]
        volume_change = row["volume_change"]
        volume_change_pct = row["volume_change_pct"]
        
        # 基础过滤条件
        if volume_new <= MIN_VOLUME:
            continue  # 最新成交量必须大于3000
        
        if volume_change_pct <= MIN_VOLUME_INCREASE_PCT:
            continue  # 成交量增幅必须大于30%
        
        # 计算金额门槛：成交量变化 * 期权lastPrice * 100
        amount_threshold = abs(volume_change) * row["lastPrice_new"] * 100
        if amount_threshold <= MIN_AMOUNT_THRESHOLD:
            continue  # 金额门槛必须大于200万
        
        # 对于 volume_old 为 0 的情况，需要额外的市值过滤条件
        if row["volume_old"] == 0:
            market_cap = row["market_cap"]
            if market_cap > 0:
                # 计算 amount_threshold / market_cap 的百分比
                market_cap_ratio = amount_threshold / market_cap
                if market_cap_ratio <= 0.00001:  # 0.001% = 0.00001
                    continue  # 跳过不满足市值比例要求的合约
            else:
                # 如果没有市值数据，跳过该合约
                continue
        
        # 判断变化方向
        stock_up = stock_change > STOCK_CHANGE_THRESHOLD
        stock_down = stock_change < -STOCK_CHANGE_THRESHOLD
        option_up = option_change > OPTION_CHANGE_THRESHOLD
        option_down = option_change < -OPTION_CHANGE_THRESHOLD
        volume_increase = volume_change > 0  # 成交量增加
        
        # 判断是否满足异常条件
        is_outlier = False
        signal_type = ""
        
        if option_type == "CALL":
            if stock_up and option_up and volume_increase:
                is_outlier = True
                signal_type = "买Call，看涨"
            elif stock_up and option_down and volume_increase:
                is_outlier = True
                signal_type = "卖Call，看空 / 价差对冲"
            elif stock_down and option_up and volume_increase:
                is_outlier = True
                signal_type = "买Call 平仓 / 做波动率交易"
            elif stock_down and option_down and volume_increase:
                is_outlier = True
                signal_type = "卖Call，看跌"
        elif option_type == "PUT":
            if stock_down and option_up and volume_increase:
                is_outlier = True
                signal_type = "买Put，看跌"
            elif stock_down and option_down and volume_increase:
                is_outlier = True
                signal_type = "卖Put，看涨 / 对冲"
            elif stock_up and option_up and volume_increase:
                is_outlier = True
                signal_type = "买Put 平仓 / 做波动率交易"
            elif stock_up and option_down and volume_increase:
                is_outlier = True
                signal_type = "卖Put，看涨"
        
        if is_outlier:
            # 添加异常信息
            outlier_row = row.copy()
            outlier_row["signal_type"] = signal_type
            outlier_row["stock_price_change_pct"] = stock_change * 100
            outlier_row["option_price_change_pct"] = option_change * 100
            outlier_row["volume_change_abs"] = abs(volume_change)
            outlier_row["volume_change_pct"] = volume_change_pct * 100
            outlier_row["amount_threshold"] = amount_threshold
            # 写入金额/市值占比（百分比）
            mc = row.get("market_cap", 0)
            outlier_row["amount_to_market_cap_pct"] = (amount_threshold / mc * 100) if mc and mc > 0 else 0
            # 写入最新 openInterest（如果存在于最新期权数据中）
            oi_new_val = row.get("openInterest", None)
            if oi_new_val is not None:
                outlier_row["openInterest_new"] = oi_new_val
            outliers.append(outlier_row)
    
    # 添加统计信息
    print(f"\n数据统计:")
    print(f"处理的合约总数: {len(merged)}")
    print(f"股票价格上涨的合约: {len(merged[merged['stock_price_change'] > STOCK_CHANGE_THRESHOLD])}")
    print(f"股票价格下跌的合约: {len(merged[merged['stock_price_change'] < -STOCK_CHANGE_THRESHOLD])}")
    print(f"期权价格上涨的合约: {len(merged[merged['option_price_change'] > OPTION_CHANGE_THRESHOLD])}")
    print(f"期权价格下跌的合约: {len(merged[merged['option_price_change'] < -OPTION_CHANGE_THRESHOLD])}")
    print(f"成交量增加的合约: {len(merged[merged['volume_change'] > 0])}")
    print(f"成交量减少的合约: {len(merged[merged['volume_change'] < 0])}")
    
    # 计算成交量异常统计
    volume_filtered = merged[
        (merged['volume_new'] > MIN_VOLUME) & 
        (merged['volume_change_pct'] > MIN_VOLUME_INCREASE_PCT)
    ]
    print(f"成交量大于{MIN_VOLUME}且增幅大于{MIN_VOLUME_INCREASE_PCT*100:.0f}%的合约: {len(volume_filtered)}")
    
    # 计算金额门槛统计
    volume_filtered = volume_filtered.copy()
    volume_filtered["amount_threshold"] = volume_filtered["volume_change"].abs() * volume_filtered["lastPrice_new"] * 100
    high_amount = volume_filtered[volume_filtered["amount_threshold"] > MIN_AMOUNT_THRESHOLD]
    print(f"金额门槛超过{MIN_AMOUNT_THRESHOLD/1_000_000:.0f}万的合约: {len(high_amount)}")
    
    if not high_amount.empty:
        print("  合约代码:", ", ".join(high_amount["contractSymbol"].head(10).tolist()))
    
    print(f"检测到的异常合约: {len(outliers)}")
    
    if not outliers:
        return pd.DataFrame()
    
    # 转换为DataFrame
    outliers_df = pd.DataFrame(outliers)
    
    # 标注金额分档
    def _amount_tier(x: float) -> str:
        if x <= 5_000_000:
            return "<=5M"
        if x <= 10_000_000:
            return "5M-10M"
        if x <= 50_000_000:
            return "10M-50M"
        return ">50M"
    
    outliers_df["amount_tier"] = outliers_df["amount_threshold"].apply(_amount_tier)
    
    # 按金额门槛排序
    outliers_df = outliers_df.sort_values(by="amount_threshold", ascending=False)
    
    return outliers_df


def ensure_dir(path: str):
    """确保目录存在"""
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def save_volume_outliers(df: pd.DataFrame, out_dir: str) -> str:
    """保存成交量异常结果"""
    ensure_dir(out_dir)
    ts = datetime.now().strftime("%Y%m%d-%H%M")
    out_path = os.path.join(out_dir, f"volume_outlier_{ts}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    
    # 另存为Excel并按金额分档着色
    try:
        xlsx_path = os.path.join(out_dir, f"volume_outlier_{ts}.xlsx")
        color_map = {
            "<=5M": "#2F80ED",   # 蓝
            "5M-10M": "#F2994A", # 橙
            "10M-50M": "#EB5757",# 大红
            ">50M": "#9B51E0"    # 特殊色（紫）
        }

        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="volume_outliers")
            workbook = writer.book
            worksheet = writer.sheets["volume_outliers"]

            # 找到 amount_tier 列索引
            header = list(df.columns)
            tier_col_idx = header.index("amount_tier") if "amount_tier" in header else None
            if tier_col_idx is not None:
                n_rows = len(df)
                n_cols = len(header)
                excel_range = 1, 0, n_rows, n_cols - 1

                for tier, color in color_map.items():
                    format_obj = workbook.add_format({"font_color": color})
                    worksheet.conditional_format(
                        excel_range[0], excel_range[1], excel_range[2], excel_range[3],
                        {
                            "type": "formula",
                            "criteria": f'=${chr(ord("A") + tier_col_idx)}2="{tier}"',
                            "format": format_obj
                        }
                    )
    except Exception:
        pass

    return out_path


def main():
    """主函数"""
    try:
        latest_option, previous_option, latest_stock, previous_stock, latest_ts, previous_ts = find_latest_two_all_csv(OPTION_DIR, STOCK_PRICE_DIR)
        print(f"最新期权文件: {latest_option}")
        print(f"上一份期权文件: {previous_option}")
        print(f"最新股票价格文件: {latest_stock}")
        print(f"上一份股票价格文件: {previous_stock}")

        latest_option_df = load_option_csv(latest_option)
        prev_option_df = load_option_csv(previous_option)
        latest_stock_df = load_stock_csv(latest_stock)
        prev_stock_df = load_stock_csv(previous_stock)
        market_cap_df = load_market_cap_csv(MARKET_CAP_FILE)

        # 若 option_data 中最新两个文件的 YYYYMMDD 不一致，则认为前一个 CSV 中的 volume 全为 0
        try:
            if latest_ts.strftime('%Y%m%d') != previous_ts.strftime('%Y%m%d'):
                prev_option_df = prev_option_df.copy()
                if "volume" in prev_option_df.columns:
                    prev_option_df["volume"] = 0
                else:
                    # 若不存在 volume 列，创建之以确保后续计算正常
                    prev_option_df["volume"] = 0
                print("检测到跨日数据，对上一份期权快照的 volume 置为 0")
        except Exception:
            # 安全兜底，遇到解析异常不影响主流程
            pass

        out_df = compute_volume_outliers(latest_option_df, prev_option_df, latest_stock_df, prev_stock_df, market_cap_df)
        if out_df.empty:
            print("未发现符合成交量异常条件的期权合约。")
            return
        
        out_path = save_volume_outliers(out_df, VOLUME_OUTLIER_DIR)
        print(f"已保存成交量异常结果: {out_path}")
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
                out_df_copy = out_df.copy()
                out_df_copy["is_bullish"] = st.str.contains("看涨", na=False)
                out_df_copy["is_bearish"] = st.str.contains("看跌", na=False)

                grouped = out_df_copy.groupby("symbol").agg(
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
