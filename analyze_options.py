#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
期权数据分析脚本
分析期权数据，处理openInterest为0的问题
"""

import pandas as pd
import numpy as np
import argparse
import os
from datetime import datetime

def analyze_options_data(csv_file):
    """
    分析期权数据
    
    Args:
        csv_file: CSV文件路径
    """
    print("=" * 60)
    print("期权数据分析")
    print("=" * 60)
    
    # 读取数据
    try:
        df = pd.read_csv(csv_file)
        print(f"成功读取数据: {len(df)} 行")
    except Exception as e:
        print(f"读取文件失败: {e}")
        return
    
    # 基本统计
    print(f"\n基本统计:")
    print(f"总期权数: {len(df)}")
    print(f"股票数量: {df['symbol'].nunique()}")
    print(f"期权类型分布:")
    print(df['option_type'].value_counts().to_string())
    
    # 数据质量分析
    print(f"\n数据质量分析:")
    
    # openInterest 分析
    open_interest_stats = df['openInterest'].describe()
    print(f"openInterest 统计:")
    print(f"  非零值数量: {(df['openInterest'] > 0).sum()}")
    print(f"  零值数量: {(df['openInterest'] == 0).sum()}")
    print(f"  非零比例: {(df['openInterest'] > 0).mean()*100:.1f}%")
    
    # volume 分析
    volume_stats = df['volume'].describe()
    print(f"\nvolume 统计:")
    print(f"  非零值数量: {(df['volume'] > 0).sum()}")
    print(f"  零值数量: {(df['volume'] == 0).sum()}")
    print(f"  非零比例: {(df['volume'] > 0).mean()*100:.1f}%")
    
    # liquidity_score 分析
    if 'liquidity_score' in df.columns:
        liquidity_stats = df['liquidity_score'].describe()
        print(f"\nliquidity_score 统计:")
        print(f"  非零值数量: {(df['liquidity_score'] > 0).sum()}")
        print(f"  零值数量: {(df['liquidity_score'] == 0).sum()}")
        print(f"  非零比例: {(df['liquidity_score'] > 0).mean()*100:.1f}%")
    
    # 价格数据质量
    print(f"\n价格数据质量:")
    print(f"  有lastPrice的期权: {(df['lastPrice'] > 0).sum()}")
    print(f"  有bid价格的期权: {(df['bid'] > 0).sum()}")
    print(f"  有ask价格的期权: {(df['ask'] > 0).sum()}")
    print(f"  有bid-ask价差的期权: {((df['bid'] > 0) & (df['ask'] > 0)).sum()}")
    
    # 价内期权分析
    if 'inTheMoney' in df.columns:
        print(f"\n价内期权分析:")
        print(f"  价内期权数量: {df['inTheMoney'].sum()}")
        print(f"  价外期权数量: {(~df['inTheMoney']).sum()}")
        print(f"  价内比例: {df['inTheMoney'].mean()*100:.1f}%")
    
    # 推荐的高质量期权
    print(f"\n推荐的高质量期权 (流动性好):")
    
    # 筛选条件
    has_volume = df['volume'] > 0
    has_price = (df['lastPrice'] > 0) | ((df['bid'] > 0) & (df['ask'] > 0))
    has_iv = df['impliedVolatility'] > 0
    
    good_options = df[has_volume & has_price & has_iv].copy()
    
    if len(good_options) > 0:
        print(f"  符合条件的期权数量: {len(good_options)}")
        print(f"  占总数的比例: {len(good_options)/len(df)*100:.1f}%")
        
        # 按流动性排序
        if 'liquidity_score' in good_options.columns:
            good_options = good_options.sort_values('liquidity_score', ascending=False)
        else:
            good_options = good_options.sort_values('volume', ascending=False)
        
        print(f"\n前10个高流动性期权:")
        display_cols = ['symbol', 'contractSymbol', 'strike', 'lastPrice', 'volume', 'openInterest']
        if 'liquidity_score' in good_options.columns:
            display_cols.append('liquidity_score')
        if 'inTheMoney' in good_options.columns:
            display_cols.append('inTheMoney')
        
        print(good_options[display_cols].head(10).to_string(index=False))
    else:
        print("  没有找到符合条件的高质量期权")
    
    # 按股票分析
    print(f"\n按股票分析 (前10个):")
    stock_stats = df.groupby('symbol').agg({
        'contractSymbol': 'count',
        'volume': 'sum',
        'openInterest': 'sum',
        'lastPrice': lambda x: (x > 0).sum()
    }).rename(columns={
        'contractSymbol': '期权数量',
        'volume': '总成交量',
        'openInterest': '总未平仓',
        'lastPrice': '有价格期权数'
    }).sort_values('期权数量', ascending=False)
    
    print(stock_stats.head(10).to_string())
    
    # 保存分析结果
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    output_file = f"option_analysis_{timestamp}.txt"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("期权数据分析报告\n")
        f.write("=" * 60 + "\n")
        f.write(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"数据文件: {csv_file}\n")
        f.write(f"总期权数: {len(df)}\n")
        f.write(f"股票数量: {df['symbol'].nunique()}\n")
        f.write(f"openInterest非零比例: {(df['openInterest'] > 0).mean()*100:.1f}%\n")
        f.write(f"volume非零比例: {(df['volume'] > 0).mean()*100:.1f}%\n")
        f.write(f"高质量期权数量: {len(good_options)}\n")
        f.write(f"高质量期权比例: {len(good_options)/len(df)*100:.1f}%\n")
    
    print(f"\n分析结果已保存到: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='期权数据分析脚本')
    parser.add_argument('--file', '-f', type=str, required=True, help='CSV文件路径')
    return parser.parse_args()

if __name__ == "__main__":
    args = main()
    analyze_options_data(args.file)
