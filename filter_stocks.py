#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票筛选程序
从nasdaq_screener文件中筛选市值大于100,000,000的股票代码
"""

import pandas as pd
import os
from datetime import datetime

def filter_stocks_by_market_cap(input_file, output_file, min_market_cap=100000000):
    """
    筛选市值大于指定值的股票
    
    Args:
        input_file: 输入CSV文件路径
        output_file: 输出CSV文件路径
        min_market_cap: 最小市值阈值
    """
    try:
        print(f"正在读取文件: {input_file}")
        
        # 读取CSV文件
        df = pd.read_csv(input_file)
        
        print(f"原始数据: {len(df)} 个股票")
        print(f"列名: {list(df.columns)}")
        
        # 检查必要的列是否存在
        if 'Symbol' not in df.columns:
            print("错误: 文件中没有找到 'Symbol' 列")
            return False
            
        if 'Market Cap' not in df.columns:
            print("错误: 文件中没有找到 'Market Cap' 列")
            return False
        
        # 显示市值列的基本统计信息
        print(f"\n市值统计信息:")
        print(f"非空市值记录数: {df['Market Cap'].notna().sum()}")
        print(f"空值记录数: {df['Market Cap'].isna().sum()}")
        
        if df['Market Cap'].notna().sum() > 0:
            print(f"最小市值: ${df['Market Cap'].min():,.0f}")
            print(f"最大市值: ${df['Market Cap'].max():,.0f}")
            print(f"平均市值: ${df['Market Cap'].mean():,.0f}")
        
        # 筛选市值大于阈值的股票
        print(f"\n正在筛选市值大于 ${min_market_cap:,} 的股票...")
        
        # 先处理空值，将空值替换为0
        df_clean = df.copy()
        df_clean['Market Cap'] = df_clean['Market Cap'].fillna(0)
        
        # 筛选条件
        filtered_df = df_clean[df_clean['Market Cap'] > min_market_cap]
        
        print(f"筛选结果: {len(filtered_df)} 个股票")
        
        if len(filtered_df) == 0:
            print("没有找到符合条件的股票")
            return False
        
        # 保留Symbol和Sector列
        result_df = filtered_df[['Symbol', 'Sector']].copy()
        
        # 去重并排序
        result_df = result_df.drop_duplicates().sort_values('Symbol')
        
        print(f"去重后: {len(result_df)} 个唯一股票代码")
        
        # 保存结果
        result_df.to_csv(output_file, index=False)
        print(f"结果已保存到: {output_file}")
        
        # 显示前20个股票代码
        print(f"\n前20个股票代码:")
        for i, symbol in enumerate(result_df['Symbol'].head(20), 1):
            print(f"{i:2d}. {symbol}")
        
        if len(result_df) > 20:
            print(f"... 还有 {len(result_df) - 20} 个股票代码")
        
        return True
        
    except Exception as e:
        print(f"处理文件时出错: {e}")
        return False

def main():
    """主函数"""
    print("=" * 60)
    print("股票筛选程序 - 按市值筛选")
    print("=" * 60)
    
    # 输入文件路径
    input_file = "stock_symbol/nasdaq_screener_1759097351927.csv"
    output_file = "stock_symbol/symbol.csv"
    
    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误: 输入文件不存在: {input_file}")
        return
    
    # 市值阈值（15,000,000,000）
    min_market_cap = 15000000000
    
    print(f"输入文件: {input_file}")
    print(f"输出文件: {output_file}")
    print(f"最小市值阈值: ${min_market_cap:,}")
    print()
    
    # 执行筛选
    success = filter_stocks_by_market_cap(input_file, output_file, min_market_cap)
    
    if success:
        print("\n" + "=" * 60)
        print("筛选完成！")
        print("=" * 60)
        
        # 显示输出文件信息
        if os.path.exists(output_file):
            result_df = pd.read_csv(output_file)
            print(f"输出文件包含 {len(result_df)} 个股票代码")
            print(f"文件大小: {os.path.getsize(output_file)} 字节")
    else:
        print("\n" + "=" * 60)
        print("筛选失败！")
        print("=" * 60)

if __name__ == "__main__":
    main()
