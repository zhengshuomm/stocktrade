#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
比较两个期权数据文件中相同合约的 openInterest 差异

功能说明：
- 只在命令行输出结果，不写任何文件
- 比较两个期权数据文件的 openInterest 差异
- 分析合约分布和变化统计
- 显示有变化的合约详情

使用方法：
python3 program/compare_openinterest.py --file file1.csv file2.csv
python3 program/compare_openinterest.py -f file1.csv file2.csv
"""

import pandas as pd
import os
import argparse
from datetime import datetime
import re

def parse_ts_from_filename(path):
    """从文件名中解析时间戳"""
    filename = os.path.basename(path)
    m = re.match(r'all-(\d{8})-(\d{4})\.csv', filename)
    if not m:
        return datetime.fromtimestamp(os.path.getmtime(path))
    ymd, hm = m.groups()
    return datetime.strptime(ymd + hm, "%Y%m%d%H%M")

def compare_openinterest(file1_path, file2_path):
    """比较两个文件的 openInterest 差异"""
    
    print("=" * 60)
    print("期权数据 openInterest 差异比较程序")
    print("=" * 60)
    
    # 读取文件
    print(f"读取文件1: {file1_path}")
    df1 = pd.read_csv(file1_path)
    print(f"读取文件2: {file2_path}")
    df2 = pd.read_csv(file2_path)
    
    print(f"\n文件1 合约数量: {len(df1)}")
    print(f"文件2 合约数量: {len(df2)}")
    
    # 检查列名
    if 'contractSymbol' not in df1.columns or 'openInterest' not in df1.columns:
        print("错误: 文件1 缺少必要列 (contractSymbol, openInterest)")
        return
    
    if 'contractSymbol' not in df2.columns or 'openInterest' not in df2.columns:
        print("错误: 文件2 缺少必要列 (contractSymbol, openInterest)")
        return
    
    # 合并数据
    merged = pd.merge(
        df1[['contractSymbol', 'openInterest']], 
        df2[['contractSymbol', 'openInterest']], 
        on='contractSymbol', 
        suffixes=('_file1', '_file2'),
        how='outer'  # 使用外连接以包含所有合约
    )
    
    print(f"\n合并后总合约数: {len(merged)}")
    
    # 分析合约分布
    only_in_file1 = merged['openInterest_file2'].isna()
    only_in_file2 = merged['openInterest_file1'].isna()
    in_both = ~(only_in_file1 | only_in_file2)
    
    print(f"只在文件1中的合约: {only_in_file1.sum()} 个")
    print(f"只在文件2中的合约: {only_in_file2.sum()} 个")
    print(f"两个文件都有的合约: {in_both.sum()} 个")
    
    # 分析共同合约的 openInterest 差异
    common_contracts = merged[in_both].copy()
    
    if len(common_contracts) == 0:
        print("\n没有共同合约可以比较")
        return
    
    # 处理 NaN 值
    has_nan_file1 = common_contracts['openInterest_file1'].isna()
    has_nan_file2 = common_contracts['openInterest_file2'].isna()
    both_nan = has_nan_file1 & has_nan_file2
    one_nan = has_nan_file1 != has_nan_file2
    both_valid = ~(has_nan_file1 | has_nan_file2)
    
    print(f"\n共同合约中:")
    print(f"  两个文件都是 NaN: {both_nan.sum()} 个")
    print(f"  只有一个文件是 NaN: {one_nan.sum()} 个")
    print(f"  两个文件都有有效值: {both_valid.sum()} 个")
    
    # 计算有效数据的差异
    valid_data = common_contracts[both_valid].copy()
    
    if len(valid_data) > 0:
        valid_data['oi_diff'] = valid_data['openInterest_file2'] - valid_data['openInterest_file1']
        valid_data['oi_changed'] = valid_data['oi_diff'] != 0
        
        print(f"\n有效数据统计:")
        print(f"  总有效合约: {len(valid_data)} 个")
        print(f"  openInterest 有变化: {valid_data['oi_changed'].sum()} 个")
        print(f"  openInterest 无变化: {(~valid_data['oi_changed']).sum()} 个")
        
        # 显示有变化的合约
        changed = valid_data[valid_data['oi_changed']].sort_values('oi_diff', key=abs, ascending=False)
        
        if len(changed) > 0:
            print(f"\nopenInterest 有变化的合约 (共 {len(changed)} 个):")
            print("-" * 80)
            print(f"{'合约代码':<25} {'文件1 OI':<12} {'文件2 OI':<12} {'差异':<10}")
            print("-" * 80)
            
            for _, row in changed.iterrows():
                print(f"{row['contractSymbol']:<25} {row['openInterest_file1']:<12.0f} {row['openInterest_file2']:<12.0f} {row['oi_diff']:<10.0f}")
            
            # 统计变化情况
            print(f"\n变化统计:")
            print(f"  增加: {(valid_data['oi_diff'] > 0).sum()} 个")
            print(f"  减少: {(valid_data['oi_diff'] < 0).sum()} 个")
            print(f"  最大增加: {valid_data['oi_diff'].max():.0f}")
            print(f"  最大减少: {valid_data['oi_diff'].min():.0f}")
            
            # 显示变化最大的合约
            print(f"\n变化最大的前5个合约:")
            top_changes = changed.head(5)
            for _, row in top_changes.iterrows():
                print(f"  {row['contractSymbol']}: {row['openInterest_file1']:.0f} -> {row['openInterest_file2']:.0f} ({row['oi_diff']:+.0f})")
        else:
            print("\n所有有效合约的 openInterest 都完全相同")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='比较两个期权数据文件的 openInterest 差异')
    parser.add_argument('--file', '-f', nargs=2, metavar=('FILE1', 'FILE2'),
                       help='要比较的两个期权数据文件名，例如: --file all-20250930-0923.csv all-20250930-1150.csv')
    
    args = parser.parse_args()
    
    if not args.file:
        print("错误: 请提供两个要比较的文件名")
        print("使用方法: python3 program/compare_openinterest.py --file file1.csv file2.csv")
        return
    
    file1_name, file2_name = args.file
    
    # 构建完整文件路径
    option_dir = "data/option_data"
    file1_path = os.path.join(option_dir, file1_name)
    file2_path = os.path.join(option_dir, file2_name)
    
    # 检查文件是否存在
    if not os.path.exists(file1_path):
        print(f"错误: 文件1不存在: {file1_path}")
        print(f"请检查 {option_dir} 目录中是否有文件: {file1_name}")
        return
    
    if not os.path.exists(file2_path):
        print(f"错误: 文件2不存在: {file2_path}")
        print(f"请检查 {option_dir} 目录中是否有文件: {file2_name}")
        return
    
    try:
        compare_openinterest(file1_path, file2_path)
    except Exception as e:
        print(f"错误: {e}")

if __name__ == "__main__":
    main()
