#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BABA股票分钟级数据分析程序
从Yahoo Finance下载数据，计算技术指标，并保存到本地文件
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
import argparse
import sys

class BabaAnalyzer:
    def __init__(self, symbol="BABA"):
        self.symbol = symbol
        self.ticker = yf.Ticker(self.symbol)
        self.output_dir = "yahoo_finance"
        
        # 创建输出目录
        self._create_output_directory()
    
    def _create_output_directory(self):
        """
        创建输出目录
        """
        try:
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
                print(f"创建输出目录: {self.output_dir}")
        except Exception as e:
            print(f"创建输出目录时出错: {e}")
        
    def download_data(self, days=None, period="1d", interval="1m"):
        """
        从Yahoo Finance下载股票数据
        
        Args:
            days: 下载过去N天的数据 (如果指定，会覆盖period参数)
            period: 数据周期 ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
            interval: 数据间隔 ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")
        """
        print(f"正在下载 {self.symbol} 股票数据...")
        
        try:
            if days is not None:
                # 计算开始日期
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                print(f"下载过去 {days} 天的数据")
                print(f"时间范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
                print(f"间隔: {interval}")
                
                data = self.ticker.history(start=start_date, end=end_date, interval=interval)
            else:
                print(f"周期: {period}, 间隔: {interval}")
                data = self.ticker.history(period=period, interval=interval)
            
            if data.empty:
                print("警告: 没有获取到数据，可能是市场关闭或数据不可用")
                return None
            
            print(f"成功下载 {len(data)} 条数据记录")
            print(f"数据时间范围: {data.index[0]} 到 {data.index[-1]}")
            return data
            
        except Exception as e:
            print(f"下载数据时出错: {e}")
            return None
    
    def calculate_technical_indicators(self, data):
        """
        计算技术指标
        
        Args:
            data: 包含OHLCV数据的DataFrame
        """
        print("正在计算技术指标...")
        
        # 创建数据副本
        df = data.copy()
        
        # 确保数据按时间排序
        df = df.sort_index()
        
        # 1. 移动平均线
        df['MA_5'] = df['Close'].rolling(window=5).mean()
        df['MA_10'] = df['Close'].rolling(window=10).mean()
        df['MA_20'] = df['Close'].rolling(window=20).mean()
        df['MA_50'] = df['Close'].rolling(window=50).mean()
        
        # 2. 指数移动平均线 (EMA)
        df['EMA_12'] = df['Close'].ewm(span=12).mean()
        df['EMA_26'] = df['Close'].ewm(span=26).mean()
        
        # 3. MACD
        df['MACD'] = df['EMA_12'] - df['EMA_26']
        df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
        df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']
        
        # 4. RSI (相对强弱指数)
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # 5. 布林带
        df['BB_Middle'] = df['Close'].rolling(window=20).mean()
        bb_std = df['Close'].rolling(window=20).std()
        df['BB_Upper'] = df['BB_Middle'] + (bb_std * 2)
        df['BB_Lower'] = df['BB_Middle'] - (bb_std * 2)
        df['BB_Width'] = df['BB_Upper'] - df['BB_Lower']
        df['BB_Position'] = (df['Close'] - df['BB_Lower']) / (df['BB_Upper'] - df['BB_Lower'])
        
        # 6. 随机指标 (Stochastic)
        low_14 = df['Low'].rolling(window=14).min()
        high_14 = df['High'].rolling(window=14).max()
        df['Stoch_K'] = 100 * (df['Close'] - low_14) / (high_14 - low_14)
        df['Stoch_D'] = df['Stoch_K'].rolling(window=3).mean()
        
        # 7. 威廉指标 (Williams %R)
        df['Williams_R'] = -100 * (high_14 - df['Close']) / (high_14 - low_14)
        
        # 8. 成交量指标
        df['Volume_MA_10'] = df['Volume'].rolling(window=10).mean()
        df['Volume_Ratio'] = df['Volume'] / df['Volume_MA_10']
        
        # 9. 价格变化
        df['Price_Change'] = df['Close'].diff()
        df['Price_Change_Pct'] = df['Close'].pct_change() * 100
        
        # 10. 波动率
        df['Volatility'] = df['Price_Change_Pct'].rolling(window=20).std()
        
        # 11. ATR (平均真实波幅)
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        true_range = np.maximum(high_low, np.maximum(high_close, low_close))
        df['ATR'] = true_range.rolling(window=14).mean()
        
        # 12. 支撑阻力位
        df['Support'] = df['Low'].rolling(window=20).min()
        df['Resistance'] = df['High'].rolling(window=20).max()
        
        # 13. 趋势指标
        df['Trend_5'] = np.where(df['Close'] > df['MA_5'], 1, -1)
        df['Trend_20'] = np.where(df['Close'] > df['MA_20'], 1, -1)
        
        # 14. 动量指标
        df['Momentum_5'] = df['Close'] / df['Close'].shift(5) - 1
        df['Momentum_10'] = df['Close'] / df['Close'].shift(10) - 1
        
        # 15. 价格位置
        df['Price_Position_20'] = (df['Close'] - df['Low'].rolling(20).min()) / (df['High'].rolling(20).max() - df['Low'].rolling(20).min())
        
        print(f"技术指标计算完成，共计算了 {len([col for col in df.columns if col not in ['Open', 'High', 'Low', 'Close', 'Volume']])} 个指标")
        
        return df
    
    def save_data(self, raw_data, processed_data, raw_filename=None, processed_filename=None):
        """
        保存数据到文件
        
        Args:
            raw_data: 原始数据
            processed_data: 处理后的数据
            raw_filename: 原始数据文件名
            processed_filename: 处理后数据文件名
        """
        print("正在保存数据到文件...")
        
        try:
            # 生成默认文件名，包含输出目录路径
            if raw_filename is None:
                raw_filename = os.path.join(self.output_dir, f"{self.symbol.lower()}_raw_data.csv")
            else:
                # 如果用户指定了文件名，确保路径包含输出目录
                if not os.path.dirname(raw_filename):
                    raw_filename = os.path.join(self.output_dir, raw_filename)
                    
            if processed_filename is None:
                processed_filename = os.path.join(self.output_dir, f"{self.symbol.lower()}_processed_data.csv")
            else:
                # 如果用户指定了文件名，确保路径包含输出目录
                if not os.path.dirname(processed_filename):
                    processed_filename = os.path.join(self.output_dir, processed_filename)
            
            # 保存原始数据
            raw_data.to_csv(raw_filename)
            print(f"原始数据已保存到: {raw_filename}")
            
            # 处理空值 - 将NaN替换为"N/A"以便更好地阅读
            processed_data_clean = processed_data.copy()
            processed_data_clean = processed_data_clean.fillna("N/A")
            
            # 保存处理后的数据
            processed_data_clean.to_csv(processed_filename)
            print(f"处理后数据已保存到: {processed_filename}")
            
            # 计算有效数据统计
            valid_data_stats = {}
            for col in processed_data.columns:
                if col not in ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']:
                    valid_count = processed_data[col].notna().sum()
                    total_count = len(processed_data)
                    valid_data_stats[col] = {
                        "valid_points": int(valid_count),
                        "total_points": total_count,
                        "valid_percentage": round((valid_count / total_count) * 100, 2)
                    }
            
            # 创建数据摘要
            summary = {
                "symbol": self.symbol,
                "data_points": len(raw_data),
                "time_range": {
                    "start": str(raw_data.index[0]),
                    "end": str(raw_data.index[-1])
                },
                "indicators": [col for col in processed_data.columns if col not in ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']],
                "valid_data_stats": valid_data_stats,
                "files": {
                    "raw_data": os.path.basename(raw_filename),
                    "processed_data": os.path.basename(processed_filename)
                },
                "generated_at": datetime.now().isoformat(),
                "note": "空值(N/A)表示该指标需要更多历史数据才能计算，这是正常现象"
            }
            
            summary_filename = os.path.join(self.output_dir, f"{self.symbol.lower()}_analysis_summary.json")
            with open(summary_filename, "w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            
            print(f"数据摘要已保存到: {summary_filename}")
            
            # 显示空值统计信息
            print("\n技术指标有效数据统计:")
            print("-" * 60)
            for col, stats in valid_data_stats.items():
                print(f"{col:20s}: {stats['valid_points']:4d}/{stats['total_points']:4d} ({stats['valid_percentage']:5.1f}%)")
            
        except Exception as e:
            print(f"保存数据时出错: {e}")
    
    def run_analysis(self, days=None, period="1d", interval="1m"):
        """
        运行完整的分析流程
        
        Args:
            days: 下载过去N天的数据 (如果指定，会覆盖period参数)
            period: 数据周期 ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
            interval: 数据间隔 ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")
        """
        print("=" * 50)
        print(f"{self.symbol}股票分钟级数据分析程序")
        print("=" * 50)
        
        # 下载数据
        raw_data = self.download_data(days=days, period=period, interval=interval)
        if raw_data is None:
            print("无法获取数据，程序退出")
            return
        
        # 计算技术指标
        processed_data = self.calculate_technical_indicators(raw_data)
        
        # 保存数据
        self.save_data(raw_data, processed_data)
        
        print("=" * 50)
        print("分析完成！")
        print("=" * 50)
        
        # 显示数据概览
        print("\n数据概览:")
        print(f"原始数据形状: {raw_data.shape}")
        print(f"处理后数据形状: {processed_data.shape}")
        print(f"技术指标数量: {len([col for col in processed_data.columns if col not in ['Open', 'High', 'Low', 'Close', 'Volume']])}")
        
        return raw_data, processed_data

def parse_arguments():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='股票数据分析程序 - 下载股票数据并计算技术指标')
    
    parser.add_argument('--symbol', '-s', type=str, default='BABA', 
                       help='股票代码 (默认: BABA)')
    
    parser.add_argument('--days', '-d', type=int, default=None,
                       help='下载过去N天的数据 (例如: --days 7 下载过去7天的数据)')
    
    parser.add_argument('--period', '-p', type=str, default='1d',
                       choices=['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'],
                       help='数据周期 (默认: 1d)')
    
    parser.add_argument('--interval', '-i', type=str, default='1m',
                       choices=['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo'],
                       help='数据间隔 (默认: 1m)')
    
    parser.add_argument('--raw-file', type=str, default=None,
                       help='原始数据文件名 (默认: {symbol}_raw_data.csv)')
    
    parser.add_argument('--processed-file', type=str, default=None,
                       help='处理后数据文件名 (默认: {symbol}_processed_data.csv)')
    
    return parser.parse_args()

def main():
    """
    主函数
    """
    args = parse_arguments()
    
    # 创建分析器实例
    analyzer = BabaAnalyzer(symbol=args.symbol)
    
    print(f"股票代码: {args.symbol}")
    if args.days:
        print(f"下载过去 {args.days} 天的数据")
    else:
        print(f"数据周期: {args.period}")
    print(f"数据间隔: {args.interval}")
    print()
    
    # 运行分析
    raw_data, processed_data = analyzer.run_analysis(
        days=args.days,
        period=args.period,
        interval=args.interval
    )
    
    if raw_data is not None and processed_data is not None:
        print("\n前5行原始数据:")
        print(raw_data.head())
        
        print("\n前5行处理后数据:")
        print(processed_data.head())
        
        # 显示一些基本统计信息
        print(f"\n价格统计信息:")
        print(f"最高价: ${raw_data['High'].max():.2f}")
        print(f"最低价: ${raw_data['Low'].min():.2f}")
        print(f"平均收盘价: ${raw_data['Close'].mean():.2f}")
        print(f"总成交量: {raw_data['Volume'].sum():,}")

if __name__ == "__main__":
    main()
