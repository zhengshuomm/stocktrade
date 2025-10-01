#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量扫描股票期权数据程序
扫描symbol_market.csv中的股票代码，获取期权数据并保存到options_data文件夹
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import os
import time
import sys

class StockOptionsScanner:
    def __init__(self, symbol_file="data/stock_symbol/symbol_market.csv", data_folder="data"):
        """
        初始化扫描器
        
        Args:
            symbol_file: 包含股票代码的CSV文件路径
            data_folder: 数据文件夹路径 (默认: data)
        """
        self.data_folder = data_folder
        self.symbol_file = symbol_file
        self.output_dir = f"{data_folder}/option_data"
        self.stock_price_dir = f"{data_folder}/stock_price"
        self.symbols = []
        self.results = []
        
        # 创建输出目录
        self._create_output_directory()
        
        # 加载股票代码
        self._load_symbols()
    
    def _create_output_directory(self):
        """
        创建输出目录
        """
        try:
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)
                print(f"创建输出目录: {self.output_dir}")
            
            if not os.path.exists(self.stock_price_dir):
                os.makedirs(self.stock_price_dir)
                print(f"创建股票价格目录: {self.stock_price_dir}")
        except Exception as e:
            print(f"创建输出目录时出错: {e}")
    
    def _load_symbols(self):
        """
        从CSV文件加载股票代码
        """
        try:
            df = pd.read_csv(self.symbol_file)
            if 'Symbol' in df.columns:
                self.symbols = df['Symbol'].tolist()
                print(f"从 {self.symbol_file} 加载了 {len(self.symbols)} 个股票代码")
            else:
                print(f"错误: {self.symbol_file} 中没有找到 'Symbol' 列")
                sys.exit(1)
        except Exception as e:
            print(f"加载股票代码时出错: {e}")
            sys.exit(1)
    
    def get_options_data(self, symbol, max_deviation=0.3):
        """
        获取单个股票的期权数据
        
        Args:
            symbol: 股票代码
            max_deviation: 最大偏差比例（默认30%）
            
        Returns:
            期权数据字典或None
        """
        try:
            ticker = yf.Ticker(symbol)
            
            # 获取期权到期日
            expirations = ticker.options
            if not expirations:
                return None
            
            print(f"    找到 {len(expirations)} 个到期日")
            
            options_chain_data = []
            
            # 处理所有到期日（不限制数量）
            for i, exp_date in enumerate(expirations):
                try:
                    # 获取期权链
                    opt_chain = ticker.option_chain(exp_date)
                    
                    # 处理看涨期权
                    if not opt_chain.calls.empty:
                        calls = opt_chain.calls.copy()
                        calls['option_type'] = 'CALL'
                        calls['expiry_date'] = exp_date
                        calls['strike_price'] = calls['strike']
                        calls['contractSymbol'] = calls['contractSymbol']
                        options_chain_data.append(calls)
                    
                    # 处理看跌期权
                    if not opt_chain.puts.empty:
                        puts = opt_chain.puts.copy()
                        puts['option_type'] = 'PUT'
                        puts['expiry_date'] = exp_date
                        puts['strike_price'] = puts['strike']
                        puts['contractSymbol'] = puts['contractSymbol']
                        options_chain_data.append(puts)
                    
                    time.sleep(0.1)  # 避免请求过于频繁
                    
                except Exception as e:
                    print(f"    处理到期日 {exp_date} 时出错: {e}")
                    continue
            
            if not options_chain_data:
                return None
            
            # 合并所有期权数据
            options_df = pd.concat(options_chain_data, ignore_index=True)
            
            # 获取股票当前价格
            stock_info = ticker.info
            current_price = stock_info.get('currentPrice', 0)
            
            # 过滤执行价格差别太大的期权
            if current_price > 0 and not options_df.empty:
                options_df = self._filter_options_by_strike_price(options_df, current_price, max_deviation)
            
            return {
                'symbol': symbol,
                'options_chain': options_df,
                'current_price': current_price,
                'expirations': expirations,
                'stock_info': stock_info
            }
            
        except Exception as e:
            return None
    
    def _filter_options_by_strike_price(self, options_df, current_price, max_deviation=0.3):
        """
        过滤执行价格差别太大的期权
        
        Args:
            options_df: 期权数据DataFrame
            current_price: 当前股价
            max_deviation: 最大偏差比例（默认30%）
            
        Returns:
            过滤后的期权数据DataFrame
        """
        options_df['price_deviation'] = abs(options_df['strike_price'] - current_price) / current_price
        
        # 过滤条件：看涨期权执行价不超过当前价格的(1+max_deviation)，看跌期权执行价不低于当前价格的(1-max_deviation)
        call_mask = (options_df['option_type'] == 'CALL') & (options_df['strike_price'] <= current_price * (1 + max_deviation))
        put_mask = (options_df['option_type'] == 'PUT') & (options_df['strike_price'] >= current_price * (1 - max_deviation))
        deviation_mask = options_df['price_deviation'] <= max_deviation
        
        filter_mask = ((call_mask) | (put_mask)) & deviation_mask
        filtered_df = options_df[filter_mask].copy()
        
        if 'price_deviation' in filtered_df.columns:
            filtered_df = filtered_df.drop('price_deviation', axis=1)
        
        return filtered_df
    
    def get_stock_price(self, symbol):
        """
        获取单个股票的当前价格数据
        
        Args:
            symbol: 股票代码
            
        Returns:
            包含价格数据的字典或None
        """
        try:
            ticker = yf.Ticker(symbol)
            
            # 获取最近1天的历史数据
            hist = ticker.history(period="1d")
            
            if hist.empty:
                return None
            
            # 获取最新的一行数据
            latest_data = hist.iloc[-1]
            
            # 获取当前日期
            current_date = datetime.now().strftime("%Y-%m-%d")
            
            return {
                'symbol': symbol,
                'Date': current_date,
                'Open': float(latest_data['Open']),
                'High': float(latest_data['High']),
                'Low': float(latest_data['Low']),
                'Close': float(latest_data['Close']),
                'Volume': int(latest_data['Volume'])
            }
            
        except Exception as e:
            print(f"    获取 {symbol} 价格数据时出错: {e}")
            return None
    
    def scan_all_stocks(self, max_deviation=0.3, delay=1.0, max_stocks=None):
        """
        扫描所有股票（单线程版本）
        
        Args:
            max_deviation: 最大偏差比例
            delay: 每个股票之间的延时（秒）
            max_stocks: 最大扫描股票数量（None表示扫描所有）
        """
        symbols_to_scan = self.symbols[:max_stocks] if max_stocks else self.symbols
        print(f"开始扫描 {len(symbols_to_scan)} 个股票的期权数据...")
        print(f"最大偏差比例: {max_deviation*100:.0f}%")
        print(f"延时设置: {delay}秒")
        print("=" * 60)
        
        all_options_data = []
        all_stock_prices = []
        successful_scans = 0
        failed_scans = 0
        
        for i, symbol in enumerate(symbols_to_scan, 1):
            print(f"[{i}/{len(symbols_to_scan)}] 扫描 {symbol}...")
            
            try:
                # 获取期权数据
                options_data = self.get_options_data(symbol, max_deviation)
                
                # 获取股票价格数据
                stock_price_data = self.get_stock_price(symbol)
                if stock_price_data:
                    all_stock_prices.append(stock_price_data)
                
                if options_data and not options_data['options_chain'].empty:
                    # 保留更多有用的列
                    columns_to_keep = ['contractSymbol', 'strike', 'lastPrice', 'bid', 'ask', 'volume', 'openInterest', 'impliedVolatility', 'inTheMoney', 'option_type', 'expiry_date']
                    
                    # 检查哪些列存在
                    available_columns = [col for col in columns_to_keep if col in options_data['options_chain'].columns]
                    
                    # 如果某些列不存在，使用替代列名
                    column_mapping = {
                        'strike': 'strike_price',
                        'lastPrice': 'lastPrice',
                        'bid': 'bid',
                        'ask': 'ask',
                        'volume': 'volume',
                        'openInterest': 'openInterest',
                        'impliedVolatility': 'impliedVolatility',
                        'inTheMoney': 'inTheMoney',
                        'option_type': 'option_type',
                        'expiry_date': 'expiry_date'
                    }
                    
                    # 构建最终要保留的列
                    final_columns = []
                    for col in columns_to_keep:
                        if col in options_data['options_chain'].columns:
                            final_columns.append(col)
                        elif col in column_mapping and column_mapping[col] in options_data['options_chain'].columns:
                            final_columns.append(column_mapping[col])
                    
                    # 只保留存在的列
                    filtered_df = options_data['options_chain'][final_columns].copy()
                    
                    # 重命名列以匹配要求的输出格式
                    rename_mapping = {
                        'strike_price': 'strike',
                        'expiry_date': 'expiry_date'
                    }
                    filtered_df = filtered_df.rename(columns=rename_mapping)
                    
                    # 不再创建liquidity_score列
                    
                    # 添加股票代码列
                    filtered_df['symbol'] = symbol
                    
                    all_options_data.append(filtered_df)
                    successful_scans += 1
                    print(f"  ✅ 成功: {len(filtered_df)} 个期权")
                else:
                    failed_scans += 1
                    print(f"  ❌ 失败: 无期权数据")
                
            except Exception as e:
                failed_scans += 1
                print(f"  ❌ 错误: {e}")
            
            # 延时避免请求过于频繁
            if i < len(symbols_to_scan):
                time.sleep(delay)
        
        # 生成时间戳
        timestamp = datetime.now().strftime("%Y%m%d-%H%M")
        
        # 检查 openInterest 为 0 的比例
        should_save_data = True
        if all_options_data:
            combined_df = pd.concat(all_options_data, ignore_index=True)
            
            # 检查 openInterest 列是否存在
            if 'openInterest' in combined_df.columns:
                total_contracts = len(combined_df)
                zero_openinterest_count = (combined_df['openInterest'] == 0).sum()
                zero_openinterest_ratio = zero_openinterest_count / total_contracts
                
                print(f"\n📊 OpenInterest 统计:")
                print(f"总合约数: {total_contracts}")
                print(f"OpenInterest = 0 的合约数: {zero_openinterest_count}")
                print(f"OpenInterest = 0 的比例: {zero_openinterest_ratio:.2%}")
                
                if zero_openinterest_ratio > 0.8:
                    print(f"⚠️  OpenInterest = 0 的比例 ({zero_openinterest_ratio:.2%}) 超过 80%，跳过数据保存")
                    should_save_data = False
                else:
                    print(f"✅ OpenInterest = 0 的比例 ({zero_openinterest_ratio:.2%}) 在可接受范围内，继续保存数据")
            else:
                print("⚠️  未找到 openInterest 列，无法进行数据质量检查")
        
        # 保存期权数据
        options_output_file = None
        if all_options_data and should_save_data:
            combined_df = pd.concat(all_options_data, ignore_index=True)
            options_output_file = os.path.join(self.output_dir, f"all-{timestamp}.csv")
            combined_df.to_csv(options_output_file, index=False, encoding='utf-8-sig')
        
        # 保存股票价格数据
        stock_price_output_file = None
        if all_stock_prices and should_save_data:
            stock_prices_df = pd.DataFrame(all_stock_prices)
            stock_price_output_file = os.path.join(self.stock_price_dir, f"all-{timestamp}.csv")
            stock_prices_df.to_csv(stock_price_output_file, index=False, encoding='utf-8-sig')
        
        print("\n" + "=" * 60)
        print("扫描完成！")
        print("=" * 60)
        print(f"成功扫描: {successful_scans} 个股票")
        print(f"失败扫描: {failed_scans} 个股票")
        
        if not should_save_data:
            print("\n⚠️  由于数据质量检查未通过，未保存任何文件")
            print("   - OpenInterest = 0 的比例超过 80%")
            print("   - 建议检查数据源或稍后重试")
        
        if options_output_file:
            print(f"总期权数: {len(combined_df)} 个")
            print(f"期权数据文件: {options_output_file}")
            print(f"期权文件大小: {os.path.getsize(options_output_file) / 1024:.1f} KB")
            
            # 显示期权统计信息
            print(f"\n期权统计信息:")
            print(f"股票代码数量: {combined_df['symbol'].nunique()}")
            print(f"期权类型分布:")
            option_type_counts = combined_df['option_type'].value_counts()
            for opt_type, count in option_type_counts.items():
                print(f"  {opt_type}: {count} 个")
        elif all_options_data and not should_save_data:
            print(f"\n📊 期权数据统计 (未保存):")
            combined_df = pd.concat(all_options_data, ignore_index=True)
            print(f"总期权数: {len(combined_df)} 个")
            print(f"股票代码数量: {combined_df['symbol'].nunique()}")
        
        if stock_price_output_file:
            print(f"股票价格数据: {len(all_stock_prices)} 个")
            print(f"股票价格文件: {stock_price_output_file}")
            print(f"股票价格文件大小: {os.path.getsize(stock_price_output_file) / 1024:.1f} KB")
        elif all_stock_prices and not should_save_data:
            print(f"\n📊 股票价格数据统计 (未保存):")
            print(f"股票价格数据: {len(all_stock_prices)} 个")
        
        if not all_options_data and not all_stock_prices:
            print("\n❌ 没有获取到任何数据")
            return None, None, None
        
        return options_output_file, combined_df if all_options_data else None, stock_price_output_file

def parse_arguments():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='批量扫描股票期权数据程序')
    
    parser.add_argument('--folder', type=str, default='data',
                       help='数据文件夹路径 (默认: data)')
    
    parser.add_argument('--symbol-file', '-f', type=str, default=None,
                       help='股票代码文件路径 (默认: {folder}/stock_symbol/symbol_market.csv)')
    
    parser.add_argument('--max-deviation', '-m', type=float, default=0.3,
                       help='最大执行价格偏差比例 (默认: 0.3, 即30%%)')
    
    parser.add_argument('--delay', '-d', type=float, default=0.5,
                       help='每个股票之间的延时秒数 (默认: 0.5)')
    
    parser.add_argument('--max-stocks', '-n', type=int, default=None,
                       help='最大扫描股票数量 (默认: 扫描所有)')
    
    return parser.parse_args()

def main():
    """
    主函数
    """
    args = parse_arguments()
    
    # 设置默认的symbol_file路径
    if args.symbol_file is None:
        args.symbol_file = f"{args.folder}/stock_symbol/symbol_market.csv"
    
    print("=" * 60)
    print("批量股票期权数据扫描程序")
    print("=" * 60)
    print(f"数据文件夹: {args.folder}")
    print(f"股票代码文件: {args.symbol_file}")
    print(f"最大偏差比例: {args.max_deviation*100:.0f}%")
    print(f"延时设置: {args.delay}秒")
    print()
    
    # 创建扫描器实例
    scanner = StockOptionsScanner(symbol_file=args.symbol_file, data_folder=args.folder)
    
    # 开始扫描
    options_file, options_df, stock_price_file = scanner.scan_all_stocks(
        max_deviation=args.max_deviation,
        delay=args.delay,
        max_stocks=args.max_stocks
    )
    
    if options_file or stock_price_file:
        print(f"\n✅ 扫描完成！")
        if options_file:
            print(f"期权数据已保存到: {options_file}")
        if stock_price_file:
            print(f"股票价格数据已保存到: {stock_price_file}")
    else:
        print("\n❌ 扫描失败！")
        sys.exit(1)

if __name__ == "__main__":
    main()
