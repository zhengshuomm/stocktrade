#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BABA期权数据分析程序 (使用Yahoo Finance)
从Yahoo Finance获取BABA期权数据，分析期权交易活动
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import os
import time

class BabaOptionsYahooAnalyzer:
    def __init__(self, symbol="BABA"):
        """
        初始化分析器
        
        Args:
            symbol: 股票代码
        """
        self.symbol = symbol
        self.ticker = yf.Ticker(symbol)
        self.output_dir = "data/option_data"
        
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
    
    def get_options_data(self, days=30, max_deviation=0.3):
        """
        获取期权数据
        
        Args:
            days: 获取过去N天的数据
            max_deviation: 最大偏差比例（默认30%）
            
        Returns:
            期权数据字典
        """
        print(f"正在获取 {self.symbol} 期权数据...")
        
        try:
            # 获取期权到期日
            expirations = self.ticker.options
            if not expirations:
                print("未找到期权数据")
                return None
            
            print(f"找到 {len(expirations)} 个到期日")
            
            # 计算日期范围
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            all_options_data = []
            options_chain_data = []
            
            for i, exp_date in enumerate(expirations):  # 处理所有到期日
                print(f"处理到期日 {i+1}/{len(expirations)}: {exp_date}")
                
                try:
                    # 获取期权链
                    opt_chain = self.ticker.option_chain(exp_date)
                    
                    # 处理看涨期权
                    if not opt_chain.calls.empty:
                        calls = opt_chain.calls.copy()
                        calls['option_type'] = 'CALL'
                        calls['expiry_date'] = exp_date
                        calls['strike_price'] = calls['strike']
                        calls['option_code'] = f"{self.symbol}C{exp_date.replace('-', '')}"
                        options_chain_data.append(calls)
                    
                    # 处理看跌期权
                    if not opt_chain.puts.empty:
                        puts = opt_chain.puts.copy()
                        puts['option_type'] = 'PUT'
                        puts['expiry_date'] = exp_date
                        puts['strike_price'] = puts['strike']
                        puts['option_code'] = f"{self.symbol}P{exp_date.replace('-', '')}"
                        options_chain_data.append(puts)
                    
                    time.sleep(0.5)  # 避免请求过于频繁
                    
                except Exception as e:
                    print(f"处理到期日 {exp_date} 时出错: {e}")
                    continue
            
            if not options_chain_data:
                print("未获取到有效的期权数据")
                return None
            
            # 合并所有期权数据
            options_df = pd.concat(options_chain_data, ignore_index=True)
            
            # 获取股票当前价格
            stock_info = self.ticker.info
            current_price = stock_info.get('currentPrice', 0)
            
            # 过滤执行价格差别太大的期权
            if current_price > 0 and not options_df.empty:
                options_df = self._filter_options_by_strike_price(options_df, current_price, max_deviation)
            
            return {
                'options_chain': options_df,
                'current_price': current_price,
                'expirations': expirations,
                'stock_info': stock_info
            }
            
        except Exception as e:
            print(f"获取期权数据时出错: {e}")
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
        print(f"正在过滤执行价格差别太大的期权...")
        print(f"当前股价: ${current_price:.2f}")
        print(f"最大偏差比例: {max_deviation*100:.0f}%")
        
        original_count = len(options_df)
        
        # 计算执行价格与当前价格的偏差比例
        options_df['price_deviation'] = abs(options_df['strike_price'] - current_price) / current_price
        
        # 过滤条件：
        # 1. 看涨期权：执行价格不能超过当前价格的(1+max_deviation)倍
        # 2. 看跌期权：执行价格不能低于当前价格的(1-max_deviation)倍
        # 3. 所有期权：偏差比例不能超过max_deviation
        
        call_mask = (options_df['option_type'] == 'CALL') & (options_df['strike_price'] <= current_price * (1 + max_deviation))
        put_mask = (options_df['option_type'] == 'PUT') & (options_df['strike_price'] >= current_price * (1 - max_deviation))
        deviation_mask = options_df['price_deviation'] <= max_deviation
        
        # 综合过滤条件
        filter_mask = ((call_mask) | (put_mask)) & deviation_mask
        
        filtered_df = options_df[filter_mask].copy()
        
        # 移除临时列
        if 'price_deviation' in filtered_df.columns:
            filtered_df = filtered_df.drop('price_deviation', axis=1)
        
        filtered_count = len(filtered_df)
        removed_count = original_count - filtered_count
        
        print(f"过滤前: {original_count} 个期权")
        print(f"过滤后: {filtered_count} 个期权")
        print(f"移除: {removed_count} 个期权 ({removed_count/original_count*100:.1f}%)")
        
        # 显示被移除的期权类型统计
        if removed_count > 0:
            removed_df = options_df[~filter_mask]
            removed_calls = len(removed_df[removed_df['option_type'] == 'CALL'])
            removed_puts = len(removed_df[removed_df['option_type'] == 'PUT'])
            print(f"移除的看涨期权: {removed_calls} 个")
            print(f"移除的看跌期权: {removed_puts} 个")
        
        return filtered_df
    
    def analyze_options_activity(self, options_data):
        """
        分析期权活动
        
        Args:
            options_data: 期权数据字典
            
        Returns:
            分析结果字典
        """
        print("正在分析期权活动...")
        
        if not options_data:
            return None
        
        options_df = options_data['options_chain']
        current_price = options_data['current_price']
        
        analysis_result = {
            "analysis_time": datetime.now().isoformat(),
            "symbol": self.symbol,
            "current_price": current_price,
            "total_options": len(options_df),
            "analysis": {}
        }
        
        # 1. 期权类型分析
        call_options = options_df[options_df['option_type'] == 'CALL']
        put_options = options_df[options_df['option_type'] == 'PUT']
        
        analysis_result["analysis"]["option_type_analysis"] = {
            "call_options": len(call_options),
            "put_options": len(put_options),
            "call_put_ratio": len(call_options) / len(put_options) if len(put_options) > 0 else 0
        }
        
        # 2. 到期日分析
        expiry_groups = options_df.groupby('expiry_date')
        analysis_result["analysis"]["expiry_analysis"] = {
            "total_expiry_dates": len(expiry_groups),
            "expiry_dates": list(expiry_groups.groups.keys())
        }
        
        # 3. 执行价格分析
        strike_prices = options_df['strike_price'].dropna()
        if not strike_prices.empty:
            analysis_result["analysis"]["strike_price_analysis"] = {
                "min_strike": float(strike_prices.min()),
                "max_strike": float(strike_prices.max()),
                "avg_strike": float(strike_prices.mean()),
                "median_strike": float(strike_prices.median()),
                "current_price": current_price
            }
        
        # 4. 成交量分析
        volume_data = options_df['volume'].dropna()
        if not volume_data.empty:
            analysis_result["analysis"]["volume_analysis"] = {
                "total_volume": int(volume_data.sum()),
                "avg_volume": float(volume_data.mean()),
                "max_volume": int(volume_data.max()),
                "min_volume": int(volume_data.min()),
                "options_with_volume": int((volume_data > 0).sum())
            }
        
        # 5. 价格分析
        price_data = options_df['lastPrice'].dropna()
        if not price_data.empty:
            analysis_result["analysis"]["price_analysis"] = {
                "avg_price": float(price_data.mean()),
                "max_price": float(price_data.max()),
                "min_price": float(price_data.min()),
                "price_std": float(price_data.std())
            }
        
        # 6. 最活跃期权
        if not volume_data.empty:
            most_active = options_df.nlargest(5, 'volume')
            analysis_result["analysis"]["most_active_options"] = [
                {
                    "option_code": row['option_code'],
                    "option_type": row['option_type'],
                    "strike_price": float(row['strike_price']),
                    "expiry_date": row['expiry_date'],
                    "volume": int(row['volume']),
                    "last_price": float(row['lastPrice']) if pd.notna(row['lastPrice']) else 0,
                    "open_interest": int(row['openInterest']) if pd.notna(row['openInterest']) else 0
                }
                for _, row in most_active.iterrows()
            ]
        
        # 7. 价内/价外分析
        if current_price > 0:
            itm_calls = call_options[call_options['strike_price'] < current_price]
            itm_puts = put_options[put_options['strike_price'] > current_price]
            
            analysis_result["analysis"]["moneyness_analysis"] = {
                "itm_calls": len(itm_calls),
                "itm_puts": len(itm_puts),
                "otm_calls": len(call_options) - len(itm_calls),
                "otm_puts": len(put_options) - len(itm_puts)
            }
        
        return analysis_result
    
    def save_analysis_results(self, options_data, analysis_result, days):
        """
        保存分析结果
        
        Args:
            options_data: 期权数据
            analysis_result: 分析结果
            days: 数据天数
        """
        print("正在保存分析结果...")
        
        timestamp = datetime.now().strftime("%Y%m%d-%H%M")
        
        try:
            # 保存期权链数据
            if options_data and not options_data['options_chain'].empty:
                chain_filename = os.path.join(self.output_dir, f"{self.symbol.lower()}_options_{timestamp}.csv")
                
                # 保留所有列
                filtered_df = options_data['options_chain'].copy()
                
                # 只重命名必要的列以保持一致性，避免重复列名
                rename_mapping = {
                    'strike_price': 'strike_price_original',
                    'expiry_date': 'expiry_date'
                }
                filtered_df = filtered_df.rename(columns=rename_mapping)
                
                filtered_df.to_csv(chain_filename, index=False, encoding='utf-8-sig')
                print(f"期权链数据已保存到: {chain_filename}")
                print(f"保留的列: {list(filtered_df.columns)}")
            
        except Exception as e:
            print(f"保存文件时出错: {e}")
    
    def display_analysis_summary(self, analysis_result):
        """
        显示分析结果摘要
        """
        if not analysis_result:
            return
        
        print("\n" + "=" * 60)
        print("期权分析结果摘要")
        print("=" * 60)
        
        print(f"分析时间: {analysis_result['analysis_time']}")
        print(f"股票代码: {analysis_result['symbol']}")
        print(f"当前价格: ${analysis_result['current_price']:.2f}")
        print(f"期权合约总数: {analysis_result['total_options']}")
        
        if 'option_type_analysis' in analysis_result['analysis']:
            type_analysis = analysis_result['analysis']['option_type_analysis']
            print(f"\n期权类型分析:")
            print(f"  看涨期权: {type_analysis['call_options']}")
            print(f"  看跌期权: {type_analysis['put_options']}")
            print(f"  看涨/看跌比例: {type_analysis['call_put_ratio']:.2f}")
        
        if 'volume_analysis' in analysis_result['analysis']:
            volume = analysis_result['analysis']['volume_analysis']
            print(f"\n成交量分析:")
            print(f"  总成交量: {volume['total_volume']:,}")
            print(f"  平均成交量: {volume['avg_volume']:.0f}")
            print(f"  有成交量的期权: {volume['options_with_volume']}")
        
        if 'most_active_options' in analysis_result['analysis']:
            print(f"\n最活跃的期权:")
            for i, option in enumerate(analysis_result['analysis']['most_active_options'][:3], 1):
                print(f"  {i}. {option['option_code']} ({option['option_type']})")
                print(f"     执行价: ${option['strike_price']:.2f}, 成交量: {option['volume']:,}")
                print(f"     最新价: ${option['last_price']:.2f}, 持仓量: {option['open_interest']:,}")
        
        print("\n" + "=" * 60)
        print("分析完成！")
        print("=" * 60)
    
    def run_analysis(self, days=30, max_deviation=0.5):
        """
        运行完整的期权分析
        """
        print("=" * 60)
        print(f"BABA期权数据分析程序 (Yahoo Finance) - 过去{days}天")
        print("=" * 60)
        
        # 获取期权数据
        options_data = self.get_options_data(days, max_deviation)
        if not options_data:
            print("无法获取期权数据，程序退出")
            return
        
        # 分析期权活动
        analysis_result = self.analyze_options_activity(options_data)
        
        # 保存结果
        self.save_analysis_results(options_data, analysis_result, days)
        
        # 显示结果
        self.display_analysis_summary(analysis_result)

def parse_arguments():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='BABA期权数据分析程序 (Yahoo Finance)')
    
    parser.add_argument('--days', '-d', type=int, default=30,
                       help='分析过去N天的数据 (默认: 30)')
    
    parser.add_argument('--symbol', '-s', type=str, default='BABA',
                       help='股票代码 (默认: BABA)')
    
    parser.add_argument('--max-deviation', '-m', type=float, default=0.3,
                       help='最大执行价格偏差比例 (默认: 0.3, 即30%%)')
    
    return parser.parse_args()

def main():
    """
    主函数
    """
    args = parse_arguments()
    
    print("BABA期权数据分析程序 (使用Yahoo Finance)")
    print(f"股票代码: {args.symbol}")
    print(f"分析天数: {args.days}")
    print(f"最大偏差比例: {args.max_deviation*100:.0f}%")
    print()
    
    # 创建分析器实例
    analyzer = BabaOptionsYahooAnalyzer(symbol=args.symbol)
    
    # 运行分析
    analyzer.run_analysis(days=args.days, max_deviation=args.max_deviation)

if __name__ == "__main__":
    main()
