#!/usr/bin/env python3
"""
Discord异常数据发送模块
从find_outliers_by_*.py中抽取的Discord功能，提供统一的异常数据发送接口

Discord表格列含义说明：
==========================================
股票统计表格中的各列含义和判断条件：

【看涨】- 看涨信号数量
判断条件：signal_type属于以下类型且should_count=True
- 多头买 Call，看涨
- 空头平仓 Call，回补信号，看涨  
- 买 Call，看涨
- 空头卖 Put，看涨/看不跌
- 多头平仓 Put，减仓，看跌减弱
- 卖 Put，看涨/对冲
- 卖 Put，看涨

【看跌】- 看跌信号数量  
判断条件：signal_type属于以下类型且should_count=True
- 空头卖 Call，看跌/看不涨
- 多头平仓 Call，减仓，看涨减弱
- 卖 Call，看空/价差对冲
- 卖 Call，看跌
- 多头买 Put，看跌
- 买 Put，看跌

【看涨C】- 看涨Call期权金额
判断条件：signal_type属于看涨Call类型且option_type='CALL'且should_count=True
- 多头买 Call，看涨
- 空头平仓 Call，回补信号，看涨
- 买 Call，看涨

【看跌C】- 看跌Call期权金额
判断条件：signal_type属于看跌Call类型且option_type='CALL'且should_count=True  
- 空头卖 Call，看跌/看不涨
- 多头平仓 Call，减仓，看涨减弱
- 卖 Call，看空/价差对冲
- 卖 Call，看跌

【看涨P】- 看涨Put期权金额
判断条件：signal_type属于看涨Put类型且option_type='PUT'且should_count=True
- 空头卖 Put，看涨/看不跌
- 多头平仓 Put，减仓，看跌减弱
- 卖 Put，看涨/对冲
- 卖 Put，看涨

【看跌P】- 看跌Put期权金额
判断条件：signal_type属于看跌Put类型且option_type='PUT'且should_count=True
- 多头买 Put，看跌
- 买 Put，看跌

排除的信号类型（不参与统计）：
- 空头平仓Put，回补，看跌信号减弱
- 买Call平仓/做波动率交易
- 买Put平仓/做波动率交易

金额计算：使用amount_threshold的绝对值
==========================================
"""

import discord
import asyncio
import gc
from datetime import datetime
import sys
import os

# 添加rules目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'rules'))
from signal_classification_rules import classify_signal
from pytz import timezone
import pandas as pd


class DiscordOutlierSender:
    """Discord 异常数据发送器类"""
    
    def __init__(self, message_title="异常", data_folder="data", time_range=None, stock_prices=None):
        """
        初始化Discord发送器
        
        Args:
            message_title (str): 消息标题，如"OI异常"或"Volume异常"
            data_folder (str): 数据文件夹路径，用于确定执行类型
            time_range (str): 时间范围，格式: "20251010-1336 to 20251010-1354"
            stock_prices (dict): 股票价格信息，格式: {symbol: {"new": price, "old": price}}
        """
        # 从 discord_outlier_sender.py 中获取的配置
        self.token = "MTQyMjQ0NDY2OTg5MTI1MjI0NQ.GXPW4w.N9gMYn_3hOs4TNVbj9JIt_47PPTV8Dc4uB_aJk"
        self.channel_id = 1422402343135088663
        self.message_title = message_title
        self.data_folder = data_folder
        self.time_range = time_range  # 格式: "20251010-1336 to 20251010-1354"
        self.stock_prices = stock_prices or {}  # 格式: {symbol: {"new": price, "old": price}}
        
    def _colorize_signal_type(self, signal_type):
        """为信号类型添加颜色"""
        if "看涨" in signal_type:
            return f"🔴 {signal_type}"
        elif "看跌" in signal_type:
            return f"🟢 {signal_type}"
        else:
            return signal_type
    
    def _format_sig2_percent(self, x):
        """格式化为两位有效数字的百分比（不使用科学计数法）"""
        try:
            x = float(x)
            if x == 0:
                return "0%"
            # 转换为百分比
            percent = x * 100
            s = f"{percent:.2g}%"
            if 'e' in s or 'E' in s:
                from decimal import Decimal
                s = f"{format(Decimal(str(percent)), 'f')}%"
            return s
        except Exception:
            return "N/A"
    
    def _format_amount(self, amount):
        """格式化金额为易读格式（如15B、1M等）"""
        try:
            amount = float(amount)
            if amount == 0:
                return "$0"
            elif amount >= 1_000_000_000:  # 10亿以上
                return f"${amount/1_000_000_000:.0f}B"
            elif amount >= 1_000_000:  # 100万以上
                return f"${amount/1_000_000:.0f}M"
            elif amount >= 1_000:  # 1000以上
                return f"${amount/1_000:.0f}K"
            else:
                return f"${amount:,.0f}"
        except Exception:
            return "$0"
    
    def format_outlier_message(self, row, outlier_type="oi"):
        """
        格式化异常数据消息为Discord嵌入消息
        
        Args:
            row: 异常数据行
            outlier_type (str): 异常类型，"oi" 或 "volume"
        """
        symbol = row.get('symbol', 'N/A')
        contract_symbol = row.get('contractSymbol', 'N/A')
        strike = row.get('strike', 'N/A')
        expiry_date = row.get('expiry_date', 'N/A')
        signal_type = row.get('signal_type', 'N/A')
        amount_threshold = row.get('amount_threshold', 0)
        stock_change_pct = row.get('stock_price_change_pct', 0)
        option_change_pct = row.get('option_price_change_pct', 0)
        amount_tier = row.get('amount_tier', 'N/A')
        yahoo_url = f"https://finance.yahoo.com/quote/{contract_symbol}"
        
        # 根据金额档位设置前缀和颜色
        if amount_tier == ">50M":
            prefix = "!!!!! "
            color_emoji = "🔴"
        elif amount_tier == "10M-50M":
            prefix = "! "
            color_emoji = "🟠"
        else:
            prefix = ""
            color_emoji = "⚪"
        
        # 创建Discord嵌入消息
        embed = discord.Embed(
            title=f"{color_emoji} {prefix}{self.message_title} **** {symbol} ****",
            color=0xff0000 if amount_tier == ">50M" else (0xff8c00 if amount_tier == "10M-50M" else 0xffffff),
            timestamp=datetime.now()
        )
        # 让标题可点击跳转
        try:
            embed.url = yahoo_url
        except Exception:
            pass

        # 处理信号类型颜色
        colored_signal_type = self._colorize_signal_type(signal_type)
        
        # 添加字段
        embed.add_field(
            name="📊 合约信息",
            value=f"**Symbol**: `{symbol}`\n**Strike**: ${strike}\n**Expiry**: {expiry_date}",
            inline=True
        )
        
        # 根据异常类型显示不同的变化数据
        if outlier_type == "oi":
            oi_change_abs = row.get('oi_change_abs', 0)
            open_interest_new = row.get('openInterest_new', row.get('openInterest', 0))
            open_interest_old = row.get('openInterest_old', 0)
            last_price_new = row.get('lastPrice_new', row.get('lastPrice', 0))
            last_price_old = row.get('lastPrice_old', 0)
            
            embed.add_field(
                name="📈 变化数据",
                value=f"**OI变化**: {oi_change_abs:,.0f}\n**OI(new)**: {open_interest_new:,.0f}\n**OI(old)**: {open_interest_old:,.0f}",
                inline=True
            )
            
            embed.add_field(
                name="🔢 数值",
                value=f"**期权价格(new)**: ${last_price_new}\n**期权价格(old)**: ${last_price_old}\n**期权变化**: {option_change_pct:.2f}%",
                inline=True
            )
        else:  # volume
            volume_change_abs = row.get('volume_change_abs', 0)
            volume_new_val = row.get('volume_new', row.get('volume', 0))
            last_price_new = row.get('lastPrice_new', row.get('lastPrice', 0))
            last_price_old = row.get('lastPrice_old', 0)
            
            embed.add_field(
                name="📈 变化数据",
                value=f"**Volume变化**: {volume_change_abs:,.0f}\n**Volume(new)**: {volume_new_val:,.0f}",
                inline=True
            )
            
            embed.add_field(
                name="🔢 数值",
                value=f"**期权价格(new)**: ${last_price_new}\n**期权价格(old)**: ${last_price_old}\n**期权变化**: {option_change_pct:.2f}%",
                inline=True
            )
        
        # 添加股票价格字段
        if symbol in self.stock_prices:
            stock_price_info = self.stock_prices[symbol]
            stock_price_new = stock_price_info.get('new', 'N/A')
            stock_price_old = stock_price_info.get('old', 'N/A')
            stock_price_open = stock_price_info.get('new_open', 'N/A')
            stock_price_old_open = stock_price_info.get('old_open', 'N/A')
            stock_price_high = stock_price_info.get('new_high', 'N/A')
            stock_price_low = stock_price_info.get('new_low', 'N/A')
            
            # 格式化价格，保留2位小数
            if stock_price_new != 'N/A':
                stock_price_new = f"{float(stock_price_new):.2f}"
            if stock_price_old != 'N/A':
                stock_price_old = f"{float(stock_price_old):.2f}"
            if stock_price_open != 'N/A':
                stock_price_open = f"{float(stock_price_open):.2f}"
            if stock_price_high != 'N/A':
                stock_price_high = f"{float(stock_price_high):.2f}"
            if stock_price_low != 'N/A':
                stock_price_low = f"{float(stock_price_low):.2f}"
            
            # 计算股票趋势（第一个高低平：当前open与昨天close比较；第二个高低平：当前close与当前open比较）
            trend_text = "N/A"
            if (stock_price_new != 'N/A' and stock_price_old != 'N/A' and stock_price_open != 'N/A' and stock_price_old_open != 'N/A'):
                try:
                    new_price = float(stock_price_new)  # 当前收盘价
                    old_price = float(stock_price_old)  # 昨天收盘价
                    open_price = float(stock_price_open)  # 当前开盘价
                    old_open_price = float(stock_price_old_open)  # 昨天开盘价

                    # 计算趋势
                    open_vs_old_pct = (open_price - old_price) / old_price if old_price != 0 else 0.0
                    close_vs_open_pct = (new_price - open_price) / open_price if open_price != 0 else 0.0
                    
                    # 检查是否为数据未更新（只有当开盘价完全相同且收盘价也几乎相同时）
                    if (abs(open_price - old_open_price) < 0.01 and 
                        abs(new_price - old_price) < 0.01):
                        trend_text = "数据未更新"
                    else:
                        # 第一个高低平：当前open与昨天close比较
                        is_high_open = open_vs_old_pct > 0.01   # 高开：开盘价比昨收高超过1%
                        is_low_open = open_vs_old_pct < -0.01  # 低开：开盘价比昨收低超过1%
                        is_flat_open = abs(open_vs_old_pct) <= 0.01  # 平开：开盘价与昨收价差在1%以内

                        # 第二个高低平：当前close与当前open比较
                        is_high_close = close_vs_open_pct > 0.01   # 高走：收盘价比开盘高超过1%
                        is_low_close = close_vs_open_pct < -0.01  # 低走：收盘价比开盘低超过1%
                        is_flat_close = abs(close_vs_open_pct) <= 0.01  # 平走：收盘价与开盘价差在1%以内

                        # 组合判定
                        if is_high_open and is_high_close:
                            trend_text = "🔴高开高走"  # 红色
                        elif is_high_open and is_low_close:
                            trend_text = "🟢高开低走"  # 绿色
                        elif is_high_open and is_flat_close:
                            trend_text = "🔴高开平走"  # 红色
                        elif is_low_open and is_high_close:
                            trend_text = "🔴低开高走"  # 红色
                        elif is_low_open and is_low_close:
                            trend_text = "🟢低开低走"  # 绿色
                        elif is_low_open and is_flat_close:
                            trend_text = "🟢低开平走"  # 绿色
                        elif is_flat_open and is_high_close:
                            trend_text = "🔴平开高走"  # 红色
                        elif is_flat_open and is_low_close:
                            trend_text = "🟢平开低走"  # 绿色
                        elif is_flat_open and is_flat_close:
                            trend_text = "平开平走"
                        else:
                            trend_text = "平开平走"
                except (ValueError, TypeError):
                    trend_text = "N/A"
            
            embed.add_field(
                name="💰 股票价格",
                value=f"**股票价格(old)**: ${stock_price_old}\n**股票价格(new close)**: ${stock_price_new}\n**股票价格(new open)**: ${stock_price_open}\n**股票价格(new high)**: ${stock_price_high}\n**股票价格(new low)**: ${stock_price_low}\n**股票变化**: {stock_change_pct:.2f}%\n**股票趋势**: {trend_text}",
                inline=True
            )

        # 占总市值（两位有效数字，显示为百分比）
        amt_to_mc = row.get('amount_to_market_cap', None)
        if amt_to_mc is None:
            amt_pct = row.get('amount_to_market_cap_pct', None)
            if amt_pct is not None:
                try:
                    amt_to_mc = float(amt_pct) / 100.0
                except Exception:
                    amt_to_mc = None
        
        embed.add_field(
            name="🚨 异常信号",
            value=f"**信号类型**: {colored_signal_type}\n**金额门槛**: ${amount_threshold:,.0f}\n**金额档位**: {amount_tier}\n**占总市值**: {self._format_sig2_percent(amt_to_mc)}",
            inline=True
        )

        # 添加Yahoo链接
        embed.add_field(
            name="🔗 Yahoo",
            value=yahoo_url,
            inline=False
        )

        # 添加时间范围字段
        if self.time_range:
            embed.add_field(
                name="⏰ 时间范围",
                value=f"**比较时段**: {self.time_range}",
                inline=True
            )
        
        # 设置footer
        embed.set_footer(text=f"检测时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return embed
        
    async def send_outliers(self, outliers_df, outlier_type="oi", high_amount_but_not_outlier_df=None, signal_type_stats=None, csv_file_path=None):
        """
        发送异常数据到 Discord
        
        Args:
            outliers_df: 异常数据DataFrame
            outlier_type (str): 异常类型，"oi" 或 "volume"
            high_amount_but_not_outlier_df: 大于500万但不满足异常条件的数据（可选）
        """
        if outliers_df.empty:
            print("没有异常数据需要发送到 Discord")
            return
            
        client = None
        try:
            client = discord.Client(intents=discord.Intents.default())
            
            @client.event
            async def on_ready():
                try:
                    print(f'Discord Bot登录成功: {client.user}')
                    channel = client.get_channel(self.channel_id)
                    
                    if not channel:
                        print("❌ Discord频道未找到!")
                        return
                    
                    print(f"开始发送汇总统计到 Discord...")
                    
                    # 生成时间戳
                    pst_timestamp = (datetime.now().astimezone(timezone('US/Pacific'))).strftime("%Y%m%d-%H%M")
                    
                    # 确定执行类型
                    execution_type = "GENERAL Execution" if self.data_folder == "data" else "Priority Execution"
                    
                    # 发送汇总统计
                    stats_message = "******************************************\n"
                    stats_message += f"# {pst_timestamp} PST #\n"
                    stats_message += f"{execution_type}\n"
                    stats_message += f"🔍 **{self.message_title}检测结果**\n"
                    stats_message += f"📊 检测到 {len(outliers_df)} 个异常合约\n"
                    
                    if "symbol" in outliers_df.columns and "signal_type" in outliers_df.columns:
                        st = outliers_df["signal_type"].astype(str)
                        outliers_df_copy = outliers_df.copy()
                        
                        # 使用导入的分类函数
                        def classify_signal_wrapper(row):
                            signal_type = str(row["signal_type"])
                            option_type = str(row["option_type"]).upper()
                            return classify_signal(signal_type, option_type)
                        
                        # 应用分类
                        classification = outliers_df_copy.apply(classify_signal_wrapper, axis=1, result_type='expand')
                        outliers_df_copy["is_bullish"] = classification['is_bullish']
                        outliers_df_copy["is_bearish"] = classification['is_bearish'] 
                        outliers_df_copy["is_call"] = classification['is_call']
                        outliers_df_copy["is_put"] = classification['is_put']
                        outliers_df_copy["should_count"] = classification['should_count']
                        
                        # 计算金额 (使用amount_threshold的绝对值)
                        outliers_df_copy["amount"] = outliers_df_copy["amount_threshold"].abs()
                        
                        # 按股票分组统计
                        def calculate_amounts(group):
                            # 只统计should_count=True的记录
                            countable_group = group[group["should_count"]]
                            bullish_call = countable_group[(countable_group["is_bullish"]) & (countable_group["is_call"])]["amount"].sum()
                            bearish_call = countable_group[(countable_group["is_bearish"]) & (countable_group["is_call"])]["amount"].sum()
                            bullish_put = countable_group[(countable_group["is_bullish"]) & (countable_group["is_put"])]["amount"].sum()
                            bearish_put = countable_group[(countable_group["is_bearish"]) & (countable_group["is_put"])]["amount"].sum()
                            return pd.Series({
                                'bullish_call_amount': bullish_call,
                                'bearish_call_amount': bearish_call,
                                'bullish_put_amount': bullish_put,
                                'bearish_put_amount': bearish_put,
                                'total_count': len(countable_group)
                            })
                        
                        grouped = outliers_df_copy.groupby("symbol").apply(calculate_amounts, include_groups=False).reset_index()
                        grouped = grouped.sort_values(by=["total_count"], ascending=[False])
                        
                        # 添加文件链接
                        if csv_file_path:
                            # 确定文件夹类型
                            folder_type = "outlier" if outlier_type == "oi" else "volume_outlier"
                            # 构建GitHub链接
                            github_url = f"https://github.com/zhengshuomm/stocktrade/blob/main/{self.data_folder}/{folder_type}/{csv_file_path.split('/')[-1]}"
                            stats_message += f"\n📁 **数据文件:** {github_url}\n"
                        
                        stats_message += "\n📈 **按股票统计:**\n"
                        stats_message += "```\n"
                        stats_message += f"{'股票':<2} {'看涨':>3} {'看跌':>3} {'看涨C':>2} {'看跌C':>2} {'看涨P':>2} {'看跌P':>2}\n"
                        stats_message += "-" * 35 + "\n"
                        
                        # 只显示前25个股票
                        display_count = min(25, len(grouped))
                        for i, (_, row) in enumerate(grouped.iterrows()):
                            if i >= display_count:
                                break
                                
                            sym = row["symbol"]
                            total_count = int(row['total_count'])
                            
                            # 计算看涨/看跌合约数量
                            bullish_count = 0
                            bearish_count = 0
                            
                            # 从原始数据中计算看涨/看跌数量（只统计should_count=True的）
                            symbol_data = outliers_df_copy[outliers_df_copy['symbol'] == sym]
                            countable_data = symbol_data[symbol_data['should_count']]
                            if not countable_data.empty:
                                bullish_count = int(countable_data['is_bullish'].sum())
                                bearish_count = int(countable_data['is_bearish'].sum())
                            else:
                                bullish_count = 0
                                bearish_count = 0
                            
                            # 格式化金额（更紧凑的格式）
                            bull_call = self._format_amount(row['bullish_call_amount']).replace('$', '')
                            bear_call = self._format_amount(row['bearish_call_amount']).replace('$', '')
                            bull_put = self._format_amount(row['bullish_put_amount']).replace('$', '')
                            bear_put = self._format_amount(row['bearish_put_amount']).replace('$', '')
                            
                            stats_message += f"{sym:<4} {bullish_count:>3} {bearish_count:>3} {bull_call:>4} {bear_call:>4} {bull_put:>4} {bear_put:>4}\n"
                        
                        if len(grouped) > display_count:
                            stats_message += f"... 还有 {len(grouped) - display_count} 个股票\n"
                        
                        stats_message += "```"
                        
                        # 添加按股票统计（考虑今日股票变化）
                        stats_message += "\n📈 **按股票统计（考虑今日股票变化）:**\n"
                        stats_message += "```\n"
                        stats_message += f"{'股票':<2} {'看涨':>3} {'看跌':>3} {'看涨C':>2} {'看跌C':>2} {'看涨P':>2} {'看跌P':>2}\n"
                        stats_message += "-" * 35 + "\n"
                        
                        # 计算考虑股票趋势的统计
                        def calculate_trend_filtered_amounts(group):
                            # 只统计should_count=True的记录
                            countable_group = group[group["should_count"]]
                            if countable_group.empty:
                                return pd.Series({
                                    'bullish_call_amount': 0,
                                    'bearish_call_amount': 0,
                                    'bullish_put_amount': 0,
                                    'bearish_put_amount': 0,
                                    'total_count': 0
                                })
                            
                            # 获取股票趋势信息
                            symbol = group.iloc[0]['symbol']
                            trend_text = "N/A"
                            if symbol in self.stock_prices:
                                stock_price_info = self.stock_prices[symbol]
                                stock_price_new = stock_price_info.get('new', 'N/A')
                                stock_price_old = stock_price_info.get('old', 'N/A')
                                stock_price_open = stock_price_info.get('new_open', 'N/A')
                                stock_price_old_open = stock_price_info.get('old_open', 'N/A')
                                
                                if (stock_price_new != 'N/A' and stock_price_old != 'N/A' and 
                                    stock_price_open != 'N/A' and stock_price_old_open != 'N/A'):
                                    try:
                                        new_price = float(stock_price_new)
                                        old_price = float(stock_price_old)
                                        open_price = float(stock_price_open)
                                        old_open_price = float(stock_price_old_open)
                                        
                                        # 计算趋势
                                        open_vs_old_pct = (open_price - old_price) / old_price if old_price != 0 else 0.0
                                        close_vs_open_pct = (new_price - open_price) / open_price if open_price != 0 else 0.0
                                        
                                        # 检查是否为数据未更新（只有当开盘价完全相同且收盘价也几乎相同时）
                                        if (abs(open_price - old_open_price) < 0.01 and 
                                            abs(new_price - old_price) < 0.01):
                                            trend_text = "数据未更新"
                                        else:
                                            
                                            is_high_open = open_vs_old_pct > 0.01
                                            is_low_open = open_vs_old_pct < -0.01
                                            is_flat_open = abs(open_vs_old_pct) <= 0.01
                                            
                                            is_high_close = close_vs_open_pct > 0.01
                                            is_low_close = close_vs_open_pct < -0.01
                                            is_flat_close = abs(close_vs_open_pct) <= 0.01
                                            
                                            # 组合判定
                                            if is_high_open and is_high_close:
                                                trend_text = "🔴高开高走"
                                            elif is_high_open and is_low_close:
                                                trend_text = "🟢高开低走"
                                            elif is_high_open and is_flat_close:
                                                trend_text = "🔴高开平走"
                                            elif is_low_open and is_high_close:
                                                trend_text = "🔴低开高走"
                                            elif is_low_open and is_low_close:
                                                trend_text = "🟢低开低走"
                                            elif is_low_open and is_flat_close:
                                                trend_text = "🟢低开平走"
                                            elif is_flat_open and is_high_close:
                                                trend_text = "🔴平开高走"
                                            elif is_flat_open and is_low_close:
                                                trend_text = "🟢平开低走"
                                            elif is_flat_open and is_flat_close:
                                                trend_text = "平开平走"
                                            else:
                                                trend_text = "平开平走"
                                    except (ValueError, TypeError):
                                        trend_text = "N/A"
                            
                            # 根据趋势过滤数据
                            bullish_trends = ["🔴高开高走", "🔴低开高走", "🔴平开高走", "🔴高开平走"]
                            bearish_trends = ["🟢高开低走", "🟢低开低走", "🟢平开低走", "🟢低开平走"]
                            
                            # 过滤看涨信号
                            bullish_filtered = countable_group[
                                (countable_group["is_bullish"]) & 
                                (trend_text in bullish_trends)
                            ]
                            # 过滤看跌信号
                            bearish_filtered = countable_group[
                                (countable_group["is_bearish"]) & 
                                (trend_text in bearish_trends)
                            ]
                            
                            # 计算金额
                            bullish_call = bullish_filtered[
                                (bullish_filtered["is_call"])
                            ]["amount"].sum()
                            bearish_call = bearish_filtered[
                                (bearish_filtered["is_call"])
                            ]["amount"].sum()
                            bullish_put = bullish_filtered[
                                (bullish_filtered["is_put"])
                            ]["amount"].sum()
                            bearish_put = bearish_filtered[
                                (bearish_filtered["is_put"])
                            ]["amount"].sum()
                            
                            return pd.Series({
                                'bullish_call_amount': bullish_call,
                                'bearish_call_amount': bearish_call,
                                'bullish_put_amount': bullish_put,
                                'bearish_put_amount': bearish_put,
                                'total_count': len(bullish_filtered) + len(bearish_filtered),
                                'bullish_count': len(bullish_filtered),
                                'bearish_count': len(bearish_filtered)
                            })
                        
                        # 按股票分组计算趋势过滤后的统计
                        trend_filtered_results = []
                        for symbol in outliers_df_copy['symbol'].unique():
                            symbol_data = outliers_df_copy[outliers_df_copy['symbol'] == symbol]
                            result = calculate_trend_filtered_amounts(symbol_data)
                            result['symbol'] = symbol
                            trend_filtered_results.append(result)
                        
                        trend_filtered_grouped = pd.DataFrame(trend_filtered_results)
                        trend_filtered_grouped = trend_filtered_grouped.sort_values(by=["total_count"], ascending=[False])
                        
                        # 过滤掉看涨和看跌都为0的股票
                        filtered_trend_grouped = trend_filtered_grouped[
                            (trend_filtered_grouped['bullish_count'] > 0) | 
                            (trend_filtered_grouped['bearish_count'] > 0)
                        ]
                        
                        # 只显示前25个股票
                        display_count = min(25, len(filtered_trend_grouped))
                        for i, (_, row) in enumerate(filtered_trend_grouped.iterrows()):
                            if i >= display_count:
                                break
                                
                            sym = row["symbol"]
                            total_count = int(row['total_count'])
                            
                            # 计算看涨/看跌合约数量（趋势过滤后）
                            symbol_data = outliers_df_copy[outliers_df_copy['symbol'] == sym]
                            countable_data = symbol_data[symbol_data['should_count']]
                            
                            # 获取股票趋势
                            trend_text = "N/A"
                            if sym in self.stock_prices:
                                stock_price_info = self.stock_prices[sym]
                                stock_price_new = stock_price_info.get('new', 'N/A')
                                stock_price_old = stock_price_info.get('old', 'N/A')
                                stock_price_open = stock_price_info.get('new_open', 'N/A')
                                stock_price_old_open = stock_price_info.get('old_open', 'N/A')
                                
                                if (stock_price_new != 'N/A' and stock_price_old != 'N/A' and 
                                    stock_price_open != 'N/A' and stock_price_old_open != 'N/A'):
                                    try:
                                        new_price = float(stock_price_new)
                                        old_price = float(stock_price_old)
                                        open_price = float(stock_price_open)
                                        old_open_price = float(stock_price_old_open)
                                        
                                        # 计算趋势
                                        open_vs_old_pct = (open_price - old_price) / old_price if old_price != 0 else 0.0
                                        close_vs_open_pct = (new_price - open_price) / open_price if open_price != 0 else 0.0
                                        
                                        # 检查是否为数据未更新（只有当开盘价完全相同且收盘价也几乎相同时）
                                        if (abs(open_price - old_open_price) < 0.01 and 
                                            abs(new_price - old_price) < 0.01):
                                            trend_text = "数据未更新"
                                        else:
                                            is_high_open = open_vs_old_pct > 0.01
                                            is_low_open = open_vs_old_pct < -0.01
                                            is_flat_open = abs(open_vs_old_pct) <= 0.01
                                            
                                            is_high_close = close_vs_open_pct > 0.01
                                            is_low_close = close_vs_open_pct < -0.01
                                            is_flat_close = abs(close_vs_open_pct) <= 0.01
                                            
                                            if is_high_open and is_high_close:
                                                trend_text = "🔴高开高走"
                                            elif is_high_open and is_low_close:
                                                trend_text = "🟢高开低走"
                                            elif is_high_open and is_flat_close:
                                                trend_text = "🔴高开平走"
                                            elif is_low_open and is_high_close:
                                                trend_text = "🔴低开高走"
                                            elif is_low_open and is_low_close:
                                                trend_text = "🟢低开低走"
                                            elif is_low_open and is_flat_close:
                                                trend_text = "🟢低开平走"
                                            elif is_flat_open and is_high_close:
                                                trend_text = "🔴平开高走"
                                            elif is_flat_open and is_low_close:
                                                trend_text = "🟢平开低走"
                                            elif is_flat_open and is_flat_close:
                                                trend_text = "平开平走"
                                            else:
                                                trend_text = "平开平走"
                                    except (ValueError, TypeError):
                                        trend_text = "N/A"
                            
                            # 根据趋势过滤
                            bullish_trends = ["🔴高开高走", "🔴低开高走", "🔴平开高走", "🔴高开平走"]
                            bearish_trends = ["🟢高开低走", "🟢低开低走", "🟢平开低走", "🟢低开平走"]
                            
                            bullish_count = 0
                            bearish_count = 0
                            
                            if not countable_data.empty:
                                if trend_text in bullish_trends:
                                    bullish_count = int(countable_data['is_bullish'].sum())
                                if trend_text in bearish_trends:
                                    bearish_count = int(countable_data['is_bearish'].sum())
                            
                            # 格式化金额（更紧凑的格式）
                            bull_call = self._format_amount(row['bullish_call_amount']).replace('$', '')
                            bear_call = self._format_amount(row['bearish_call_amount']).replace('$', '')
                            bull_put = self._format_amount(row['bullish_put_amount']).replace('$', '')
                            bear_put = self._format_amount(row['bearish_put_amount']).replace('$', '')
                            
                            stats_message += f"{sym:<4} {bullish_count:>3} {bearish_count:>3} {bull_call:>4} {bear_call:>4} {bull_put:>4} {bear_put:>4}\n"
                        
                        if len(filtered_trend_grouped) > display_count:
                            stats_message += f"... 还有 {len(filtered_trend_grouped) - display_count} 个股票\n"
                        
                        stats_message += "```"
                        
                    
                    # 添加大于500万但不满足异常条件的统计
                    if high_amount_but_not_outlier_df is not None and not high_amount_but_not_outlier_df.empty:
                        stats_message += "\n💰 **大于500万但未触发异常条件:**\n"
                        
                        # 按股票分组统计
                        high_amount_copy = high_amount_but_not_outlier_df.copy()
                        high_amount_copy["is_call"] = high_amount_copy["option_type"].str.contains("Call", case=False, na=False)
                        high_amount_copy["is_put"] = high_amount_copy["option_type"].str.contains("Put", case=False, na=False)
                        # 计算金额 (使用amount_threshold的绝对值)
                        high_amount_copy["amount"] = high_amount_copy["amount_threshold"].abs()
                        
                        def calculate_high_amount_stats(group):
                            call_amount = group[group["is_call"]]["amount"].sum()
                            put_amount = group[group["is_put"]]["amount"].sum()
                            return pd.Series({
                                'call_amount': call_amount,
                                'put_amount': put_amount,
                                'total_count': len(group)
                            })
                        
                        high_amount_grouped = high_amount_copy.groupby("symbol").apply(calculate_high_amount_stats, include_groups=False).reset_index()
                        high_amount_grouped = high_amount_grouped.sort_values(by=["total_count"], ascending=[False])
                        
                        for _, row in high_amount_grouped.iterrows():
                            sym = row["symbol"]
                            call_amount = self._format_amount(row['call_amount'])
                            put_amount = self._format_amount(row['put_amount'])
                            # 获取股票当前价格
                            stock_price = high_amount_copy[high_amount_copy["symbol"] == sym]["Close"].iloc[0] if "Close" in high_amount_copy.columns else "N/A"
                            stock_price_str = f"${stock_price:.2f}" if stock_price != "N/A" else "N/A"
                            stats_message += f"• {sym}: 当前价格 {stock_price_str}, Call {call_amount}, Put {put_amount}\n"
                    
                    # 添加异常类型统计
                    if signal_type_stats:
                        stats_message += "\n📊 **异常类型统计:**\n"
                        for signal_type, count in signal_type_stats.items():
                            stats_message += f"  {signal_type}: {count} 个\n"
                    
                    stats_message += "\n\n"
                    
                    await channel.send(stats_message)
                    print(f"✅ 成功发送汇总统计到 Discord")
                    
                    # 为每个股票symbol发送单个消息（按照"按股票统计（考虑今日股票变化）"的顺序）
                    if "symbol" in outliers_df.columns and "amount_threshold" in outliers_df.columns:
                        # 使用与"按股票统计（考虑今日股票变化）"相同的逻辑和顺序
                        st = outliers_df["signal_type"].astype(str)
                        outliers_df_copy = outliers_df.copy()
                        
                        # 使用导入的分类函数
                        def classify_signal_wrapper(row):
                            signal_type = str(row["signal_type"])
                            option_type = str(row["option_type"]).upper()
                            return classify_signal(signal_type, option_type)
                        
                        # 应用分类
                        classification = outliers_df_copy.apply(classify_signal_wrapper, axis=1, result_type='expand')
                        outliers_df_copy["is_bullish"] = classification['is_bullish']
                        outliers_df_copy["is_bearish"] = classification['is_bearish'] 
                        outliers_df_copy["is_call"] = classification['is_call']
                        outliers_df_copy["is_put"] = classification['is_put']
                        outliers_df_copy["should_count"] = classification['should_count']
                        
                        # 计算每个symbol的趋势过滤后统计
                        def calculate_trend_filtered_amounts_for_individual(group):
                            # 只统计should_count=True的记录
                            countable_group = group[group["should_count"]]
                            if countable_group.empty:
                                return pd.Series({
                                    'bullish_call_amount': 0,
                                    'bearish_call_amount': 0,
                                    'bullish_put_amount': 0,
                                    'bearish_put_amount': 0,
                                    'total_count': 0,
                                    'bullish_count': 0,
                                    'bearish_count': 0
                                })
                            
                            # 获取股票趋势信息
                            symbol = group.iloc[0]['symbol']
                            trend_text = "N/A"
                            if symbol in self.stock_prices:
                                stock_price_info = self.stock_prices[symbol]
                                stock_price_new = stock_price_info.get('new', 'N/A')
                                stock_price_old = stock_price_info.get('old', 'N/A')
                                stock_price_open = stock_price_info.get('new_open', 'N/A')
                                stock_price_old_open = stock_price_info.get('old_open', 'N/A')
                                
                                if (stock_price_new != 'N/A' and stock_price_old != 'N/A' and 
                                    stock_price_open != 'N/A' and stock_price_old_open != 'N/A'):
                                    try:
                                        new_price = float(stock_price_new)
                                        old_price = float(stock_price_old)
                                        open_price = float(stock_price_open)
                                        old_open_price = float(stock_price_old_open)
                                        
                                        # 计算趋势
                                        open_vs_old_pct = (open_price - old_price) / old_price if old_price != 0 else 0.0
                                        close_vs_open_pct = (new_price - open_price) / open_price if open_price != 0 else 0.0
                                        
                                        # 检查是否为数据未更新（只有当开盘价完全相同且收盘价也几乎相同时）
                                        if (abs(open_price - old_open_price) < 0.01 and 
                                            abs(new_price - old_price) < 0.01):
                                            trend_text = "数据未更新"
                                        else:
                                            
                                            is_high_open = open_vs_old_pct > 0.01
                                            is_low_open = open_vs_old_pct < -0.01
                                            is_flat_open = abs(open_vs_old_pct) <= 0.01
                                            
                                            is_high_close = close_vs_open_pct > 0.01
                                            is_low_close = close_vs_open_pct < -0.01
                                            is_flat_close = abs(close_vs_open_pct) <= 0.01
                                            
                                            # 组合判定
                                            if is_high_open and is_high_close:
                                                trend_text = "🔴高开高走"
                                            elif is_high_open and is_low_close:
                                                trend_text = "🟢高开低走"
                                            elif is_high_open and is_flat_close:
                                                trend_text = "🔴高开平走"
                                            elif is_low_open and is_high_close:
                                                trend_text = "🔴低开高走"
                                            elif is_low_open and is_low_close:
                                                trend_text = "🟢低开低走"
                                            elif is_low_open and is_flat_close:
                                                trend_text = "🟢低开平走"
                                            elif is_flat_open and is_high_close:
                                                trend_text = "🔴平开高走"
                                            elif is_flat_open and is_low_close:
                                                trend_text = "🟢平开低走"
                                            elif is_flat_open and is_flat_close:
                                                trend_text = "平开平走"
                                            else:
                                                trend_text = "平开平走"
                                    except (ValueError, TypeError):
                                        trend_text = "N/A"
                            
                            # 根据趋势过滤数据
                            bullish_trends = ["🔴高开高走", "🔴低开高走", "🔴平开高走", "🔴高开平走"]
                            bearish_trends = ["🟢高开低走", "🟢低开低走", "🟢平开低走", "🟢低开平走"]
                            
                            # 过滤看涨信号
                            bullish_filtered = countable_group[
                                (countable_group["is_bullish"]) & 
                                (trend_text in bullish_trends)
                            ]
                            # 过滤看跌信号
                            bearish_filtered = countable_group[
                                (countable_group["is_bearish"]) & 
                                (trend_text in bearish_trends)
                            ]
                            
                            # 计算金额
                            bullish_call = bullish_filtered[
                                (bullish_filtered["is_call"])
                            ]["amount_threshold"].abs().sum()
                            bearish_call = bearish_filtered[
                                (bearish_filtered["is_call"])
                            ]["amount_threshold"].abs().sum()
                            bullish_put = bullish_filtered[
                                (bullish_filtered["is_put"])
                            ]["amount_threshold"].abs().sum()
                            bearish_put = bearish_filtered[
                                (bearish_filtered["is_put"])
                            ]["amount_threshold"].abs().sum()
                            
                            return pd.Series({
                                'bullish_call_amount': bullish_call,
                                'bearish_call_amount': bearish_call,
                                'bullish_put_amount': bullish_put,
                                'bearish_put_amount': bearish_put,
                                'total_count': len(bullish_filtered) + len(bearish_filtered),
                                'bullish_count': len(bullish_filtered),
                                'bearish_count': len(bearish_filtered)
                            })
                        
                        # 计算趋势过滤后的统计
                        trend_filtered_results = []
                        for symbol in outliers_df_copy['symbol'].unique():
                            symbol_data = outliers_df_copy[outliers_df_copy['symbol'] == symbol]
                            result = calculate_trend_filtered_amounts_for_individual(symbol_data)
                            result['symbol'] = symbol
                            trend_filtered_results.append(result)
                        
                        trend_filtered_grouped = pd.DataFrame(trend_filtered_results)
                        trend_filtered_grouped = trend_filtered_grouped.sort_values(by=["total_count"], ascending=[False])
                        
                        # 过滤掉看涨看跌都为0的股票
                        filtered_grouped = trend_filtered_grouped[
                            (trend_filtered_grouped['bullish_count'] > 0) | 
                            (trend_filtered_grouped['bearish_count'] > 0)
                        ]
                        
                        # 找到每个symbol的amount_threshold最大的记录，但只考虑should_count=True的记录
                        if 'should_count' in outliers_df.columns:
                            countable_outliers = outliers_df[outliers_df['should_count'] == True]
                        else:
                            countable_outliers = outliers_df
                        if not countable_outliers.empty:
                            max_records = countable_outliers.loc[countable_outliers.groupby("symbol")["amount_threshold"].idxmax()]
                        else:
                            max_records = pd.DataFrame()
                        
                        # 按照过滤后的顺序重新排列max_records
                        if not filtered_grouped.empty and not max_records.empty:
                            # 只保留有趋势过滤后数据的股票
                            valid_symbols = filtered_grouped['symbol'].tolist()
                            max_records = max_records[max_records['symbol'].isin(valid_symbols)]
                            
                            # 按照趋势过滤后的顺序重新排列
                            if not max_records.empty:
                                try:
                                    max_records = max_records.set_index("symbol").loc[filtered_grouped["symbol"]].reset_index()
                                except KeyError:
                                    # 如果索引操作失败，保持原有顺序
                                    pass
                        
                        success_count = 0
                        if not max_records.empty:
                            for _, row in max_records.iterrows():
                                try:
                                    embed = self.format_outlier_message(row, outlier_type)
                                    await channel.send(embed=embed)
                                    success_count += 1
                                    await asyncio.sleep(0.1)  # 避免发送过快
                                except Exception as e:
                                    print(f"❌ 发送单个消息失败: {e}")
                                    continue
                        
                        print(f"✅ 成功发送 {success_count} 个单个消息到 Discord")
                    
                except Exception as e:
                    import traceback
                    print(f"❌ Discord发送过程中出错: {e}")
                    print(f"错误详情: {traceback.format_exc()}")
                finally:
                    if client:
                        await client.close()
                        await asyncio.sleep(0.1)
                        gc.collect()
            
            await client.start(self.token)
            
        except Exception as e:
            print(f"❌ Discord连接失败: {e}")
        finally:
            if client:
                try:
                    await client.close()
                except:
                    pass
                await asyncio.sleep(0.1)
                gc.collect()


# 便捷函数
async def send_oi_outliers(outliers_df, data_folder="data", time_range=None, stock_prices=None, high_amount_but_not_outlier_df=None, signal_type_stats=None, csv_file_path=None):
    """发送OI异常数据到Discord"""
    sender = DiscordOutlierSender("OI异常", data_folder, time_range, stock_prices)
    await sender.send_outliers(outliers_df, "oi", high_amount_but_not_outlier_df, signal_type_stats, csv_file_path)


async def send_volume_outliers(outliers_df, data_folder="data", time_range=None, stock_prices=None, high_amount_but_not_outlier_df=None, signal_type_stats=None, csv_file_path=None):
    """发送Volume异常数据到Discord"""
    sender = DiscordOutlierSender("Volume异常", data_folder, time_range, stock_prices)
    await sender.send_outliers(outliers_df, "volume", high_amount_but_not_outlier_df, signal_type_stats, csv_file_path)
