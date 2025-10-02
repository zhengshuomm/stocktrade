#!/usr/bin/env python3
"""
Discord异常数据发送模块
从find_outliers_by_*.py中抽取的Discord功能，提供统一的异常数据发送接口
"""

import discord
import asyncio
import gc
from datetime import datetime
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
            title=f"{color_emoji} {prefix}{self.message_title} --- {symbol}",
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
            
            # 格式化价格，保留2位小数
            if stock_price_new != 'N/A':
                stock_price_new = f"{float(stock_price_new):.2f}"
            if stock_price_old != 'N/A':
                stock_price_old = f"{float(stock_price_old):.2f}"
            
            embed.add_field(
                name="💰 股票价格",
                value=f"**股票价格(new)**: ${stock_price_new}\n**股票价格(old)**: ${stock_price_old}\n**股票变化**: {stock_change_pct:.2f}%",
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
        
        if amt_to_mc is not None:
            embed.add_field(
                name="📐 占总市值",
                value=self._format_sig2_percent(amt_to_mc),
                inline=True
            )
        
        embed.add_field(
            name="🚨 异常信号",
            value=f"**信号类型**: {colored_signal_type}\n**金额门槛**: ${amount_threshold:,.0f}\n**金额档位**: {amount_tier}",
            inline=False
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
        
    async def send_outliers(self, outliers_df, outlier_type="oi", high_amount_but_not_outlier_df=None):
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
                        
                        # 精确分类逻辑
                        def classify_signal(row):
                            signal_type = str(row["signal_type"])
                            option_type = str(row["option_type"]).upper()
                            
                            # 不统计的信号类型
                            exclude_signals = [
                                "空头平仓Put，回补，看跌信号减弱",
                                "买Call平仓/做波动率交易", 
                                "买Put平仓/做波动率交易"
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
                        classification = outliers_df_copy.apply(classify_signal, axis=1, result_type='expand')
                        outliers_df_copy["is_bullish"] = classification[0]
                        outliers_df_copy["is_bearish"] = classification[1] 
                        outliers_df_copy["is_call"] = classification[2]
                        outliers_df_copy["is_put"] = classification[3]
                        outliers_df_copy["should_count"] = classification[4]
                        
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
                        
                        stats_message += "\n📈 **按股票统计:**\n"
                        for _, row in grouped.iterrows():
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
                            
                            # 格式化金额
                            bull_call = self._format_amount(row['bullish_call_amount'])
                            bear_call = self._format_amount(row['bearish_call_amount'])
                            bull_put = self._format_amount(row['bullish_put_amount'])
                            bear_put = self._format_amount(row['bearish_put_amount'])
                            
                            stats_message += f"• {sym}: 看涨 {bullish_count} 个, 看跌 {bearish_count} 个, 看涨Call {bull_call}, 看跌Call {bear_call}, 看涨Put {bull_put}, 看跌Put {bear_put}\n"
                    
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
                    
                    stats_message += "\n\n"
                    
                    await channel.send(stats_message)
                    print(f"✅ 成功发送汇总统计到 Discord")
                    
                    # 为每个股票symbol发送单个消息（只发送amount_threshold最大的记录）
                    if "symbol" in outliers_df.columns and "amount_threshold" in outliers_df.columns:
                        # 找到每个symbol的amount_threshold最大的记录
                        max_records = outliers_df.loc[outliers_df.groupby("symbol")["amount_threshold"].idxmax()]
                        
                        if not max_records.empty:
                            st = outliers_df["signal_type"].astype(str)
                            outliers_df_copy = outliers_df.copy()
                            
                            # 使用相同的精确分类逻辑
                            def classify_signal(row):
                                signal_type = str(row["signal_type"])
                                option_type = str(row["option_type"]).upper()
                                
                                # 不统计的信号类型
                                exclude_signals = [
                                    "空头平仓Put，回补，看跌信号减弱",
                                    "买Call平仓/做波动率交易", 
                                    "买Put平仓/做波动率交易"
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
                            classification = outliers_df_copy.apply(classify_signal, axis=1, result_type='expand')
                            outliers_df_copy["is_bullish"] = classification[0]
                            outliers_df_copy["is_bearish"] = classification[1] 
                            outliers_df_copy["is_call"] = classification[2]
                            outliers_df_copy["is_put"] = classification[3]
                            outliers_df_copy["should_count"] = classification[4]
                            
                            # 计算每个symbol的统计值（使用原始数据）
                            outliers_df_copy["amount"] = outliers_df_copy["amount_threshold"] * outliers_df_copy["lastPrice_new"] * 100
                            
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
                            
                            symbol_stats = outliers_df_copy.groupby("symbol").apply(calculate_amounts).reset_index()
                            
                            # 按total_count降序排列
                            symbol_stats = symbol_stats.sort_values(by=["total_count"], ascending=[False])
                            
                            # 按统计值顺序重新排列max_records
                            max_records = max_records.set_index("symbol").loc[symbol_stats["symbol"]].reset_index()
                        
                        success_count = 0
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
                    print(f"❌ Discord发送过程中出错: {e}")
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
async def send_oi_outliers(outliers_df, data_folder="data", time_range=None, stock_prices=None, high_amount_but_not_outlier_df=None):
    """发送OI异常数据到Discord"""
    sender = DiscordOutlierSender("OI异常", data_folder, time_range, stock_prices)
    await sender.send_outliers(outliers_df, "oi", high_amount_but_not_outlier_df)


async def send_volume_outliers(outliers_df, data_folder="data", time_range=None, stock_prices=None, high_amount_but_not_outlier_df=None):
    """发送Volume异常数据到Discord"""
    sender = DiscordOutlierSender("Volume异常", data_folder, time_range, stock_prices)
    await sender.send_outliers(outliers_df, "volume", high_amount_but_not_outlier_df)
