#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Discord发送outlier数据程序
从discord_bot.py读取配置，发送outlier CSV数据到Discord频道
"""

import discord
import asyncio
import os
import glob
import pandas as pd
from datetime import datetime
import argparse
import re

class DiscordOutlierSender:
    def __init__(self, outlier_dir="data/outlier"):
        self.outlier_dir = outlier_dir
        self.token = "MTQyMjQ0NDY2OTg5MTI1MjI0NQ.GXPW4w.N9gMYn_3hOs4TNVbj9JIt_47PPTV8Dc4uB_aJk"
        self.channel_id = 1422402343135088663  # 常规文字频道
        self.message_title = "OI异常"
        self.timeframe = None
        
    def find_latest_csv(self):
        """查找最新的CSV文件"""
        pattern = os.path.join(self.outlier_dir, "*.csv")
        csv_files = glob.glob(pattern)
        
        if not csv_files:
            print(f"在 {self.outlier_dir} 目录下未找到CSV文件")
            return None
        
        latest_file = max(csv_files, key=os.path.getmtime)
        print(f"找到最新CSV文件: {latest_file}")
        return latest_file

    def compute_timeframe_from_option_dir(self, option_dir: str = "data/option_data") -> str:
        """从 option_data 中最新两个 all-*.csv 文件计算时间范围字符串"""
        try:
            files = glob.glob(os.path.join(option_dir, "all-*.csv"))
            if len(files) < 2:
                return None
            def parse_ts(path: str):
                name = os.path.basename(path)
                m = re.match(r"all-(\d{8}-\d{4})\.csv$", name)
                if m:
                    return m.group(1)
                # fallback mtime
                return datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y%m%d-%H%M")
            def fmt_human(ts_str: str) -> str:
                # ts_str like 20250929-1344 -> 09月29日13时44分
                try:
                    dt = datetime.strptime(ts_str, "%Y%m%d-%H%M")
                    return dt.strftime("%m月%d日%H时%M分")
                except Exception:
                    return ts_str
            files_sorted = sorted(files, key=lambda p: os.path.getmtime(p), reverse=True)
            ts_latest = parse_ts(files_sorted[0])
            ts_prev = parse_ts(files_sorted[1])
            return f"{fmt_human(ts_latest)}-{fmt_human(ts_prev)}"
        except Exception:
            return None
    
    def format_outlier_message(self, row):
        """格式化异常数据消息"""
        symbol = row.get('symbol', 'N/A')
        contract_symbol = row.get('contractSymbol', 'N/A')
        strike = row.get('strike', 'N/A')
        expiry_date = row.get('expiry_date', 'N/A')
        signal_type = row.get('signal_type', 'N/A')
        amount_threshold = row.get('amount_threshold', 0)
        stock_change_pct = row.get('stock_price_change_pct', 0)
        option_change_pct = row.get('option_price_change_pct', 0)
        oi_change_abs = row.get('oi_change_abs', 0)
        volume_change_abs = row.get('volume_change_abs', 0)
        volume_new_val = row.get('volume_new', row.get('volume', 0))
        last_price_new = row.get('lastPrice_new', row.get('lastPrice', 0))
        last_price_old = row.get('lastPrice_old', 0)
        amount_tier = row.get('amount_tier', 'N/A')
        yahoo_url = f"https://finance.yahoo.com/quote/{contract_symbol}"
        timeframe = getattr(self, 'timeframe', None)
        
        # 根据金额档位设置前缀和颜色
        if amount_tier == ">50M":
            prefix = "!!!!! "
            color = "🔴"  # 红色
        elif amount_tier == "10M-50M":
            prefix = "! "
            color = "🟠"  # 橘红色
        else:
            prefix = ""
            color = "⚪"  # 白色
        
        # 创建Discord嵌入消息
        embed = discord.Embed(
            title=f"{color} {prefix}{self.message_title}",
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
        
        embed.add_field(
            name="📈 变化数据",
            value=f"**股票变化**: {stock_change_pct:.2f}%\n**期权变化**: {option_change_pct:.2f}%\n**OI变化**: {oi_change_abs:,.0f}\n**Volume变化**: {volume_change_abs:,.0f}\n**Volume(new)**: {volume_new_val:,.0f}",
            inline=True
        )

        # 数值明细
        embed.add_field(
            name="🔢 数值",
            value=f"**lastPrice(new)**: ${last_price_new}\n**lastPrice(old)**: ${last_price_old}",
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

        # 时间范围
        if timeframe:
            embed.add_field(
                name="🕒 时间范围",
                value=timeframe,
                inline=False
            )
        
        # 设置footer
        embed.set_footer(text=f"检测时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return embed
    
    def format_simple_message(self, row):
        """格式化简单文本消息"""
        symbol = row.get('symbol', 'N/A')
        contract_symbol = row.get('contractSymbol', 'N/A')
        strike = row.get('strike', 'N/A')
        expiry_date = row.get('expiry_date', 'N/A')
        signal_type = row.get('signal_type', 'N/A')
        amount_threshold = row.get('amount_threshold', 0)
        stock_change_pct = row.get('stock_price_change_pct', 0)
        option_change_pct = row.get('option_price_change_pct', 0)
        oi_change_abs = row.get('oi_change_abs', 0)
        volume_change_abs = row.get('volume_change_abs', 0)
        volume_new_val = row.get('volume_new', row.get('volume', 0))
        last_price_new = row.get('lastPrice_new', row.get('lastPrice', 0))
        last_price_old = row.get('lastPrice_old', 0)
        amount_tier = row.get('amount_tier', 'N/A')
        yahoo_url = f"https://finance.yahoo.com/quote/{contract_symbol}"
        timeframe = getattr(self, 'timeframe', None)
        
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
        
        # 处理信号类型颜色
        colored_signal_type = self._colorize_signal_type(signal_type)
        
        message = f"""{color_emoji} {prefix}**{self.message_title}**

📊 **合约信息**
• Symbol: `{symbol}`
• Strike: ${strike}
• Expiry: {expiry_date}

📈 **变化数据**
• 股票变化: {stock_change_pct:.2f}%
• 期权变化: {option_change_pct:.2f}%
• OI变化: {oi_change_abs:,.0f}
• Volume变化: {volume_change_abs:,.0f}
• Volume(new): {volume_new_val:,.0f}

🔢 **数值**
• lastPrice(new): ${last_price_new}
• lastPrice(old): ${last_price_old}

🚨 **异常信号**
• 信号类型: {colored_signal_type}
• 金额门槛: ${amount_threshold:,.0f}
• 金额档位: {amount_tier}

🔗 **Yahoo**
{yahoo_url}

🕒 **时间范围**
{timeframe if timeframe else 'N/A'}

⏰ 检测时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        
        return message
    
    def _colorize_signal_type(self, signal_type):
        """为信号类型添加颜色"""
        if "看涨" in signal_type:
            # Discord中红色文本
            return f"🔴 {signal_type}"
        elif "看跌" in signal_type:
            # Discord中绿色文本
            return f"🟢 {signal_type}"
        else:
            # 其他情况保持原样
            return signal_type

class DiscordClient(discord.Client):
    def __init__(self, outlier_dir="outlier", delay=2.0, use_embed=True):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.sender = DiscordOutlierSender(outlier_dir)
        self.delay = delay
        self.use_embed = use_embed
        
    async def on_ready(self):
        print(f'Discord Bot登录成功: {self.user}')
        print(f'Bot ID: {self.user.id}')
        
        # 获取频道
        channel = self.get_channel(self.sender.channel_id)
        print(f'目标频道: {channel.name if channel else "未找到"} (ID: {self.sender.channel_id})')
        
        if not channel:
            print("❌ 频道未找到!")
            await self.close()
            return
        
        # 读取CSV文件
        csv_file = self.sender.find_latest_csv()
        if not csv_file:
            print("❌ 未找到CSV文件!")
            await self.close()
            return
        
        # 根据文件名设置消息标题
        base_name = os.path.basename(csv_file)
        if base_name.startswith("volume_outlier"):
            self.sender.message_title = "volume异常"
        else:
            self.sender.message_title = "OI异常"

        # 计算时间范围（来自 option_data 最新两个 all-*.csv）
        self.sender.timeframe = self.sender.compute_timeframe_from_option_dir("data/option_data")

        try:
            df = pd.read_csv(csv_file)
            print(f"读取到 {len(df)} 条异常数据")
            
            if df.empty:
                print("CSV文件为空，无需发送")
                await self.close()
                return
            
            # 逐行发送数据（pandas读取CSV时标题行不会作为数据行）
            success_count = 0
            for index, row in df.iterrows():
                    
                print(f"发送第 {index}/{len(df)} 条数据...")
                
                try:
                    if self.use_embed:
                        # 使用嵌入消息
                        embed = self.sender.format_outlier_message(row)
                        await channel.send(embed=embed)
                    else:
                        # 使用简单文本消息
                        message = self.sender.format_simple_message(row)
                        await channel.send(message)
                    
                    success_count += 1
                    print(f"✅ 第 {index} 条数据发送成功")
                    
                    # 消息间延时
                    if index < len(df) - 1:
                        await asyncio.sleep(self.delay)
                        
                except discord.Forbidden:
                    print(f"❌ 第 {index} 条数据发送失败: 没有权限")
                except discord.HTTPException as e:
                    print(f"❌ 第 {index} 条数据发送失败: HTTP错误 - {e}")
                except Exception as e:
                    print(f"❌ 第 {index} 条数据发送失败: {e}")
            
            # 发送一个“空消息”（使用零宽空格+5个换行）用于分隔
            try:
                await channel.send("\u200B----------------------------------------------------------------------\n")
            except Exception as e:
                print(f"发送空消息失败: {e}")

            print(f"\n🎉 发送完成! 成功发送 {success_count}/{len(df)-1} 条数据")
            
        except Exception as e:
            print(f"❌ 处理CSV文件时出错: {e}")
        finally:
            await self.close()

def parse_arguments():
    parser = argparse.ArgumentParser(description='Discord发送outlier CSV数据程序')
    
    parser.add_argument('--outlier-dir', '-d', type=str, default='data/outlier',
                       help='outlier目录路径 (默认: data/outlier)')
    
    parser.add_argument('--delay', '-t', type=float, default=2.0,
                       help='每条消息之间的延时秒数 (默认: 2.0)')
    
    parser.add_argument('--simple', action='store_true',
                       help='使用简单文本格式而不是嵌入消息')
    
    return parser.parse_args()

async def main():
    args = parse_arguments()
    
    print("=" * 60)
    print("Discord发送outlier CSV数据程序")
    print("=" * 60)
    print(f"outlier目录: {args.outlier_dir}")
    print(f"消息延时: {args.delay}秒")
    print(f"消息格式: {'简单文本' if args.simple else '嵌入消息'}")
    print()
    
    client = DiscordClient(
        outlier_dir=args.outlier_dir,
        delay=args.delay,
        use_embed=not args.simple
    )
    
    try:
        await client.start(client.sender.token)
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序执行出错: {e}")

if __name__ == "__main__":
    asyncio.run(main())
