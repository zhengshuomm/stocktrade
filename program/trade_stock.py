#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票交易模拟系统
基于异常检测数据进行自动化股票交易模拟

=== 交易策略说明 ===

【买入时机 (BUY SIGNALS)】
1. 成交量异常检测 (Volume Outlier)：
   - 看涨信号：买 Call，看涨 (signal_type = "买 Call，看涨")
   - 看跌信号：买 Put，看跌 (signal_type = "买 Put，看跌")
   - 条件：异常金额 >= 200万 (MIN_AMOUNT_THRESHOLD = 2,000,000)
   - 金额分档：5M, 10M, 50M (500万, 1000万, 5000万)

2. 持仓量异常检测 (OI Outlier)：
   - 看涨信号：买 Call，看涨 (signal_type = "买 Call，看涨")
   - 看跌信号：买 Put，看跌 (signal_type = "买 Put，看跌")
   - 条件：异常金额 >= 500万 (THRESHOLD_5M = 5,000,000)
   - 金额分档：5M, 10M, 50M (500万, 1000万, 5000万)

3. 买入触发条件 (严格条件)：
   - 看涨信号数量 >= 2个 (bullish >= 2)
   - 看跌信号数量 = 0个 (bearish == 0)
   - 重要：只要有看跌信号就不会买入！

【卖出时机 (SELL SIGNALS)】
1. 时间止损：
   - 持有时间超过 HOLD_HOURS_LIMIT (24小时) 自动卖出
   - 防止长期套牢，确保资金流动性

2. 文件超时：
   - 如果数据文件超过 FILE_TIMEOUT_MINUTES (20分钟) 未更新
   - 认为市场数据可能过时，卖出相关持仓

3. 反向信号：
   - 出现相反方向的异常信号时卖出
   - 例如：持有看涨仓位时出现看跌信号

4. 强制平仓：
   - 程序重启或异常时，清理所有持仓
   - 确保交易状态的一致性

【风险控制】
- 每次买入金额：总资产的 BUY_RATIO (10%)
- 最大持仓时间：HOLD_HOURS_LIMIT (24小时)
- 数据新鲜度检查：FILE_TIMEOUT_MINUTES (20分钟)
- 初始资金：INITIAL_CASH (100,000)

【交易逻辑流程】
1. 扫描异常数据文件
2. 解析信号类型和股票信息
3. 检查现有持仓状态
4. 根据信号类型决定买入/卖出
5. 更新持仓记录和资金状态
6. 记录交易日志
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime, timedelta
import glob
import re
from typing import List, Dict, Tuple, Optional
import yfinance as yf
import argparse

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trade_stock.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库连接配置
DATABASE_URL = "postgresql://neondb_owner:npg_actGluWDr3d1@ep-raspy-river-af178kn5-pooler.c-2.us-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# 默认数据文件夹
DEFAULT_DATA_FOLDER = "data"

# 数据文件路径（将在main函数中动态设置）
OUTLIER_DIR = None
VOLUME_OUTLIER_DIR = None

# 交易配置
INITIAL_CASH = 100000.0
BUY_RATIO = 0.1  # 每次买入总资产的10%
HOLD_HOURS_LIMIT = 24  # 持有时间限制（小时）
FILE_TIMEOUT_MINUTES = 20  # 文件超时时间（分钟）


class StockTrader:
    """股票交易模拟器"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        
    def connect_database(self):
        """连接数据库"""
        try:
            self.conn = psycopg2.connect(DATABASE_URL)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False
    
    def close_database(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("数据库连接已关闭")
    
    def create_tables(self):
        """创建数据库表"""
        try:
            # 创建 User 表
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    cash DECIMAL(15,2) DEFAULT 100000.00,
                    stock DECIMAL(15,2) DEFAULT 0.00,
                    total_value DECIMAL(15,2) GENERATED ALWAYS AS (cash + stock) STORED,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建 Transaction_History 表
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS transaction_history (
                    transaction_id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    buy_price DECIMAL(10,2) NOT NULL,
                    sell_price DECIMAL(10,2) DEFAULT 0.00,
                    current_price DECIMAL(10,2) NOT NULL,
                    number_shares INTEGER NOT NULL,
                    amount DECIMAL(15,2) GENERATED ALWAYS AS (current_price * number_shares) STORED,
                    gain DECIMAL(15,2) GENERATED ALWAYS AS (
                        CASE 
                            WHEN is_hold = true THEN (current_price - buy_price) * number_shares
                            ELSE (sell_price - buy_price) * number_shares
                        END
                    ) STORED,
                    is_hold BOOLEAN DEFAULT true,
                    buy_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sell_date TIMESTAMP
                )
            """)
            
            # 创建唯一约束：每个symbol在is_hold=true时只能有一条记录
            self.cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_holding_symbol 
                ON transaction_history (symbol) WHERE is_hold = true
            """)
            
            # 初始化用户数据（如果不存在）
            self.cursor.execute("SELECT COUNT(*) FROM users")
            if self.cursor.fetchone()['count'] == 0:
                self.cursor.execute("INSERT INTO users (cash, stock) VALUES (%s, %s)", 
                                  (INITIAL_CASH, 0.0))
                logger.info("初始化用户数据")
            
            self.conn.commit()
            logger.info("数据库表创建/检查完成")
            return True
            
        except Exception as e:
            logger.error(f"创建表失败: {e}")
            self.conn.rollback()
            return False
    
    def get_latest_files(self) -> Tuple[Optional[str], Optional[str]]:
        """获取最新的异常检测文件"""
        try:
            # 获取outlier目录最新文件
            outlier_files = glob.glob(f"{OUTLIER_DIR}/*.csv")
            volume_outlier_files = glob.glob(f"{VOLUME_OUTLIER_DIR}/*.csv")
            
            latest_outlier = None
            latest_volume_outlier = None
            
            # 解析文件名中的时间戳
            def extract_timestamp(filename):
                match = re.search(r'(\d{8}-\d{4})', os.path.basename(filename))
                if match:
                    return datetime.strptime(match.group(1), '%Y%m%d-%H%M')
                return None
            
            # 找到最新的outlier文件
            if outlier_files:
                latest_outlier_file = max(outlier_files, key=extract_timestamp)
                file_time = extract_timestamp(latest_outlier_file)
                if file_time and (datetime.now() - file_time).total_seconds() <= FILE_TIMEOUT_MINUTES * 60:
                    latest_outlier = latest_outlier_file
                    logger.info(f"找到最新outlier文件: {latest_outlier_file}")
            
            # 找到最新的volume_outlier文件
            if volume_outlier_files:
                latest_volume_file = max(volume_outlier_files, key=extract_timestamp)
                file_time = extract_timestamp(latest_volume_file)
                if file_time and (datetime.now() - file_time).total_seconds() <= FILE_TIMEOUT_MINUTES * 60:
                    latest_volume_outlier = latest_volume_file
                    logger.info(f"找到最新volume_outlier文件: {latest_volume_file}")
            
            return latest_outlier, latest_volume_outlier
            
        except Exception as e:
            logger.error(f"获取最新文件失败: {e}")
            return None, None
    
    def load_outlier_data(self, file_path: str) -> pd.DataFrame:
        """加载异常检测数据"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"加载文件 {file_path}: {len(df)} 行数据")
            return df
        except Exception as e:
            logger.error(f"加载文件失败 {file_path}: {e}")
            return pd.DataFrame()
    
    def analyze_signals(self, outlier_df: pd.DataFrame, volume_outlier_df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
        """分析看涨/看跌信号"""
        signals = {}
        
        try:
            # 处理outlier数据（持仓量异常）
            if not outlier_df.empty:
                for _, row in outlier_df.iterrows():
                    symbol = row.get('symbol', '')
                    signal_type = row.get('signal_type', '')
                    
                    if symbol and signal_type:
                        if symbol not in signals:
                            signals[symbol] = {'bullish': 0, 'bearish': 0}
                        
                        if '看涨' in signal_type or 'bullish' in signal_type.lower():
                            signals[symbol]['bullish'] += 1
                        elif '看跌' in signal_type or 'bearish' in signal_type.lower():
                            signals[symbol]['bearish'] += 1
            
            # 处理volume_outlier数据（成交量异常）
            if not volume_outlier_df.empty:
                for _, row in volume_outlier_df.iterrows():
                    symbol = row.get('symbol', '')
                    signal_type = row.get('signal_type', '')
                    
                    if symbol and signal_type:
                        if symbol not in signals:
                            signals[symbol] = {'bullish': 0, 'bearish': 0}
                        
                        if '看涨' in signal_type or 'bullish' in signal_type.lower():
                            signals[symbol]['bullish'] += 1
                        elif '看跌' in signal_type or 'bearish' in signal_type.lower():
                            signals[symbol]['bearish'] += 1
            
            logger.info(f"分析信号完成: {len(signals)} 个股票")
            return signals
            
        except Exception as e:
            logger.error(f"分析信号失败: {e}")
            return {}
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """获取股票当前价格"""
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d")
            if not hist.empty:
                return float(hist['Close'].iloc[-1])
            return None
        except Exception as e:
            logger.error(f"获取 {symbol} 价格失败: {e}")
            return None
    
    def get_user_info(self) -> Dict:
        """获取用户信息"""
        try:
            self.cursor.execute("SELECT * FROM users ORDER BY id DESC LIMIT 1")
            result = self.cursor.fetchone()
            if result:
                return dict(result)
            return None
        except Exception as e:
            logger.error(f"获取用户信息失败: {e}")
            return None
    
    def get_holding_stocks(self) -> List[Dict]:
        """获取持有的股票"""
        try:
            self.cursor.execute("""
                SELECT * FROM transaction_history 
                WHERE is_hold = true 
                ORDER BY buy_date
            """)
            return [dict(row) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取持有股票失败: {e}")
            return []
    
    def buy_stock(self, symbol: str, buy_amount: float) -> bool:
        """买入股票"""
        try:
            current_price = self.get_current_price(symbol)
            if not current_price:
                logger.error(f"无法获取 {symbol} 的当前价格")
                return False
            
            number_shares = int(buy_amount / current_price)
            if number_shares <= 0:
                logger.error(f"买入金额 {buy_amount} 不足以购买 {symbol}")
                return False
            
            # 插入交易记录
            self.cursor.execute("""
                INSERT INTO transaction_history 
                (symbol, buy_price, current_price, number_shares, is_hold, buy_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (symbol, current_price, current_price, number_shares, True, datetime.now()))
            
            # 更新用户现金
            self.cursor.execute("""
                UPDATE users SET cash = cash - %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = (SELECT id FROM users ORDER BY id DESC LIMIT 1)
            """, (buy_amount,))
            
            self.conn.commit()
            logger.info(f"买入 {symbol}: {number_shares} 股, 价格 {current_price}, 金额 {buy_amount}")
            return True
            
        except Exception as e:
            logger.error(f"买入 {symbol} 失败: {e}")
            self.conn.rollback()
            return False
    
    def sell_stock(self, transaction_id: int, symbol: str) -> bool:
        """卖出股票"""
        try:
            # 获取当前价格
            current_price = self.get_current_price(symbol)
            if not current_price:
                logger.error(f"无法获取 {symbol} 的当前价格")
                return False
            
            # 更新交易记录
            self.cursor.execute("""
                UPDATE transaction_history 
                SET sell_price = %s, current_price = %s, is_hold = false, sell_date = %s
                WHERE transaction_id = %s
            """, (current_price, current_price, datetime.now(), transaction_id))
            
            # 获取卖出金额
            self.cursor.execute("""
                SELECT amount FROM transaction_history WHERE transaction_id = %s
            """, (transaction_id,))
            result = self.cursor.fetchone()
            if result:
                sell_amount = result['amount']
                
                # 更新用户现金
                self.cursor.execute("""
                    UPDATE users SET cash = cash + %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = (SELECT id FROM users ORDER BY id DESC LIMIT 1)
                """, (sell_amount,))
            
            self.conn.commit()
            logger.info(f"卖出 {symbol}: 价格 {current_price}, 金额 {sell_amount}")
            return True
            
        except Exception as e:
            logger.error(f"卖出 {symbol} 失败: {e}")
            self.conn.rollback()
            return False
    
    def update_holding_prices(self):
        """更新持有股票的价格"""
        try:
            holdings = self.get_holding_stocks()
            for holding in holdings:
                symbol = holding['symbol']
                current_price = self.get_current_price(symbol)
                if current_price:
                    self.cursor.execute("""
                        UPDATE transaction_history 
                        SET current_price = %s
                        WHERE transaction_id = %s AND is_hold = true
                    """, (current_price, holding['transaction_id']))
            
            self.conn.commit()
            logger.info(f"更新了 {len(holdings)} 只持有股票的价格")
            
        except Exception as e:
            logger.error(f"更新持有股票价格失败: {e}")
            self.conn.rollback()
    
    def update_user_stock_value(self):
        """更新用户股票总价值"""
        try:
            self.cursor.execute("""
                UPDATE users SET stock = (
                    SELECT COALESCE(SUM(amount), 0) 
                    FROM transaction_history 
                    WHERE is_hold = true
                ), updated_at = CURRENT_TIMESTAMP
                WHERE id = (SELECT id FROM users ORDER BY id DESC LIMIT 1)
            """)
            self.conn.commit()
            logger.info("更新用户股票总价值")
            
        except Exception as e:
            logger.error(f"更新用户股票总价值失败: {e}")
            self.conn.rollback()
    
    def process_trading_signals(self, signals: Dict[str, Dict[str, int]]):
        """处理交易信号"""
        try:
            user_info = self.get_user_info()
            if not user_info:
                logger.error("无法获取用户信息")
                return
            
            holdings = self.get_holding_stocks()
            holding_symbols = {h['symbol'] for h in holdings}
            
            # 处理持有股票的卖出逻辑
            for holding in holdings:
                symbol = holding['symbol']
                transaction_id = holding['transaction_id']
                buy_date = holding['buy_date']
                
                if symbol in signals:
                    signal = signals[symbol]
                    bullish = signal['bullish']
                    bearish = signal['bearish']
                    
                    # 卖出条件
                    should_sell = False
                    
                    if bullish > 0 and bearish > 0:
                        # 同时包含看涨和看跌
                        if bearish >= 3:
                            # 看跌数量 >= 3，直接卖出 (最高优先级)
                            should_sell = True
                            logger.info(f"{symbol}: 看跌数量 {bearish} >= 3, 决定卖出")
                        elif bullish > bearish:
                            # 看涨数量 > 看跌数量，继续持有
                            logger.info(f"{symbol}: 看涨 {bullish} > 看跌 {bearish}, 继续持有")
                        else:
                            # 看涨数量 <= 看跌数量 且 看跌数量 < 3，卖出
                            should_sell = True
                            logger.info(f"{symbol}: 看涨 {bullish} <= 看跌 {bearish} 且看跌 < 3, 决定卖出")
                    elif bearish > 0 and bullish == 0:
                        # 只包含看跌
                        should_sell = True
                        logger.info(f"{symbol}: 只有看跌信号, 决定卖出")
                    elif bullish > 0 and bearish == 0:
                        # 只包含看涨，继续持有
                        logger.info(f"{symbol}: 只有看涨信号, 继续持有")
                else:
                    # 不包含该股票，检查持有时间
                    hours_held = (datetime.now() - buy_date).total_seconds() / 3600
                    if hours_held > HOLD_HOURS_LIMIT:
                        should_sell = True
                        logger.info(f"{symbol}: 持有时间 {hours_held:.1f} 小时 > {HOLD_HOURS_LIMIT} 小时, 决定卖出")
                
                if should_sell:
                    self.sell_stock(transaction_id, symbol)
            
            # 处理买入逻辑
            total_value = float(user_info['total_value'])
            buy_amount = total_value * BUY_RATIO
            
            if float(user_info['cash']) >= buy_amount:
                for symbol, signal in signals.items():
                    if symbol not in holding_symbols:
                        bullish = signal['bullish']
                        bearish = signal['bearish']
                        
                        # 买入条件：>=2个看涨且无看跌
                        if bullish >= 2 and bearish == 0:
                            logger.info(f"{symbol}: 看涨 {bullish} 个, 看跌 {bearish} 个, 决定买入")
                            if self.buy_stock(symbol, buy_amount):
                                # 更新用户信息以获取最新现金余额
                                user_info = self.get_user_info()
                                if not user_info or float(user_info['cash']) < buy_amount:
                                    logger.info(f"现金不足，停止买入。当前现金: {float(user_info['cash']) if user_info else 0:.2f}, 需要: {buy_amount:.2f}")
                                    break
            else:
                logger.info(f"现金不足，无法买入。现金: {float(user_info['cash']):.2f}, 需要: {buy_amount:.2f}")
                
        except Exception as e:
            logger.error(f"处理交易信号失败: {e}")
    
    def run_trading_cycle(self):
        """运行一个完整的交易周期"""
        try:
            logger.info("开始交易周期")
            
            # 更新持有股票价格
            self.update_holding_prices()
            
            # 获取最新异常检测文件
            outlier_file, volume_outlier_file = self.get_latest_files()
            
            if not outlier_file and not volume_outlier_file:
                logger.warning("没有找到有效的异常检测文件")
                return
            
            # 加载数据
            outlier_df = pd.DataFrame()
            volume_outlier_df = pd.DataFrame()
            
            if outlier_file:
                outlier_df = self.load_outlier_data(outlier_file)
            if volume_outlier_file:
                volume_outlier_df = self.load_outlier_data(volume_outlier_file)
            
            if outlier_df.empty and volume_outlier_df.empty:
                logger.warning("异常检测数据为空")
                return
            
            # 分析信号
            signals = self.analyze_signals(outlier_df, volume_outlier_df)
            if not signals:
                logger.warning("没有分析到有效的交易信号")
                return
            
            # 处理交易信号
            self.process_trading_signals(signals)
            
            # 更新用户股票总价值
            self.update_user_stock_value()
            
            # 显示当前状态
            user_info = self.get_user_info()
            if user_info:
                logger.info(f"当前状态 - 现金: {float(user_info['cash']):.2f}, "
                          f"股票: {float(user_info['stock']):.2f}, "
                          f"总资产: {float(user_info['total_value']):.2f}")
            
            logger.info("交易周期完成")
            
        except Exception as e:
            logger.error(f"交易周期失败: {e}")


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='股票交易模拟系统')
    parser.add_argument('--folder', type=str, default=DEFAULT_DATA_FOLDER, 
                       help=f'数据文件夹路径 (默认: {DEFAULT_DATA_FOLDER})')
    
    args = parser.parse_args()
    
    # 设置全局数据路径
    global OUTLIER_DIR, VOLUME_OUTLIER_DIR
    OUTLIER_DIR = f"{args.folder}/outlier"
    VOLUME_OUTLIER_DIR = f"{args.folder}/volume_outlier"
    
    logger.info(f"使用数据文件夹: {args.folder}")
    logger.info(f"Outlier目录: {OUTLIER_DIR}")
    logger.info(f"Volume Outlier目录: {VOLUME_OUTLIER_DIR}")
    
    trader = StockTrader()
    
    try:
        # 连接数据库
        if not trader.connect_database():
            logger.error("无法连接数据库，程序退出")
            return
        
        # 创建表
        if not trader.create_tables():
            logger.error("无法创建数据库表，程序退出")
            return
        
        # 运行交易周期
        trader.run_trading_cycle()
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
    finally:
        trader.close_database()


if __name__ == "__main__":
    main()
