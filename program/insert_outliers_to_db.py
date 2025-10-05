#!/usr/bin/env python3
"""
数据库插入程序 - 将异常数据插入到PostgreSQL数据库

程序逻辑详解：
================

1. 程序入口和参数解析
   - 支持两个数据文件夹：data 和 priority_data
   - 可配置数据清理天数（默认7天）
   - 可选择跳过数据清理步骤
   - 支持详细日志模式

2. 核心处理流程
   ┌─────────────────────────────────────────────────────────────┐
   │ 1. 连接PostgreSQL数据库 (Neon云数据库)                      │
   ├─────────────────────────────────────────────────────────────┤
   │ 2. 处理volume_outlier数据                                   │
   │    ├─ 查找最新volume_outlier_*.csv文件                      │
   │    ├─ 检查processed_files表避免重复处理                     │
   │    ├─ 读取当前文件数据                                      │
   │    ├─ 与上一个文件比较数据相似性（避免重复插入相同数据）      │
   │    ├─ 获取前一天最后一个时间戳的股票价格数据（用于last_day_close_price）│
   │    ├─ 准备数据（格式化浮点数精度、处理信号类型等）            │
   │    ├─ 批量插入到volume_outlier表（使用ON CONFLICT处理重复）  │
   │    └─ 记录处理结果到processed_files表                      │
   ├─────────────────────────────────────────────────────────────┤
   │ 3. 处理oi_outlier数据                                       │
   │    ├─ 查找最新outlier/*.csv文件                            │
   │    ├─ 检查processed_files表避免重复处理                     │
   │    ├─ 读取当前文件数据                                      │
   │    ├─ 与上一个文件比较数据相似性                            │
   │    ├─ 准备数据（格式化浮点数精度、处理信号类型等）            │
   │    ├─ 批量插入到oi_outlier表（使用ON CONFLICT处理重复）      │
   │    └─ 记录处理结果到processed_files表                      │
   ├─────────────────────────────────────────────────────────────┤
   │ 4. 数据清理（可选）                                         │
   │    ├─ 清理超过指定天数的volume_outlier数据                  │
   │    ├─ 清理超过指定天数的oi_outlier数据                      │
   │    └─ 清理超过指定天数的processed_files记录                 │
   └─────────────────────────────────────────────────────────────┘

3. 关键特性
   - 重复处理防护：通过processed_files表记录已处理文件
   - 数据去重：比较相邻文件数据相似性，跳过相同数据
   - 信号类型管理：自动创建和管理signal_types表
   - 时区处理：使用PST时区统一时间格式
   - 浮点数精度：统一格式化数值精度避免精度问题
   - 前一天收盘价：自动获取前一天最后一个时间戳的股票收盘价到last_day_close_price列
   - 错误处理：完整的异常处理和回滚机制
   - 批量操作：使用execute_values提高插入效率
   - 冲突处理：使用ON CONFLICT DO UPDATE处理重复数据

4. 数据库表结构
   - volume_outlier: 存储成交量异常数据
   - oi_outlier: 存储持仓量异常数据  
   - signal_types: 存储信号类型定义
   - processed_files: 记录文件处理状态

5. 文件组织结构
   data/ 或 priority_data/
   ├── volume_outlier/     # 成交量异常CSV文件
   ├── outlier/            # 持仓量异常CSV文件
   └── 其他数据文件夹...

功能：
1. 读取 volume_outlier 和 oi_outlier CSV文件
2. 检查 processed_files 表避免重复处理
3. 批量插入数据到对应数据库表
4. 记录处理结果到 processed_files 表

使用方法：
python3 program/insert_outliers_to_db.py --folder data
python3 program/insert_outliers_to_db.py --folder priority_data
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import argparse
import logging
from datetime import datetime
import pytz
from pathlib import Path
import hashlib

# 数据库连接信息
DB_CONFIG = {
    'host': 'ep-raspy-river-af178kn5-pooler.c-2.us-west-2.aws.neon.tech',
    'database': 'neondb',
    'user': 'neondb_owner',
    'password': 'npg_actGluWDr3d1',
    'port': 5432,
    'sslmode': 'require'
}

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('insert_outliers.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatabaseInserter:
    def __init__(self, folder_name):
        self.folder_name = folder_name
        self.conn = None
        self.cursor = None
        self.signal_type_cache = {}  # 缓存信号类型ID
        self.pst_tz = pytz.timezone('US/Pacific')  # PST时区
        
    def connect_db(self):
        """连接数据库"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            logger.info("✅ 数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"❌ 数据库连接失败: {e}")
            return False
    
    def close_db(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("🔌 数据库连接已关闭")
    
    def get_signal_type_id(self, signal_type_name):
        """获取信号类型ID，如果不存在则创建"""
        if not signal_type_name or signal_type_name == '':
            return None
            
        # 检查缓存
        if signal_type_name in self.signal_type_cache:
            return self.signal_type_cache[signal_type_name]
        
        try:
            # 查询现有信号类型
            query = "SELECT id FROM signal_types WHERE signal_name = %s"
            self.cursor.execute(query, (signal_type_name,))
            result = self.cursor.fetchone()
            
            if result:
                signal_id = result[0]
                self.signal_type_cache[signal_type_name] = signal_id
                return signal_id
            else:
                # 创建新的信号类型
                insert_query = """
                INSERT INTO signal_types (signal_name, description) 
                VALUES (%s, %s) 
                RETURNING id
                """
                description = f"自动创建的信号类型: {signal_type_name}"
                self.cursor.execute(insert_query, (signal_type_name, description))
                signal_id = self.cursor.fetchone()[0]
                self.signal_type_cache[signal_type_name] = signal_id
                logger.info(f"✅ 创建新信号类型: {signal_type_name} (ID: {signal_id})")
                return signal_id
                
        except Exception as e:
            logger.error(f"❌ 获取信号类型ID失败: {signal_type_name} - {e}")
            return None
    
    def check_file_processed(self, csv_filename, file_type):
        """检查文件是否已处理过"""
        try:
            query = """
            SELECT id, status FROM processed_files 
            WHERE folder_name = %s AND csv_filename = %s AND file_type = %s
            """
            self.cursor.execute(query, (self.folder_name, csv_filename, file_type))
            result = self.cursor.fetchone()
            
            if result:
                logger.info(f"📁 文件已处理过: {csv_filename} (状态: {result[1]})")
                return True, result[1]
            return False, None
        except Exception as e:
            logger.error(f"❌ 检查文件状态失败: {e}")
            return False, None
    
    def get_latest_csv_file(self, subfolder, file_pattern):
        """获取指定子文件夹中最新的CSV文件"""
        folder_path = Path(self.folder_name) / subfolder
        if not folder_path.exists():
            logger.warning(f"⚠️ 文件夹不存在: {folder_path}")
            return None, None, None
        
        csv_files = list(folder_path.glob(file_pattern))
        if not csv_files:
            logger.warning(f"⚠️ 未找到匹配的CSV文件: {folder_path}/{file_pattern}")
            return None, None, None
        
        # 按修改时间排序，获取最新文件和上一个文件
        sorted_files = sorted(csv_files, key=os.path.getmtime)
        latest_file = sorted_files[-1]
        previous_file = sorted_files[-2] if len(sorted_files) > 1 else None
        
        logger.info(f"📄 找到最新文件: {latest_file}")
        if previous_file:
            logger.info(f"📄 找到上一个文件: {previous_file}")
        
        return latest_file, latest_file.name, previous_file
    
    def read_csv_data(self, file_path):
        """读取CSV文件数据"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"📊 读取CSV文件: {file_path.name}, 行数: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"❌ 读取CSV文件失败: {file_path}: {e}")
            return None
    
    def compare_data_similarity(self, current_df, previous_df, file_type):
        """比较当前文件和上一个文件的数据相似性"""
        if current_df is None or previous_df is None:
            logger.info("📊 无法比较数据：缺少文件数据")
            return False
        
        try:
            # 根据文件类型选择比较的列
            if file_type == 'volume_outlier':
                compare_columns = ['contractSymbol', 'lastPrice_new', '股票价格(new)']
            else:  # oi_outlier
                compare_columns = ['contractSymbol', 'lastPrice_new', '股票价格(new)']
            
            # 检查必要的列是否存在
            missing_cols = [col for col in compare_columns if col not in current_df.columns or col not in previous_df.columns]
            if missing_cols:
                logger.warning(f"⚠️ 缺少比较列: {missing_cols}")
                return False
            
            # 创建比较用的数据框
            current_compare = current_df[compare_columns].copy()
            previous_compare = previous_df[compare_columns].copy()
            
            # 按contractSymbol排序
            current_compare = current_compare.sort_values('contractSymbol').reset_index(drop=True)
            previous_compare = previous_compare.sort_values('contractSymbol').reset_index(drop=True)
            
            # 检查行数是否相同
            if len(current_compare) != len(previous_compare):
                logger.info(f"📊 数据行数不同: 当前={len(current_compare)}, 上一个={len(previous_compare)}")
                return False
            
            # 检查contractSymbol是否相同
            if not current_compare['contractSymbol'].equals(previous_compare['contractSymbol']):
                logger.info("📊 contractSymbol列表不同")
                return False
            
            # 比较数值列（允许小的浮点数误差）
            tolerance = 1e-6
            for col in ['lastPrice_new', '股票价格(new)']:
                current_col = pd.to_numeric(current_compare[col], errors='coerce')
                previous_col = pd.to_numeric(previous_compare[col], errors='coerce')
                
                # 检查是否有NaN值
                if current_col.isna().any() or previous_col.isna().any():
                    logger.info(f"📊 {col}列包含NaN值，无法比较")
                    return False
                
                # 比较数值差异
                diff = abs(current_col - previous_col)
                max_diff = diff.max()
                
                if max_diff > tolerance:
                    logger.info(f"📊 {col}列最大差异: {max_diff:.6f} > {tolerance}")
                    return False
            
            logger.info("✅ 数据完全相同，跳过插入")
            return True
            
        except Exception as e:
            logger.error(f"❌ 比较数据失败: {e}")
            return False
    
    def format_float_precision(self, value, precision=2):
        """格式化浮点数精度"""
        if pd.isna(value) or value is None:
            return None
        try:
            # 保留2位有效数字
            if abs(value) < 1e-10:
                return 0.0
            # 计算有效数字位数
            if abs(value) >= 1:
                return round(value, precision)
            else:
                # 对于小于1的数，计算需要的小数位数
                import math
                if value == 0:
                    return 0.0
                decimal_places = precision - int(math.floor(math.log10(abs(value)))) - 1
                return round(value, max(0, decimal_places))
        except:
            return None
    
    def get_previous_day_stock_prices(self, current_file_path):
        """获取前一天的股票价格数据"""
        try:
            # 获取当前文件的完整时间戳
            current_filename = current_file_path.name
            # 从文件名中提取时间戳，格式如：volume_outlier_20251003-1537.csv
            if 'volume_outlier_' in current_filename:
                # 提取时间戳部分：volume_outlier_20251003-1537.csv -> 20251003-1537
                timestamp_part = current_filename.split('volume_outlier_')[1].replace('.csv', '')
                # 解析时间戳：20251003-1537 -> 2025-10-03 15:37
                current_datetime = datetime.strptime(timestamp_part, '%Y%m%d-%H%M')
                
                # 计算前一天的时间戳
                from datetime import timedelta
                previous_datetime = current_datetime - timedelta(days=1)
                previous_date_str = previous_datetime.strftime('%Y%m%d')
                
                # 查找前一天的股票价格文件
                stock_price_folder = Path(self.folder_name) / 'stock_price'
                if not stock_price_folder.exists():
                    logger.warning(f"⚠️ 股票价格文件夹不存在: {stock_price_folder}")
                    return None
                
                # 查找前一天的股票价格文件（格式：all-YYYYMMDD-HHMM.csv）
                previous_files = list(stock_price_folder.glob(f'all-{previous_date_str}-*.csv'))
                if not previous_files:
                    logger.warning(f"⚠️ 未找到前一天的股票价格文件: {previous_date_str}")
                    return None
                
                # 找到前一天最后一个时间戳的文件
                # 按文件名中的时间戳排序，获取最后一个
                previous_files_with_time = []
                for file in previous_files:
                    try:
                        # 从文件名提取时间戳：all-20251002-1459.csv -> 20251002-1459
                        file_timestamp = file.name.split('all-')[1].replace('.csv', '')
                        file_datetime = datetime.strptime(file_timestamp, '%Y%m%d-%H%M')
                        previous_files_with_time.append((file, file_datetime))
                    except Exception as e:
                        logger.warning(f"⚠️ 无法解析文件名时间戳: {file.name} - {e}")
                        continue
                
                if not previous_files_with_time:
                    logger.warning(f"⚠️ 前一天没有有效的时间戳文件: {previous_date_str}")
                    return None
                
                # 按时间戳排序，获取最后一个（最新的）
                previous_files_with_time.sort(key=lambda x: x[1])
                previous_file = previous_files_with_time[-1][0]
                previous_file_time = previous_files_with_time[-1][1]
                
                logger.info(f"📄 找到前一天最后一个时间戳的股票价格文件: {previous_file} (时间: {previous_file_time})")
                
                # 读取前一天的股票价格数据
                previous_df = pd.read_csv(previous_file)
                logger.info(f"📊 读取前一天股票价格数据: {len(previous_df)} 条记录")
                
                return previous_df
            else:
                logger.warning(f"⚠️ 无法从文件名提取时间戳: {current_filename}")
                return None
                
        except Exception as e:
            logger.error(f"❌ 获取前一天股票价格失败: {e}")
            return None
    
    def prepare_volume_data(self, df, previous_stock_prices=None):
        """准备volume_outlier数据"""
        if df is None or df.empty:
            return []
        
        data_list = []
        # 使用PST时间，格式化为数据库可接受的格式
        current_time = datetime.now(self.pst_tz).strftime('%Y-%m-%d %H:%M:%S')
        
        # 创建前一天股票价格的映射字典
        previous_close_prices = {}
        if previous_stock_prices is not None and not previous_stock_prices.empty:
            for _, stock_row in previous_stock_prices.iterrows():
                symbol = str(stock_row.get('symbol', ''))
                close_price = self.format_float_precision(stock_row.get('Close'))
                if symbol and close_price is not None:
                    previous_close_prices[symbol] = close_price
            logger.info(f"📊 创建前一天收盘价映射: {len(previous_close_prices)} 个股票")
        
        for _, row in df.iterrows():
            try:
                signal_type_name = str(row.get('signal_type', ''))
                signal_type_id = self.get_signal_type_id(signal_type_name)
                
                # 获取前一天收盘价
                symbol = str(row.get('symbol', ''))
                last_day_close_price = previous_close_prices.get(symbol) if symbol in previous_close_prices else None
                
                data = {
                    'contractSymbol': str(row.get('contractSymbol', '')),
                    'strike': self.format_float_precision(row.get('strike')),
                    'signal_type_id': signal_type_id,
                    'folder_name': self.folder_name,
                    'option_type': str(row.get('option_type', '')),
                    'volume_old': self.format_float_precision(row.get('volume_old')),
                    'volume_new': self.format_float_precision(row.get('volume_new')),
                    'amount_threshold': self.format_float_precision(row.get('amount_threshold')),
                    'amount_to_market_cap': self.format_float_precision(row.get('amount_to_market_cap')),
                    'openInterest_new': self.format_float_precision(row.get('openInterest_new')),
                    'expiry_date': str(row.get('expiry_date', '')),
                    'lastPrice_new': self.format_float_precision(row.get('lastPrice_new')),
                    'lastPrice_old': self.format_float_precision(row.get('lastPrice_old')),
                    'symbol': symbol,
                    'stock_price_new': self.format_float_precision(row.get('股票价格(new)')),
                    'stock_price_old': self.format_float_precision(row.get('股票价格(old)')),
                    'stock_price_new_open': self.format_float_precision(row.get('股票价格(new open)')),
                    'stock_price_new_high': self.format_float_precision(row.get('股票价格(new high)')),
                    'stock_price_new_low': self.format_float_precision(row.get('股票价格(new low)')),
                    'last_day_close_price': last_day_close_price,
                    'create_time': current_time
                }
                data_list.append(data)
            except Exception as e:
                logger.warning(f"⚠️ 处理volume数据行失败: {e}")
                continue
        
        return data_list
    
    def prepare_oi_data(self, df):
        """准备oi_outlier数据"""
        if df is None or df.empty:
            return []
        
        data_list = []
        # 使用PST时间，格式化为数据库可接受的格式
        current_time = datetime.now(self.pst_tz).strftime('%Y-%m-%d %H:%M:%S')
        
        for _, row in df.iterrows():
            try:
                signal_type_name = str(row.get('signal_type', ''))
                signal_type_id = self.get_signal_type_id(signal_type_name)
                
                data = {
                    'contractSymbol': str(row.get('contractSymbol', '')),
                    'strike': self.format_float_precision(row.get('strike')),
                    'oi_change': self.format_float_precision(row.get('oi_change')),
                    'signal_type_id': signal_type_id,
                    'folder_name': self.folder_name,
                    'option_type': str(row.get('option_type', '')),
                    'openInterest_new': self.format_float_precision(row.get('openInterest_new')),
                    'openInterest_old': self.format_float_precision(row.get('openInterest_old')),
                    'amount_threshold': self.format_float_precision(row.get('amount_threshold')),
                    'amount_to_market_cap': self.format_float_precision(row.get('amount_to_market_cap')),
                    'expiry_date': str(row.get('expiry_date', '')),
                    'lastPrice_new': self.format_float_precision(row.get('lastPrice_new')),
                    'lastPrice_old': self.format_float_precision(row.get('lastPrice_old')),
                    'volume': self.format_float_precision(row.get('volume')),
                    'symbol': str(row.get('symbol', '')),
                    'stock_price_new': self.format_float_precision(row.get('股票价格(new)')),
                    'stock_price_old': self.format_float_precision(row.get('股票价格(old)')),
                    'stock_price_new_open': self.format_float_precision(row.get('股票价格(new open)')),
                    'stock_price_new_high': self.format_float_precision(row.get('股票价格(new high)')),
                    'stock_price_new_low': self.format_float_precision(row.get('股票价格(new low)')),
                    'create_time': current_time
                }
                data_list.append(data)
            except Exception as e:
                logger.warning(f"⚠️ 处理OI数据行失败: {e}")
                continue
        
        return data_list
    
    def insert_volume_data(self, data_list):
        """插入volume_outlier数据"""
        if not data_list:
            logger.info("📊 没有volume数据需要插入")
            return 0
        
        try:
            # 准备插入数据
            columns = [
                'contractSymbol', 'strike', 'signal_type_id', 'folder_name', 'option_type',
                'volume_old', 'volume_new', 'amount_threshold', 'amount_to_market_cap',
                'openInterest_new', 'expiry_date', 'lastPrice_new', 'lastPrice_old',
                'symbol', 'stock_price_new', 'stock_price_old', 'stock_price_new_open',
                'stock_price_new_high', 'stock_price_new_low', 'last_day_close_price', 'create_time'
            ]
            
            values = []
            for data in data_list:
                # 将create_time转换为PST时区的timestamp
                create_time_pst = f"{data['create_time']} PST"
                data_copy = data.copy()
                data_copy['create_time'] = create_time_pst
                values.append(tuple(data_copy.get(col) for col in columns))
            
            # 批量插入，使用PST时区
            insert_query = f"""
            INSERT INTO volume_outlier ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (contractSymbol, folder_name, create_time) 
            DO UPDATE SET
                strike = EXCLUDED.strike,
                signal_type_id = EXCLUDED.signal_type_id,
                option_type = EXCLUDED.option_type,
                volume_old = EXCLUDED.volume_old,
                volume_new = EXCLUDED.volume_new,
                amount_threshold = EXCLUDED.amount_threshold,
                amount_to_market_cap = EXCLUDED.amount_to_market_cap,
                openInterest_new = EXCLUDED.openInterest_new,
                expiry_date = EXCLUDED.expiry_date,
                lastPrice_new = EXCLUDED.lastPrice_new,
                lastPrice_old = EXCLUDED.lastPrice_old,
                symbol = EXCLUDED.symbol,
                stock_price_new = EXCLUDED.stock_price_new,
                stock_price_old = EXCLUDED.stock_price_old,
                stock_price_new_open = EXCLUDED.stock_price_new_open,
                stock_price_new_high = EXCLUDED.stock_price_new_high,
                stock_price_new_low = EXCLUDED.stock_price_new_low,
                last_day_close_price = EXCLUDED.last_day_close_price
            """
            
            execute_values(self.cursor, insert_query, values)
            self.conn.commit()
            
            logger.info(f"✅ 成功插入 {len(data_list)} 条volume数据")
            return len(data_list)
            
        except Exception as e:
            logger.error(f"❌ 插入volume数据失败: {e}")
            self.conn.rollback()
            return 0
    
    def insert_oi_data(self, data_list):
        """插入oi_outlier数据"""
        if not data_list:
            logger.info("📊 没有OI数据需要插入")
            return 0
        
        try:
            # 准备插入数据
            columns = [
                'contractSymbol', 'strike', 'oi_change', 'signal_type_id', 'folder_name',
                'option_type', 'openInterest_new', 'openInterest_old', 'amount_threshold',
                'amount_to_market_cap', 'expiry_date', 'lastPrice_new', 'lastPrice_old',
                'volume', 'symbol', 'stock_price_new', 'stock_price_old', 'stock_price_new_open',
                'stock_price_new_high', 'stock_price_new_low', 'create_time'
            ]
            
            values = []
            for data in data_list:
                # 将create_time转换为PST时区的timestamp
                create_time_pst = f"{data['create_time']} PST"
                data_copy = data.copy()
                data_copy['create_time'] = create_time_pst
                values.append(tuple(data_copy.get(col) for col in columns))
            
            # 批量插入，使用PST时区
            insert_query = f"""
            INSERT INTO oi_outlier ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (contractSymbol, folder_name, create_time) 
            DO UPDATE SET
                strike = EXCLUDED.strike,
                oi_change = EXCLUDED.oi_change,
                signal_type_id = EXCLUDED.signal_type_id,
                option_type = EXCLUDED.option_type,
                openInterest_new = EXCLUDED.openInterest_new,
                openInterest_old = EXCLUDED.openInterest_old,
                amount_threshold = EXCLUDED.amount_threshold,
                amount_to_market_cap = EXCLUDED.amount_to_market_cap,
                expiry_date = EXCLUDED.expiry_date,
                lastPrice_new = EXCLUDED.lastPrice_new,
                lastPrice_old = EXCLUDED.lastPrice_old,
                volume = EXCLUDED.volume,
                symbol = EXCLUDED.symbol,
                stock_price_new = EXCLUDED.stock_price_new,
                stock_price_old = EXCLUDED.stock_price_old,
                stock_price_new_open = EXCLUDED.stock_price_new_open,
                stock_price_new_high = EXCLUDED.stock_price_new_high,
                stock_price_new_low = EXCLUDED.stock_price_new_low
            """
            
            execute_values(self.cursor, insert_query, values)
            self.conn.commit()
            
            logger.info(f"✅ 成功插入 {len(data_list)} 条OI数据")
            return len(data_list)
            
        except Exception as e:
            logger.error(f"❌ 插入OI数据失败: {e}")
            self.conn.rollback()
            return 0
    
    def record_processed_file(self, csv_filename, file_type, file_size, row_count, status='success'):
        """记录已处理的文件"""
        try:
            insert_query = """
            INSERT INTO processed_files (folder_name, csv_filename, file_type, file_size, row_count, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (folder_name, csv_filename) 
            DO UPDATE SET
                file_type = EXCLUDED.file_type,
                file_size = EXCLUDED.file_size,
                row_count = EXCLUDED.row_count,
                status = EXCLUDED.status,
                processed_time = CURRENT_TIMESTAMP
            """
            
            self.cursor.execute(insert_query, (self.folder_name, csv_filename, file_type, file_size, row_count, status))
            self.conn.commit()
            
            logger.info(f"📝 记录处理文件: {csv_filename} (状态: {status})")
            
        except Exception as e:
            logger.error(f"❌ 记录处理文件失败: {e}")
            self.conn.rollback()
    
    def cleanup_old_data(self, days=7):
        """清理超过指定天数的旧数据"""
        try:
            logger.info(f"🧹 开始清理超过{days}天的旧数据...")
            
            # 清理volume_outlier表
            volume_query = "DELETE FROM volume_outlier WHERE create_time < NOW() - INTERVAL '%s days'"
            self.cursor.execute(volume_query, (days,))
            volume_deleted = self.cursor.rowcount
            
            # 清理oi_outlier表
            oi_query = "DELETE FROM oi_outlier WHERE create_time < NOW() - INTERVAL '%s days'"
            self.cursor.execute(oi_query, (days,))
            oi_deleted = self.cursor.rowcount
            
            # 清理processed_files表
            processed_query = "DELETE FROM processed_files WHERE processed_time < NOW() - INTERVAL '%s days'"
            self.cursor.execute(processed_query, (days,))
            processed_deleted = self.cursor.rowcount
            
            # 提交清理操作
            self.conn.commit()
            
            logger.info(f"✅ 数据清理完成:")
            logger.info(f"  volume_outlier: 删除 {volume_deleted} 条记录")
            logger.info(f"  oi_outlier: 删除 {oi_deleted} 条记录")
            logger.info(f"  processed_files: 删除 {processed_deleted} 条记录")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 清理旧数据失败: {e}")
            self.conn.rollback()
            return False
    
    def process_volume_outlier(self):
        """处理volume_outlier文件"""
        logger.info("🔄 开始处理volume_outlier文件...")
        
        # 获取最新文件和上一个文件
        file_path, csv_filename, previous_file = self.get_latest_csv_file('volume_outlier', 'volume_outlier_*.csv')
        if not file_path:
            logger.warning("⚠️ 未找到volume_outlier文件")
            return False
        
        # 检查是否已处理
        is_processed, status = self.check_file_processed(csv_filename, 'volume_outlier')
        if is_processed and status == 'success':
            logger.info(f"⏭️ 跳过已处理的文件: {csv_filename}")
            return True
        
        # 读取当前文件数据
        current_df = self.read_csv_data(file_path)
        if current_df is None:
            self.record_processed_file(csv_filename, 'volume_outlier', 0, 0, 'failed')
            return False
        
        # 如果有上一个文件，比较数据相似性
        if previous_file:
            logger.info("📊 比较当前文件与上一个文件的数据...")
            previous_df = self.read_csv_data(previous_file)
            if self.compare_data_similarity(current_df, previous_df, 'volume_outlier'):
                # 数据相同，记录为已处理但跳过插入
                self.record_processed_file(csv_filename, 'volume_outlier', file_path.stat().st_size, len(current_df), 'skipped')
                logger.info("⏭️ 数据与上一个文件相同，跳过插入")
                return True
        
        # 获取前一天的股票价格数据
        previous_stock_prices = self.get_previous_day_stock_prices(file_path)
        
        # 准备数据
        data_list = self.prepare_volume_data(current_df, previous_stock_prices)
        if not data_list:
            logger.warning("⚠️ 没有有效的volume数据")
            self.record_processed_file(csv_filename, 'volume_outlier', file_path.stat().st_size, 0, 'failed')
            return False
        
        # 插入数据
        inserted_count = self.insert_volume_data(data_list)
        
        # 记录处理结果
        status = 'success' if inserted_count > 0 else 'failed'
        self.record_processed_file(csv_filename, 'volume_outlier', file_path.stat().st_size, len(data_list), status)
        
        return inserted_count > 0
    
    def process_oi_outlier(self):
        """处理oi_outlier文件"""
        logger.info("🔄 开始处理oi_outlier文件...")
        
        # 获取最新文件和上一个文件
        file_path, csv_filename, previous_file = self.get_latest_csv_file('outlier', '*.csv')
        if not file_path:
            logger.warning("⚠️ 未找到oi_outlier文件")
            return False
        
        # 检查是否已处理
        is_processed, status = self.check_file_processed(csv_filename, 'oi_outlier')
        if is_processed and status == 'success':
            logger.info(f"⏭️ 跳过已处理的文件: {csv_filename}")
            return True
        
        # 读取当前文件数据
        current_df = self.read_csv_data(file_path)
        if current_df is None:
            self.record_processed_file(csv_filename, 'oi_outlier', 0, 0, 'failed')
            return False
        
        # 如果有上一个文件，比较数据相似性
        if previous_file:
            logger.info("📊 比较当前文件与上一个文件的数据...")
            previous_df = self.read_csv_data(previous_file)
            if self.compare_data_similarity(current_df, previous_df, 'oi_outlier'):
                # 数据相同，记录为已处理但跳过插入
                self.record_processed_file(csv_filename, 'oi_outlier', file_path.stat().st_size, len(current_df), 'skipped')
                logger.info("⏭️ 数据与上一个文件相同，跳过插入")
                return True
        
        # 准备数据
        data_list = self.prepare_oi_data(current_df)
        if not data_list:
            logger.warning("⚠️ 没有有效的OI数据")
            self.record_processed_file(csv_filename, 'oi_outlier', file_path.stat().st_size, 0, 'failed')
            return False
        
        # 插入数据
        inserted_count = self.insert_oi_data(data_list)
        
        # 记录处理结果
        status = 'success' if inserted_count > 0 else 'failed'
        self.record_processed_file(csv_filename, 'oi_outlier', file_path.stat().st_size, len(data_list), status)
        
        return inserted_count > 0
    
    def run(self, cleanup_days=7, no_cleanup=False):
        """运行主程序"""
        logger.info(f"🚀 开始处理文件夹: {self.folder_name}")
        
        # 连接数据库
        if not self.connect_db():
            return False
        
        try:
            # 处理volume_outlier
            volume_success = self.process_volume_outlier()
            
            # 处理oi_outlier
            oi_success = self.process_oi_outlier()
            
            # 数据处理完成后清理旧数据
            if volume_success or oi_success:
                logger.info("✅ 数据处理完成")
                # 清理旧数据（如果未禁用）
                if not no_cleanup:
                    self.cleanup_old_data(days=cleanup_days)
                else:
                    logger.info("⏭️ 跳过数据清理步骤")
                return True
            else:
                logger.warning("⚠️ 没有数据被处理")
                return False
                
        except Exception as e:
            logger.error(f"❌ 处理过程中发生错误: {e}")
            return False
        finally:
            self.close_db()

def main():
    parser = argparse.ArgumentParser(description='将异常数据插入到PostgreSQL数据库')
    parser.add_argument('--folder', type=str, required=True, 
                       choices=['data', 'priority_data'],
                       help='数据文件夹名称')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='显示详细日志')
    parser.add_argument('--cleanup-days', type=int, default=7,
                       help='清理超过指定天数的旧数据 (默认: 7天)')
    parser.add_argument('--no-cleanup', action='store_true',
                       help='跳过数据清理步骤')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 检查文件夹是否存在
    if not os.path.exists(args.folder):
        logger.error(f"❌ 文件夹不存在: {args.folder}")
        sys.exit(1)
    
    # 运行程序
    inserter = DatabaseInserter(args.folder)
    success = inserter.run(cleanup_days=args.cleanup_days, no_cleanup=args.no_cleanup)
    
    if success:
        logger.info("🎉 程序执行成功")
        sys.exit(0)
    else:
        logger.error("💥 程序执行失败")
        sys.exit(1)

if __name__ == "__main__":
    main()
