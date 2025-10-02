#!/usr/bin/env python3
"""
数据库插入程序 - 将异常数据插入到PostgreSQL数据库

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
            return None, None
        
        csv_files = list(folder_path.glob(file_pattern))
        if not csv_files:
            logger.warning(f"⚠️ 未找到匹配的CSV文件: {folder_path}/{file_pattern}")
            return None, None
        
        # 按修改时间排序，获取最新文件
        latest_file = max(csv_files, key=os.path.getmtime)
        logger.info(f"📄 找到最新文件: {latest_file}")
        return latest_file, latest_file.name
    
    def read_csv_data(self, file_path):
        """读取CSV文件数据"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"📊 读取CSV文件: {file_path.name}, 行数: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"❌ 读取CSV文件失败: {file_path}: {e}")
            return None
    
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
    
    def prepare_volume_data(self, df):
        """准备volume_outlier数据"""
        if df is None or df.empty:
            return []
        
        data_list = []
        current_time = datetime.now()
        
        for _, row in df.iterrows():
            try:
                data = {
                    'contractSymbol': str(row.get('contractSymbol', '')),
                    'strike': self.format_float_precision(row.get('strike')),
                    'signal_type': str(row.get('signal_type', '')),
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
                logger.warning(f"⚠️ 处理volume数据行失败: {e}")
                continue
        
        return data_list
    
    def prepare_oi_data(self, df):
        """准备oi_outlier数据"""
        if df is None or df.empty:
            return []
        
        data_list = []
        current_time = datetime.now()
        
        for _, row in df.iterrows():
            try:
                data = {
                    'contractSymbol': str(row.get('contractSymbol', '')),
                    'strike': self.format_float_precision(row.get('strike')),
                    'oi_change': self.format_float_precision(row.get('oi_change')),
                    'signal_type': str(row.get('signal_type', '')),
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
                'contractSymbol', 'strike', 'signal_type', 'folder_name', 'option_type',
                'volume_old', 'volume_new', 'amount_threshold', 'amount_to_market_cap',
                'openInterest_new', 'expiry_date', 'lastPrice_new', 'lastPrice_old',
                'volume', 'symbol',                 'stock_price_new', 'stock_price_old', 'stock_price_new_open',
                'stock_price_new_high', 'stock_price_new_low', 'create_time'
            ]
            
            values = []
            for data in data_list:
                values.append(tuple(data.get(col) for col in columns))
            
            # 批量插入
            insert_query = f"""
            INSERT INTO volume_outlier ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (contractSymbol, folder_name, create_time) 
            DO UPDATE SET
                strike = EXCLUDED.strike,
                signal_type = EXCLUDED.signal_type,
                option_type = EXCLUDED.option_type,
                volume_old = EXCLUDED.volume_old,
                volume_new = EXCLUDED.volume_new,
                amount_threshold = EXCLUDED.amount_threshold,
                amount_to_market_cap = EXCLUDED.amount_to_market_cap,
                openInterest_new = EXCLUDED.openInterest_new,
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
                'contractSymbol', 'strike', 'oi_change', 'signal_type', 'folder_name',
                'option_type', 'openInterest_new', 'openInterest_old', 'amount_threshold',
                'amount_to_market_cap', 'expiry_date', 'lastPrice_new', 'lastPrice_old',
                'volume', 'symbol',                 'stock_price_new', 'stock_price_old', 'stock_price_new_open',
                'stock_price_new_high', 'stock_price_new_low', 'create_time'
            ]
            
            values = []
            for data in data_list:
                values.append(tuple(data.get(col) for col in columns))
            
            # 批量插入
            insert_query = f"""
            INSERT INTO oi_outlier ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (contractSymbol, folder_name, create_time) 
            DO UPDATE SET
                strike = EXCLUDED.strike,
                oi_change = EXCLUDED.oi_change,
                signal_type = EXCLUDED.signal_type,
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
    
    def process_volume_outlier(self):
        """处理volume_outlier文件"""
        logger.info("🔄 开始处理volume_outlier文件...")
        
        # 获取最新文件
        file_path, csv_filename = self.get_latest_csv_file('volume_outlier', 'volume_outlier_*.csv')
        if not file_path:
            logger.warning("⚠️ 未找到volume_outlier文件")
            return False
        
        # 检查是否已处理
        is_processed, status = self.check_file_processed(csv_filename, 'volume_outlier')
        if is_processed and status == 'success':
            logger.info(f"⏭️ 跳过已处理的文件: {csv_filename}")
            return True
        
        # 读取数据
        df = self.read_csv_data(file_path)
        if df is None:
            self.record_processed_file(csv_filename, 'volume_outlier', 0, 0, 'failed')
            return False
        
        # 准备数据
        data_list = self.prepare_volume_data(df)
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
        
        # 获取最新文件
        file_path, csv_filename = self.get_latest_csv_file('outlier', '*.csv')
        if not file_path:
            logger.warning("⚠️ 未找到oi_outlier文件")
            return False
        
        # 检查是否已处理
        is_processed, status = self.check_file_processed(csv_filename, 'oi_outlier')
        if is_processed and status == 'success':
            logger.info(f"⏭️ 跳过已处理的文件: {csv_filename}")
            return True
        
        # 读取数据
        df = self.read_csv_data(file_path)
        if df is None:
            self.record_processed_file(csv_filename, 'oi_outlier', 0, 0, 'failed')
            return False
        
        # 准备数据
        data_list = self.prepare_oi_data(df)
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
    
    def run(self):
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
            
            # 总结
            if volume_success or oi_success:
                logger.info("✅ 数据处理完成")
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
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 检查文件夹是否存在
    if not os.path.exists(args.folder):
        logger.error(f"❌ 文件夹不存在: {args.folder}")
        sys.exit(1)
    
    # 运行程序
    inserter = DatabaseInserter(args.folder)
    success = inserter.run()
    
    if success:
        logger.info("🎉 程序执行成功")
        sys.exit(0)
    else:
        logger.error("💥 程序执行失败")
        sys.exit(1)

if __name__ == "__main__":
    main()
