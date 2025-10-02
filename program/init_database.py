#!/usr/bin/env python3
"""
数据库初始化脚本 - 创建必要的表结构

使用方法：
python3 program/init_database.py
"""

import psycopg2
import sys
import logging

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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_processed_files_table(cursor):
    """创建processed_files表"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS processed_files (
        id SERIAL PRIMARY KEY,
        folder_name VARCHAR(50) NOT NULL,
        csv_filename VARCHAR(255) NOT NULL,
        file_type VARCHAR(50) NOT NULL,
        processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        file_size BIGINT,
        row_count INTEGER,
        status VARCHAR(20) DEFAULT 'success',
        UNIQUE(folder_name, csv_filename)
    );
    """
    
    try:
        cursor.execute(create_table_sql)
        logger.info("✅ processed_files表创建成功")
        return True
    except Exception as e:
        logger.error(f"❌ 创建processed_files表失败: {e}")
        return False

def create_volume_outlier_table(cursor):
    """创建volume_outlier表"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS volume_outlier (
        id SERIAL PRIMARY KEY,
        contractSymbol VARCHAR(100) NOT NULL,
        strike DECIMAL(10,2),
        signal_type VARCHAR(100),
        folder_name VARCHAR(50) NOT NULL,
        option_type VARCHAR(10),
        volume_old DECIMAL(15,2),
        volume_new DECIMAL(15,2),
        amount_threshold DECIMAL(20,2),
        amount_to_market_cap DECIMAL(20,10),
        openInterest_new DECIMAL(15,2),
        expiry_date VARCHAR(20),
        lastPrice_new DECIMAL(10,4),
        lastPrice_old DECIMAL(10,4),
        volume DECIMAL(15,2),
        symbol VARCHAR(20),
        stock_price_new DECIMAL(10,4),
        stock_price_old DECIMAL(10,4),
        stock_price_new_open DECIMAL(10,4),
        stock_price_new_high DECIMAL(10,4),
        stock_price_new_low DECIMAL(10,4),
        create_time TIMESTAMP NOT NULL,
        UNIQUE(contractSymbol, folder_name, create_time)
    );
    """
    
    try:
        cursor.execute(create_table_sql)
        logger.info("✅ volume_outlier表创建成功")
        return True
    except Exception as e:
        logger.error(f"❌ 创建volume_outlier表失败: {e}")
        return False

def create_oi_outlier_table(cursor):
    """创建oi_outlier表"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS oi_outlier (
        id SERIAL PRIMARY KEY,
        contractSymbol VARCHAR(100) NOT NULL,
        strike DECIMAL(10,2),
        oi_change DECIMAL(15,2),
        signal_type VARCHAR(100),
        folder_name VARCHAR(50) NOT NULL,
        option_type VARCHAR(10),
        openInterest_new DECIMAL(15,2),
        openInterest_old DECIMAL(15,2),
        amount_threshold DECIMAL(20,2),
        amount_to_market_cap DECIMAL(20,10),
        expiry_date VARCHAR(20),
        lastPrice_new DECIMAL(10,4),
        lastPrice_old DECIMAL(10,4),
        volume DECIMAL(15,2),
        symbol VARCHAR(20),
        stock_price_new DECIMAL(10,4),
        stock_price_old DECIMAL(10,4),
        stock_price_new_open DECIMAL(10,4),
        stock_price_new_high DECIMAL(10,4),
        stock_price_new_low DECIMAL(10,4),
        create_time TIMESTAMP NOT NULL,
        UNIQUE(contractSymbol, folder_name, create_time)
    );
    """
    
    try:
        cursor.execute(create_table_sql)
        logger.info("✅ oi_outlier表创建成功")
        return True
    except Exception as e:
        logger.error(f"❌ 创建oi_outlier表失败: {e}")
        return False

def create_indexes(cursor):
    """创建索引以提高查询性能"""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_processed_files_folder ON processed_files(folder_name);",
        "CREATE INDEX IF NOT EXISTS idx_processed_files_type ON processed_files(file_type);",
        "CREATE INDEX IF NOT EXISTS idx_volume_outlier_symbol ON volume_outlier(symbol);",
        "CREATE INDEX IF NOT EXISTS idx_volume_outlier_folder ON volume_outlier(folder_name);",
        "CREATE INDEX IF NOT EXISTS idx_volume_outlier_time ON volume_outlier(create_time);",
        "CREATE INDEX IF NOT EXISTS idx_oi_outlier_symbol ON oi_outlier(symbol);",
        "CREATE INDEX IF NOT EXISTS idx_oi_outlier_folder ON oi_outlier(folder_name);",
        "CREATE INDEX IF NOT EXISTS idx_oi_outlier_time ON oi_outlier(create_time);"
    ]
    
    success_count = 0
    for index_sql in indexes:
        try:
            cursor.execute(index_sql)
            success_count += 1
        except Exception as e:
            logger.warning(f"⚠️ 创建索引失败: {e}")
    
    logger.info(f"✅ 成功创建 {success_count}/{len(indexes)} 个索引")
    return success_count == len(indexes)

def main():
    """主函数"""
    logger.info("🚀 开始初始化数据库...")
    
    conn = None
    cursor = None
    
    try:
        # 连接数据库
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logger.info("✅ 数据库连接成功")
        
        # 创建表
        tables_created = 0
        if create_processed_files_table(cursor):
            tables_created += 1
        if create_volume_outlier_table(cursor):
            tables_created += 1
        if create_oi_outlier_table(cursor):
            tables_created += 1
        
        # 创建索引
        create_indexes(cursor)
        
        # 提交事务
        conn.commit()
        
        logger.info(f"🎉 数据库初始化完成，成功创建 {tables_created}/3 个表")
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据库初始化失败: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("🔌 数据库连接已关闭")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
