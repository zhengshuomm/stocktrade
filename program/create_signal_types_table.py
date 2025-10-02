#!/usr/bin/env python3
"""
创建信号类型表并插入所有可能的信号类型

使用方法：
python3 program/create_signal_types_table.py
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

def create_signal_types_table(cursor):
    """创建signal_types表"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS signal_types (
        id SERIAL PRIMARY KEY,
        signal_name VARCHAR(100) NOT NULL UNIQUE,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        cursor.execute(create_table_sql)
        logger.info("✅ signal_types表创建成功")
        return True
    except Exception as e:
        logger.error(f"❌ 创建signal_types表失败: {e}")
        return False

def insert_signal_types(cursor):
    """插入所有可能的信号类型"""
    signal_types = [
        ("买 Call，看涨", "多头买Call期权，看涨信号"),
        ("卖 Call，看跌", "空头卖Call期权，看跌信号"),
        ("买 Put，看跌", "多头买Put期权，看跌信号"),
        ("卖 Put，看涨/对冲", "空头卖Put期权，看涨或对冲信号"),
        ("空头平仓 Call，回补信号，看涨", "空头平仓Call期权，回补信号，看涨"),
        ("空头平仓 Put，回补信号，看跌", "空头平仓Put期权，回补信号，看跌"),
        ("多头平仓 Call，获利了结，看跌", "多头平仓Call期权，获利了结，看跌"),
        ("多头平仓 Put，获利了结，看涨", "多头平仓Put期权，获利了结，看涨"),
        ("空头卖 Call，看跌/看不涨", "空头卖Call期权，看跌或看不涨"),
        ("空头卖 Put，看涨/看不跌", "空头卖Put期权，看涨或看不跌"),
        ("多头买 Call，看涨", "多头买Call期权，看涨"),
        ("多头买 Put，看跌", "多头买Put期权，看跌"),
        ("空头买 Call，看跌", "空头买Call期权，看跌"),
        ("空头买 Put，看涨", "空头买Put期权，看涨"),
        ("多头卖 Call，看跌", "多头卖Call期权，看跌"),
        ("多头卖 Put，看涨", "多头卖Put期权，看涨"),
        ("空头平仓 Call，回补信号，看涨", "空头平仓Call期权，回补信号，看涨"),
        ("空头平仓 Put，回补信号，看跌", "空头平仓Put期权，回补信号，看跌"),
        ("多头平仓 Call，获利了结，看跌", "多头平仓Call期权，获利了结，看跌"),
        ("多头平仓 Put，获利了结，看涨", "多头平仓Put期权，获利了结，看涨")
    ]
    
    try:
        # 使用ON CONFLICT DO NOTHING避免重复插入
        insert_query = """
        INSERT INTO signal_types (signal_name, description) 
        VALUES (%s, %s) 
        ON CONFLICT (signal_name) DO NOTHING
        """
        
        for signal_name, description in signal_types:
            cursor.execute(insert_query, (signal_name, description))
        
        logger.info(f"✅ 成功插入/更新 {len(signal_types)} 个信号类型")
        return True
        
    except Exception as e:
        logger.error(f"❌ 插入信号类型失败: {e}")
        return False

def show_signal_types(cursor):
    """显示所有信号类型"""
    try:
        cursor.execute("SELECT id, signal_name, description FROM signal_types ORDER BY id")
        results = cursor.fetchall()
        
        logger.info("📊 当前信号类型列表:")
        for signal_id, signal_name, description in results:
            logger.info(f"  {signal_id}: {signal_name} - {description}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 查询信号类型失败: {e}")
        return False

def main():
    """主函数"""
    logger.info("🚀 开始创建信号类型表...")
    
    conn = None
    cursor = None
    
    try:
        # 连接数据库
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logger.info("✅ 数据库连接成功")
        
        # 创建表
        if not create_signal_types_table(cursor):
            return False
        
        # 插入信号类型
        if not insert_signal_types(cursor):
            return False
        
        # 显示信号类型
        if not show_signal_types(cursor):
            return False
        
        # 提交事务
        conn.commit()
        logger.info("🎉 信号类型表创建完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 创建信号类型表失败: {e}")
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
