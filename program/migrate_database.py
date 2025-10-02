#!/usr/bin/env python3
"""
数据库迁移脚本 - 删除volume_outlier表中的volume列

使用方法：
python3 program/migrate_database.py
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

def check_column_exists(cursor, table_name, column_name):
    """检查列是否存在"""
    try:
        query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s AND column_name = %s
        """
        cursor.execute(query, (table_name, column_name))
        result = cursor.fetchone()
        return result is not None
    except Exception as e:
        logger.error(f"❌ 检查列存在性失败: {e}")
        return False

def drop_volume_column(cursor):
    """删除volume_outlier表中的volume列"""
    try:
        # 检查列是否存在
        if not check_column_exists(cursor, 'volume_outlier', 'volume'):
            logger.info("ℹ️ volume列不存在，无需删除")
            return True
        
        # 删除列
        alter_query = "ALTER TABLE volume_outlier DROP COLUMN volume"
        cursor.execute(alter_query)
        logger.info("✅ 成功删除volume_outlier表中的volume列")
        return True
        
    except Exception as e:
        logger.error(f"❌ 删除volume列失败: {e}")
        return False

def main():
    """主函数"""
    logger.info("🚀 开始数据库迁移...")
    
    conn = None
    cursor = None
    
    try:
        # 连接数据库
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logger.info("✅ 数据库连接成功")
        
        # 删除volume列
        if drop_volume_column(cursor):
            # 提交事务
            conn.commit()
            logger.info("🎉 数据库迁移完成")
            return True
        else:
            conn.rollback()
            logger.error("💥 数据库迁移失败")
            return False
        
    except Exception as e:
        logger.error(f"❌ 数据库迁移失败: {e}")
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
