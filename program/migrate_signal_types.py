#!/usr/bin/env python3
"""
数据库迁移脚本 - 将signal_type列替换为signal_type_id

使用方法：
python3 program/migrate_signal_types.py
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

def migrate_volume_outlier_table(cursor):
    """迁移volume_outlier表"""
    try:
        # 添加新的signal_type_id列
        logger.info("🔄 添加signal_type_id列到volume_outlier表...")
        cursor.execute("ALTER TABLE volume_outlier ADD COLUMN IF NOT EXISTS signal_type_id INTEGER REFERENCES signal_types(id)")
        
        # 更新现有数据
        logger.info("🔄 更新现有volume_outlier数据...")
        cursor.execute("""
            UPDATE volume_outlier 
            SET signal_type_id = st.id 
            FROM signal_types st 
            WHERE volume_outlier.signal_type = st.signal_name
        """)
        
        # 删除旧的signal_type列
        logger.info("🔄 删除旧的signal_type列...")
        cursor.execute("ALTER TABLE volume_outlier DROP COLUMN IF EXISTS signal_type")
        
        logger.info("✅ volume_outlier表迁移完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 迁移volume_outlier表失败: {e}")
        return False

def migrate_oi_outlier_table(cursor):
    """迁移oi_outlier表"""
    try:
        # 添加新的signal_type_id列
        logger.info("🔄 添加signal_type_id列到oi_outlier表...")
        cursor.execute("ALTER TABLE oi_outlier ADD COLUMN IF NOT EXISTS signal_type_id INTEGER REFERENCES signal_types(id)")
        
        # 更新现有数据
        logger.info("🔄 更新现有oi_outlier数据...")
        cursor.execute("""
            UPDATE oi_outlier 
            SET signal_type_id = st.id 
            FROM signal_types st 
            WHERE oi_outlier.signal_type = st.signal_name
        """)
        
        # 删除旧的signal_type列
        logger.info("🔄 删除旧的signal_type列...")
        cursor.execute("ALTER TABLE oi_outlier DROP COLUMN IF EXISTS signal_type")
        
        logger.info("✅ oi_outlier表迁移完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 迁移oi_outlier表失败: {e}")
        return False

def verify_migration(cursor):
    """验证迁移结果"""
    try:
        # 检查volume_outlier表结构
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'volume_outlier' 
            AND column_name IN ('signal_type', 'signal_type_id')
            ORDER BY column_name
        """)
        volume_columns = cursor.fetchall()
        
        # 检查oi_outlier表结构
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'oi_outlier' 
            AND column_name IN ('signal_type', 'signal_type_id')
            ORDER BY column_name
        """)
        oi_columns = cursor.fetchall()
        
        logger.info("📊 迁移验证结果:")
        logger.info(f"  volume_outlier表: {volume_columns}")
        logger.info(f"  oi_outlier表: {oi_columns}")
        
        # 检查数据完整性
        cursor.execute("SELECT COUNT(*) FROM volume_outlier WHERE signal_type_id IS NULL")
        volume_null_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM oi_outlier WHERE signal_type_id IS NULL")
        oi_null_count = cursor.fetchone()[0]
        
        logger.info(f"  volume_outlier表中signal_type_id为NULL的记录: {volume_null_count}")
        logger.info(f"  oi_outlier表中signal_type_id为NULL的记录: {oi_null_count}")
        
        return volume_null_count == 0 and oi_null_count == 0
        
    except Exception as e:
        logger.error(f"❌ 验证迁移结果失败: {e}")
        return False

def main():
    """主函数"""
    logger.info("🚀 开始信号类型迁移...")
    
    conn = None
    cursor = None
    
    try:
        # 连接数据库
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logger.info("✅ 数据库连接成功")
        
        # 迁移表
        if not migrate_volume_outlier_table(cursor):
            return False
        
        if not migrate_oi_outlier_table(cursor):
            return False
        
        # 验证迁移
        if not verify_migration(cursor):
            logger.warning("⚠️ 迁移验证发现问题")
        
        # 提交事务
        conn.commit()
        logger.info("🎉 信号类型迁移完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 信号类型迁移失败: {e}")
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
