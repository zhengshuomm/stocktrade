#!/usr/bin/env python3
"""
清理signal_type列的脚本
确保volume_outlier和oi_outlier表中完全删除signal_type列
"""

import psycopg2
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def cleanup_signal_type_columns():
    """清理signal_type列"""
    try:
        # 连接数据库
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logger.info("✅ 数据库连接成功")
        
        # 检查当前列状态
        logger.info("🔍 检查当前列状态...")
        
        # 检查volume_outlier表
        cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns 
        WHERE table_name = 'volume_outlier' 
        AND table_schema = 'public'
        AND column_name IN ('signal_type', 'signal_type_id')
        ORDER BY column_name
        """)
        volume_columns = cursor.fetchall()
        logger.info(f"volume_outlier表相关列: {volume_columns}")
        
        # 检查oi_outlier表
        cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns 
        WHERE table_name = 'oi_outlier' 
        AND table_schema = 'public'
        AND column_name IN ('signal_type', 'signal_type_id')
        ORDER BY column_name
        """)
        oi_columns = cursor.fetchall()
        logger.info(f"oi_outlier表相关列: {oi_columns}")
        
        # 删除signal_type列（如果存在）
        tables_to_clean = ['volume_outlier', 'oi_outlier']
        
        for table in tables_to_clean:
            logger.info(f"🔄 处理表: {table}")
            
            # 检查是否存在signal_type列
            cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = '{table}' 
            AND table_schema = 'public'
            AND column_name = 'signal_type'
            """)
            
            if cursor.fetchone():
                logger.info(f"  🗑️ 删除{table}表的signal_type列...")
                cursor.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS signal_type")
                logger.info(f"  ✅ {table}表的signal_type列已删除")
            else:
                logger.info(f"  ✅ {table}表没有signal_type列")
        
        # 提交更改
        conn.commit()
        logger.info("✅ 所有更改已提交")
        
        # 最终验证
        logger.info("🔍 最终验证...")
        for table in tables_to_clean:
            cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_name = '{table}' 
            AND table_schema = 'public'
            AND column_name IN ('signal_type', 'signal_type_id')
            ORDER BY column_name
            """)
            columns = cursor.fetchall()
            logger.info(f"{table}表最终列状态: {columns}")
        
        # 关闭连接
        cursor.close()
        conn.close()
        logger.info("✅ 数据库连接已关闭")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 清理过程中发生错误: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

def main():
    """主函数"""
    logger.info("🚀 开始清理signal_type列...")
    
    success = cleanup_signal_type_columns()
    
    if success:
        logger.info("🎉 清理完成！")
    else:
        logger.error("💥 清理失败！")

if __name__ == "__main__":
    main()
