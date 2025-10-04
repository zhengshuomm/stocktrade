#!/usr/bin/env python3
"""
删除volume_outlier表中2025-10-03 15:00之后的所有数据

功能：
1. 连接neon数据库
2. 查询volume_outlier表中2025-10-03 15:00之后的数据
3. 删除这些数据
4. 显示清理统计信息

使用方法：
python3 program/cleanup_volume_outlier_after_3pm.py
python3 program/cleanup_volume_outlier_after_3pm.py --dry-run  # 只查看不删除
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import argparse
import logging
from datetime import datetime
import pytz

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
        logging.FileHandler('cleanup_volume_outlier.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class VolumeOutlierCleaner:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.pst_tz = pytz.timezone('US/Pacific')  # 美西时区
        
    def connect_db(self):
        """连接数据库"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
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
    
    def get_data_stats(self):
        """获取数据统计信息"""
        try:
            # 获取总数据量
            self.cursor.execute("SELECT COUNT(*) as total_count FROM volume_outlier")
            total_count = self.cursor.fetchone()['total_count']
            
            # 获取2025-10-03 15:00之后的数据量
            query = """
            SELECT COUNT(*) as after_3pm_count 
            FROM volume_outlier 
            WHERE (create_time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific')::date = '2025-10-03'
            AND (create_time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific')::time >= '15:00:00'
            """
            self.cursor.execute(query)
            after_3pm_count = self.cursor.fetchone()['after_3pm_count']
            
            # 获取2025-10-03 15:00之后的数据样例
            sample_query = """
            SELECT contractsymbol, create_time, 
                   (create_time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific') as pst_time
            FROM volume_outlier 
            WHERE (create_time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific')::date = '2025-10-03'
            AND (create_time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific')::time >= '15:00:00'
            ORDER BY create_time DESC
            LIMIT 10
            """
            self.cursor.execute(sample_query)
            sample_data = self.cursor.fetchall()
            
            # 获取2025-10-03 15:00之前的数据样例
            before_sample_query = """
            SELECT contractsymbol, create_time, 
                   (create_time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific') as pst_time
            FROM volume_outlier 
            WHERE (create_time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific')::date = '2025-10-03'
            AND (create_time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific')::time < '15:00:00'
            ORDER BY create_time DESC
            LIMIT 10
            """
            self.cursor.execute(before_sample_query)
            before_sample_data = self.cursor.fetchall()
            
            # 获取其他日期的数据量
            other_dates_query = """
            SELECT COUNT(*) as other_dates_count 
            FROM volume_outlier 
            WHERE (create_time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific')::date != '2025-10-03'
            """
            self.cursor.execute(other_dates_query)
            other_dates_count = self.cursor.fetchone()['other_dates_count']
            
            return {
                'total_count': total_count,
                'after_3pm_count': after_3pm_count,
                'before_3pm_count': len(before_sample_data),
                'other_dates_count': other_dates_count,
                'sample_data': sample_data,
                'before_sample_data': before_sample_data
            }
            
        except Exception as e:
            logger.error(f"❌ 获取数据统计失败: {e}")
            return None
    
    def cleanup_after_3pm_data(self, dry_run=False):
        """删除2025-10-03 15:00之后的数据"""
        try:
            if dry_run:
                logger.info("🔍 模拟运行模式 - 只查看不删除")
            
            # 获取清理前的统计信息
            stats = self.get_data_stats()
            if not stats:
                return False
            
            logger.info(f"📊 数据统计:")
            logger.info(f"  总数据量: {stats['total_count']:,} 条")
            logger.info(f"  2025-10-03 15:00之后的数据: {stats['after_3pm_count']:,} 条")
            logger.info(f"  2025-10-03 15:00之前的数据: {stats['before_3pm_count']:,} 条")
            logger.info(f"  其他日期的数据: {stats['other_dates_count']:,} 条")
            
            if stats['after_3pm_count'] == 0:
                logger.info("✅ 没有需要清理的数据")
                return True
            
            # 显示样例数据
            logger.info("📋 将被删除的数据样例 (2025-10-03 15:00之后):")
            for row in stats['sample_data']:
                logger.info(f"  {row['contractsymbol']} - {row['pst_time']}")
            
            if stats['before_sample_data']:
                logger.info("📋 将保留的数据样例 (2025-10-03 15:00之前):")
                for row in stats['before_sample_data']:
                    logger.info(f"  {row['contractsymbol']} - {row['pst_time']}")
            
            if dry_run:
                logger.info("🔍 模拟运行完成 - 未执行删除操作")
                return True
            
            # 执行删除操作
            delete_query = """
            DELETE FROM volume_outlier 
            WHERE (create_time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific')::date = '2025-10-03'
            AND (create_time AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific')::time >= '15:00:00'
            """
            
            logger.info("🗑️ 开始删除2025-10-03 15:00之后的数据...")
            self.cursor.execute(delete_query)
            deleted_count = self.cursor.rowcount
            
            # 提交删除操作
            self.conn.commit()
            
            logger.info(f"✅ 删除完成: {deleted_count:,} 条记录")
            
            # 获取删除后的统计信息
            self.cursor.execute("SELECT COUNT(*) as remaining_count FROM volume_outlier")
            remaining_count = self.cursor.fetchone()['remaining_count']
            logger.info(f"📊 剩余数据量: {remaining_count:,} 条")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 清理数据失败: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def run(self, dry_run=False):
        """运行清理程序"""
        logger.info("🚀 开始删除volume_outlier表中2025-10-03 15:00之后的数据")
        
        # 连接数据库
        if not self.connect_db():
            return False
        
        try:
            # 执行清理
            success = self.cleanup_after_3pm_data(dry_run=dry_run)
            
            if success:
                if dry_run:
                    logger.info("🎉 模拟运行完成")
                else:
                    logger.info("🎉 数据清理完成")
                return True
            else:
                logger.error("💥 数据清理失败")
                return False
                
        except Exception as e:
            logger.error(f"❌ 程序执行过程中发生错误: {e}")
            return False
        finally:
            self.close_db()

def main():
    parser = argparse.ArgumentParser(description='删除volume_outlier表中2025-10-03 15:00之后的所有数据')
    parser.add_argument('--dry-run', action='store_true',
                       help='模拟运行模式，只查看不删除数据')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='显示详细日志')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 运行清理程序
    cleaner = VolumeOutlierCleaner()
    success = cleaner.run(dry_run=args.dry_run)
    
    if success:
        logger.info("🎉 程序执行成功")
        exit(0)
    else:
        logger.error("💥 程序执行失败")
        exit(1)

if __name__ == "__main__":
    main()
