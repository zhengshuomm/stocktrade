#!/usr/bin/env python3
"""
独立的数据清理脚本 - 清理超过指定天数的旧数据

使用方法：
python3 program/cleanup_old_data.py --days 7
python3 program/cleanup_old_data.py --days 30 --dry-run
"""

import psycopg2
import sys
import argparse
import logging
from datetime import datetime

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

class DataCleaner:
    def __init__(self):
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
    
    def get_data_counts(self, days):
        """获取要删除的数据统计"""
        try:
            # 统计volume_outlier表
            self.cursor.execute("""
                SELECT COUNT(*) FROM volume_outlier 
                WHERE create_time < NOW() - INTERVAL '%s days'
            """, (days,))
            volume_count = self.cursor.fetchone()[0]
            
            # 统计oi_outlier表
            self.cursor.execute("""
                SELECT COUNT(*) FROM oi_outlier 
                WHERE create_time < NOW() - INTERVAL '%s days'
            """, (days,))
            oi_count = self.cursor.fetchone()[0]
            
            # 统计processed_files表
            self.cursor.execute("""
                SELECT COUNT(*) FROM processed_files 
                WHERE processed_time < NOW() - INTERVAL '%s days'
            """, (days,))
            processed_count = self.cursor.fetchone()[0]
            
            return volume_count, oi_count, processed_count
            
        except Exception as e:
            logger.error(f"❌ 获取数据统计失败: {e}")
            return 0, 0, 0
    
    def cleanup_data(self, days, dry_run=False):
        """清理超过指定天数的旧数据"""
        try:
            if dry_run:
                logger.info(f"🔍 模拟清理超过{days}天的旧数据...")
                
                # 获取统计信息
                volume_count, oi_count, processed_count = self.get_data_counts(days)
                
                logger.info(f"📊 将要删除的数据:")
                logger.info(f"  volume_outlier: {volume_count} 条记录")
                logger.info(f"  oi_outlier: {oi_count} 条记录")
                logger.info(f"  processed_files: {processed_count} 条记录")
                
                total_count = volume_count + oi_count + processed_count
                if total_count == 0:
                    logger.info("✅ 没有需要清理的数据")
                else:
                    logger.info(f"📈 总计将删除 {total_count} 条记录")
                
                return True
            else:
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
                
                total_deleted = volume_deleted + oi_deleted + processed_deleted
                logger.info(f"📈 总计删除 {total_deleted} 条记录")
                
                return True
                
        except Exception as e:
            logger.error(f"❌ 清理旧数据失败: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def show_current_data_stats(self):
        """显示当前数据统计"""
        try:
            logger.info("📊 当前数据库统计:")
            
            # volume_outlier表统计
            self.cursor.execute("SELECT COUNT(*) FROM volume_outlier")
            volume_total = self.cursor.fetchone()[0]
            
            self.cursor.execute("""
                SELECT MIN(create_time), MAX(create_time) FROM volume_outlier
            """)
            volume_min, volume_max = self.cursor.fetchone()
            
            # oi_outlier表统计
            self.cursor.execute("SELECT COUNT(*) FROM oi_outlier")
            oi_total = self.cursor.fetchone()[0]
            
            self.cursor.execute("""
                SELECT MIN(create_time), MAX(create_time) FROM oi_outlier
            """)
            oi_min, oi_max = self.cursor.fetchone()
            
            # processed_files表统计
            self.cursor.execute("SELECT COUNT(*) FROM processed_files")
            processed_total = self.cursor.fetchone()[0]
            
            self.cursor.execute("""
                SELECT MIN(processed_time), MAX(processed_time) FROM processed_files
            """)
            processed_min, processed_max = self.cursor.fetchone()
            
            logger.info(f"  volume_outlier: {volume_total} 条记录")
            if volume_min and volume_max:
                logger.info(f"    时间范围: {volume_min} 到 {volume_max}")
            
            logger.info(f"  oi_outlier: {oi_total} 条记录")
            if oi_min and oi_max:
                logger.info(f"    时间范围: {oi_min} 到 {oi_max}")
            
            logger.info(f"  processed_files: {processed_total} 条记录")
            if processed_min and processed_max:
                logger.info(f"    时间范围: {processed_min} 到 {processed_max}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 获取数据统计失败: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='清理超过指定天数的旧数据')
    parser.add_argument('--days', type=int, default=7,
                       help='清理超过指定天数的数据 (默认: 7天)')
    parser.add_argument('--dry-run', action='store_true',
                       help='模拟运行，不实际删除数据')
    parser.add_argument('--stats', action='store_true',
                       help='只显示当前数据统计，不执行清理')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='显示详细日志')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 创建清理器
    cleaner = DataCleaner()
    
    # 连接数据库
    if not cleaner.connect_db():
        sys.exit(1)
    
    try:
        # 显示当前统计
        cleaner.show_current_data_stats()
        
        if args.stats:
            logger.info("📊 统计信息显示完成")
            return True
        
        # 执行清理
        success = cleaner.cleanup_data(args.days, dry_run=args.dry_run)
        
        if success:
            if args.dry_run:
                logger.info("🔍 模拟运行完成")
            else:
                logger.info("🎉 数据清理完成")
            return True
        else:
            logger.error("💥 数据清理失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 程序执行失败: {e}")
        return False
    finally:
        cleaner.close_db()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
