#!/usr/bin/env python3
"""
测试数据库插入程序

使用方法：
python3 program/test_insert_db.py --folder data
python3 program/test_insert_db.py --folder priority_data
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from insert_outliers_to_db import DatabaseInserter
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_database_connection(folder_name):
    """测试数据库连接"""
    logger.info(f"🔍 测试数据库连接 (文件夹: {folder_name})")
    
    inserter = DatabaseInserter(folder_name)
    
    # 测试连接
    if inserter.connect_db():
        logger.info("✅ 数据库连接成功")
        
        # 测试查询processed_files表
        try:
            inserter.cursor.execute("SELECT COUNT(*) FROM processed_files WHERE folder_name = %s", (folder_name,))
            count = inserter.cursor.fetchone()[0]
            logger.info(f"📊 已处理文件数量: {count}")
        except Exception as e:
            logger.warning(f"⚠️ 查询processed_files表失败: {e}")
        
        inserter.close_db()
        return True
    else:
        logger.error("❌ 数据库连接失败")
        return False

def test_file_detection(folder_name):
    """测试文件检测"""
    logger.info(f"🔍 测试文件检测 (文件夹: {folder_name})")
    
    inserter = DatabaseInserter(folder_name)
    
    # 测试volume_outlier文件检测
    volume_file, volume_filename = inserter.get_latest_csv_file('volume_outlier', 'volume_outlier_*.csv')
    if volume_file:
        logger.info(f"✅ 找到volume文件: {volume_filename}")
    else:
        logger.warning("⚠️ 未找到volume文件")
    
    # 测试oi_outlier文件检测
    oi_file, oi_filename = inserter.get_latest_csv_file('outlier', '*.csv')
    if oi_file:
        logger.info(f"✅ 找到OI文件: {oi_filename}")
    else:
        logger.warning("⚠️ 未找到OI文件")
    
    return volume_file is not None or oi_file is not None

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='测试数据库插入程序')
    parser.add_argument('--folder', type=str, required=True, 
                       choices=['data', 'priority_data'],
                       help='数据文件夹名称')
    
    args = parser.parse_args()
    
    logger.info(f"🧪 开始测试数据库插入程序 (文件夹: {args.folder})")
    
    # 测试数据库连接
    db_ok = test_database_connection(args.folder)
    
    # 测试文件检测
    files_ok = test_file_detection(args.folder)
    
    if db_ok and files_ok:
        logger.info("🎉 所有测试通过")
        return True
    else:
        logger.error("💥 测试失败")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
