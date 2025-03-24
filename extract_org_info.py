#!/usr/bin/env python3
"""
html_extractor_cli.py
用于从数据库中提取HTML内容并解析特定信息的命令行工具
"""

import argparse
import json
import os
import sys
import time
from typing import Dict, List, Any

# 从项目根目录导入模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from html_extractor.extract_table_from_remark import DBExtractor, HTMLExtractor
from utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='从数据库的HTML内容中提取组织信息')
    # 简化参数，只保留保存选项
    parser.add_argument('--mapping', help='可选的字段映射配置JSON文件，不提供则使用默认映射')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--update_db', action='store_true', help='将提取结果更新到数据库（默认）')
    group.add_argument('--output', help='将结果保存到指定的JSON文件而不更新数据库')

    args = parser.parse_args()

    # 初始化HTML提取器
    html_extractor = HTMLExtractor(args.mapping)

    # 创建数据库提取器并连接数据库
    db_extractor = DBExtractor()
    if not db_extractor.connect():
        logger.error("无法连接到数据库，退出处理")
        return

    # 将数据库提取器分配给HTML提取器
    html_extractor.db_extractor = db_extractor

    try:
        start_time = time.time()
        logger.info("开始处理所有组织的HTML内容提取...")

        # 默认处理所有组织
        results = html_extractor.process_all_organizations(
            # 如果没有指定输出文件，则默认更新数据库
            update_db=True if not args.output else False
        )

        # 如果指定了输出文件，将结果保存到文件
        if args.output and results:
            html_extractor.save_results_to_file(results, args.output)
            logger.info(f"已将提取结果保存到文件: {args.output}")
        elif not args.output:
            logger.info("已将提取结果更新到数据库")

        end_time = time.time()
        logger.info(f"处理完成，共处理 {len(results)} 个组织，耗时: {end_time - start_time:.2f} 秒")

    finally:
        # 关闭资源
        html_extractor.close()
        logger.info("HTML提取器资源已释放")


if __name__ == "__main__":
    main()