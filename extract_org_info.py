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
from html_extractor.extract_content_from_remark import BaiduBaikeExtractor
from utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='从数据库的HTML内容中提取组织信息')
    # 参数设置
    parser.add_argument('--table_mapping', help='表格提取的字段映射配置JSON文件，不提供则使用默认映射')
    parser.add_argument('--content_mapping', help='内容提取的字段映射配置JSON文件，不提供则使用默认映射')
    parser.add_argument('--only_table', action='store_true', help='仅提取表格信息')
    parser.add_argument('--only_content', action='store_true', help='仅提取内容信息')
    parser.add_argument('--org_id', type=int, help='仅处理指定ID的组织')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--update_db', action='store_true', help='将提取结果更新到数据库（默认）')
    group.add_argument('--output', help='将结果保存到指定的JSON文件而不更新数据库')

    args = parser.parse_args()

    # 创建数据库提取器并连接数据库
    db_extractor = DBExtractor()
    if not db_extractor.connect():
        logger.error("无法连接到数据库，退出处理")
        return

    # 决定提取方式
    extract_table = not args.only_content
    extract_content = not args.only_table

    start_time = time.time()
    results = []

    try:
        # 1. 处理表格提取
        if extract_table:
            logger.info("开始表格信息提取...")
            table_extractor = HTMLExtractor(args.table_mapping)
            table_extractor.db_extractor = db_extractor

            # 处理单个组织或所有组织
            if args.org_id:
                table_results = [table_extractor.process_organization(
                    args.org_id,
                    update_db=True if not args.output else False
                )]
                logger.info(f"已完成组织ID={args.org_id}的表格信息提取")
            else:
                table_results = table_extractor.process_all_organizations(
                    update_db=True if not args.output else False
                )
                logger.info(f"已完成所有组织的表格信息提取，共处理 {len(table_results)} 个组织")

            results.extend(table_results)

        # 2. 处理内容提取
        if extract_content:
            logger.info("开始内容信息提取...")
            content_extractor = BaiduBaikeExtractor(args.content_mapping)
            content_extractor.db_extractor = db_extractor

            # 处理单个组织或所有组织
            if args.org_id:
                content_results = content_extractor.process_organization(
                    args.org_id,
                    update_db=True if not args.output else False
                )
                logger.info(f"已完成组织ID={args.org_id}的内容信息提取")

                # 如果只提取内容，则需要将结果添加到results中
                if not extract_table:
                    results.append({
                        "org_id": args.org_id,
                        "org_name": db_extractor.get_org_name_by_id(args.org_id),
                        "extraction_result": content_results
                    })
            else:
                content_results = content_extractor.process_all_organizations(
                    update_db=True if not args.output else False
                )
                logger.info(f"已完成所有组织的内容信息提取，共处理 {len(content_results)} 个组织")

                # 如果只提取内容，则需要将结果添加到results中
                if not extract_table:
                    results.extend(content_results)

        # 如果指定了输出文件，将结果保存到文件
        if args.output and results:
            # 使用table_extractor的保存方法（如果有），否则使用content_extractor的保存方法
            if extract_table:
                table_extractor.save_results_to_file(results, args.output)
            else:
                content_extractor.save_results_to_file(results, args.output)
            logger.info(f"已将提取结果保存到文件: {args.output}")
        elif not args.output:
            logger.info("已将提取结果更新到数据库")

    finally:
        # 关闭资源
        if extract_table:
            table_extractor.close()
        if extract_content:
            content_extractor.close()
        logger.info("所有提取器资源已释放")

    end_time = time.time()
    logger.info(f"处理完成，耗时: {end_time - start_time:.2f} 秒")


if __name__ == "__main__":
    main()