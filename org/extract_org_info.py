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
from html_extractor.extract_table_from_remark import DBExtractor, HTMLExtractor
from html_extractor.extract_content_from_remark import BaiduBaikeExtractor
from utils.logger import get_logger
logger = get_logger(__name__)


def extract_org_info():
    # 创建数据库提取器并连接数据库
    db_extractor = DBExtractor()
    if not db_extractor.connect():
        logger.error("无法连接到数据库，退出处理")
        return

    start_time = time.time()

    logger.info("开始表格信息提取...")
    table_extractor = HTMLExtractor()
    table_extractor.db_extractor = db_extractor

    table_results = table_extractor.process_all_organizations()
    logger.info(f"已完成所有组织的表格信息提取，共处理 {len(table_results)} 个组织")

    logger.info("开始内容信息提取...")
    content_extractor = BaiduBaikeExtractor()
    content_extractor.db_extractor = db_extractor

    content_results = content_extractor.process_all_organizations()
    logger.info(f"已完成所有组织的内容信息提取，共处理 {len(content_results)} 个组织")

    table_extractor.close()
    content_extractor.close()
    logger.info("所有提取器资源已释放")

    end_time = time.time()
    logger.info(f"处理完成，耗时: {end_time - start_time:.2f} 秒")
