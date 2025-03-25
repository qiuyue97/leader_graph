#!/usr/bin/env python3
"""
extract_org_leader_info.py
用于从数据库中提取领导人的HTML内容并解析特定信息，保存到本地文件
"""

import argparse
import json
import os
import sys
import time
import datetime
from typing import Dict, List, Any, Optional

# 将项目根目录添加到模块搜索路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入现有的提取器
from html_extractor.extract_content_from_remark import BaiduBaikeExtractor
from html_extractor.extract_table_from_remark import HTMLExtractor
from utils.logger import get_logger
from utils.file_utils import ensure_dir, safe_filename


class LeaderInfoExtractor:
    """从数据库中提取和解析领导人信息的类"""

    def __init__(self, db_config: Dict[str, str], output_dir: str = "./extracted_leaders"):
        """
        初始化提取器

        Args:
            db_config: 数据库配置
            output_dir: 输出目录
        """
        self.db_config = db_config
        self.output_dir = ensure_dir(output_dir)
        self.table_dir = ensure_dir(os.path.join(output_dir, "tables"))
        self.content_dir = ensure_dir(os.path.join(output_dir, "contents"))
        self.connection = None
        self.cursor = None

        # 获取日志器
        self.logger = get_logger(__name__)

        # 初始化提取器
        self.content_extractor = BaiduBaikeExtractor()
        self.table_extractor = HTMLExtractor()

    def connect_db(self) -> bool:
        """连接到数据库"""
        try:
            import pymysql
            self.connection = pymysql.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.connection.cursor()
            self.logger.info(f"成功连接到数据库: {self.db_config['database']}")
            return True
        except Exception as e:
            self.logger.error(f"连接数据库失败: {str(e)}")
            return False

    def disconnect_db(self):
        """断开数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.info("数据库连接已关闭")

    def get_leaders(self, limit: Optional[int] = None, leader_id: Optional[int] = None) -> List[Dict]:
        """
        获取领导人列表

        Args:
            limit: 限制结果数量
            leader_id: 指定领导人ID，如果提供则仅获取该ID的领导人

        Returns:
            领导人列表
        """
        try:
            query = """
            SELECT id, uuid, leader_name, source_url, remark 
            FROM c_org_leader_info 
            WHERE is_deleted = 0 AND remark IS NOT NULL AND remark != ''
            """

            params = []

            if leader_id is not None:
                query += " AND id = %s"
                params.append(leader_id)

            if limit is not None:
                query += " LIMIT %s"
                params.append(limit)

            self.cursor.execute(query, params)
            leaders = self.cursor.fetchall()
            self.logger.info(f"找到 {len(leaders)} 条领导人记录")
            return leaders
        except Exception as e:
            self.logger.error(f"获取领导人记录时出错: {str(e)}")
            return []

    def process_leader(self, leader: Dict) -> Dict:
        """处理单个领导人信息"""
        leader_id = leader['id']
        leader_name = leader['leader_name']
        html_content = leader.get('remark', '')

        self.logger.info(f"处理领导人: {leader_name} (ID: {leader_id})")

        if not html_content:
            self.logger.warning(f"领导人 {leader_name} (ID: {leader_id}) 没有HTML内容")
            return {
                "id": leader_id,
                "name": leader_name,
                "success": False,
                "error": "没有HTML内容"
            }

        # 使用BaiduBaikeExtractor提取内容结构
        content_result = self.content_extractor.extract_from_html(html_content)

        # 使用HTMLExtractor提取表格信息
        # 从JSON文件加载字段映射配置
        leader_mapping_file = 'html_extractor/leader_table_schema.json'
        table_extractor = HTMLExtractor(leader_mapping_file)
        table_result = table_extractor.extract_info_from_html(html_content, table_extractor.field_mapping)

        # 保存表格数据
        tables_file = os.path.join(self.table_dir, f"{safe_filename(leader_name)}_{leader_id}_tables.json")
        with open(tables_file, 'w', encoding='utf-8') as f:
            json.dump(table_result, f, ensure_ascii=False, indent=2)
        self.logger.info(f"已保存表格数据到 {tables_file}")

        # 保存内容数据
        content_file = os.path.join(self.content_dir, f"{safe_filename(leader_name)}_{leader_id}_content.json")
        with open(content_file, 'w', encoding='utf-8') as f:
            json.dump(content_result, f, ensure_ascii=False, indent=2)
        self.logger.info(f"已保存内容数据到 {content_file}")

        # 保存原始HTML以便检查
        html_dir = ensure_dir(os.path.join(self.output_dir, "html"))
        html_file = os.path.join(html_dir, f"{safe_filename(leader_name)}_{leader_id}.html")
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        self.logger.info(f"已保存原始HTML到 {html_file}")

        # 构建结果摘要
        result = {
            "id": leader_id,
            "name": leader_name,
            "title": content_result.get("title", ""),
            "description": "",  # 需要添加描述字段提取
            "table_fields": list(table_result.keys()),
            "section_count": len(content_result.get("sections", [])),
            "success": True
        }

        # 检查是否有description字段
        if hasattr(self.content_extractor, '_extract_description'):
            # 如果BaiduBaikeExtractor有_extract_description方法
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            description = self.content_extractor._extract_description(soup)
            result["description"] = description

        return result

    def process_leaders(self, limit: Optional[int] = None, leader_id: Optional[int] = None) -> List[Dict]:
        """
        处理多个领导人信息

        Args:
            limit: 限制处理数量
            leader_id: 指定处理单个领导人ID

        Returns:
            处理结果列表
        """
        if not self.connect_db():
            return []

        try:
            # 获取领导人列表
            leaders = self.get_leaders(limit, leader_id)

            if not leaders:
                self.logger.warning("没有找到领导人记录")
                return []

            # 处理每个领导人
            results = []
            for leader in leaders:
                try:
                    result = self.process_leader(leader)
                    results.append(result)
                except Exception as e:
                    self.logger.error(
                        f"处理领导人 {leader.get('leader_name', '')} (ID: {leader.get('id', '')}) 时出错: {str(e)}")
                    import traceback
                    self.logger.error(traceback.format_exc())

                    # 添加错误结果
                    results.append({
                        "id": leader.get('id', ''),
                        "name": leader.get('leader_name', ''),
                        "success": False,
                        "error": str(e)
                    })

                # 等待一小段时间，避免过度消耗资源
                time.sleep(0.1)

            # 保存总结果
            summary_file = os.path.join(self.output_dir, "extraction_summary.json")
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            self.logger.info(f"已保存总结果到 {summary_file}")

            return results

        finally:
            self.disconnect_db()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='从数据库提取领导人的HTML内容并解析')
    parser.add_argument('--host', default='localhost', help='MySQL主机地址')
    parser.add_argument('--user', default='root', help='MySQL用户名')
    parser.add_argument('--password', default='wlh3338501', help='MySQL密码')
    parser.add_argument('--database', default='cnfic_leader', help='MySQL数据库名')
    parser.add_argument('--limit', type=int, help='限制处理的领导人数量')
    parser.add_argument('--leader_id', type=int, help='指定领导人ID')
    parser.add_argument('--output_dir', default='./data/extracted_leaders', help='输出目录')

    args = parser.parse_args()

    # 数据库配置
    db_config = {
        'host': args.host,
        'user': args.user,
        'password': args.password,
        'database': args.database
    }

    # 创建提取器并处理
    extractor = LeaderInfoExtractor(db_config, args.output_dir)
    results = extractor.process_leaders(args.limit, args.leader_id)

    # 打印摘要
    success_count = sum(1 for r in results if r.get('success', False))
    print(f"\n提取完成! 总共处理了 {len(results)} 个领导人，成功: {success_count}，失败: {len(results) - success_count}")
    print(f"结果保存在目录: {args.output_dir}")


if __name__ == "__main__":
    main()