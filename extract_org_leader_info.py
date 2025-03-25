#!/usr/bin/env python3
"""
extract_org_leader_info.py
用于从数据库中提取领导人的HTML内容并解析特定信息，更新到c_org_leader_info表
"""

import os
import sys
import time
import argparse
import json
from typing import Dict, List, Any, Optional

# 从项目根目录导入模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from html_extractor.extract_content_from_remark import BaiduBaikeExtractor
from html_extractor.extract_table_from_remark import DBExtractor, HTMLExtractor
from utils.logger import get_logger
from utils.file_utils import ensure_dir, safe_filename


class LeaderInfoExtractor:
    """从数据库中提取和解析领导人信息的类"""

    def __init__(self, output_dir: str = "./extracted_leaders"):
        """
        初始化提取器

        Args:
            output_dir: 输出目录
        """
        self.output_dir = ensure_dir(output_dir)
        self.table_dir = ensure_dir(os.path.join(output_dir, "tables"))
        self.content_dir = ensure_dir(os.path.join(output_dir, "contents"))

        # 获取日志器
        self.logger = get_logger(__name__)

        # 初始化数据库提取器
        self.db_extractor = DBExtractor()

        # 初始化内容和表格提取器
        self.content_extractor = BaiduBaikeExtractor('html_extractor/leader_content_schema.json')
        self.table_extractor = HTMLExtractor('html_extractor/leader_table_schema.json')

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
            if not self.db_extractor.connect():
                self.logger.error("数据库连接失败")
                return []

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

            self.db_extractor.cursor.execute(query, params)
            leaders = self.db_extractor.cursor.fetchall()
            self.logger.info(f"找到 {len(leaders)} 条领导人记录")
            return leaders
        except Exception as e:
            self.logger.error(f"获取领导人记录时出错: {str(e)}")
            return []

    def update_leader_info(self, leader_id: int, field_data: Dict[str, str]) -> bool:
        """
        更新领导人信息到数据库

        Args:
            leader_id: 领导人ID
            field_data: 字段数据

        Returns:
            是否成功更新
        """
        try:
            # 构建更新语句
            set_clauses = []
            params = []

            for field_name, field_value in field_data.items():
                if field_value:  # 只更新非空值
                    set_clauses.append(f"{field_name} = %s")
                    params.append(field_value)

            if not set_clauses:
                self.logger.warning(f"领导人 ID={leader_id} 没有需要更新的非空字段")
                return False

            # 添加更新时间
            set_clauses.append("update_time = NOW()")

            # 添加ID参数
            params.append(leader_id)

            # 构建并执行SQL
            query = f"""
            UPDATE c_org_leader_info
            SET {', '.join(set_clauses)}
            WHERE id = %s
            """

            self.db_extractor.cursor.execute(query, params)
            self.db_extractor.connection.commit()

            self.logger.info(f"成功更新领导人 ID={leader_id} 的信息，影响行数: {self.db_extractor.cursor.rowcount}")
            return True
        except Exception as e:
            self.logger.error(f"更新领导人信息时出错: {str(e)}")
            try:
                self.db_extractor.connection.rollback()
            except:
                pass
            return False

    def process_leader(self, leader: Dict, update_db: bool = True) -> Dict:
        """处理单个领导人信息并更新到数据库"""
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
        table_result = self.table_extractor.extract_info_from_html(html_content, self.table_extractor.field_mapping)

        # 特别处理 description(职务) 和 summary(简介)
        description = content_result.get("description", "")
        summary = content_result.get("summary", "")

        # 将提取的内容映射到字段
        field_data = {
            "leader_position": description,
            "current_position": description,  # 同样的内容写入两个字段
            "leader_profile": summary
        }

        # 处理内容section中的字段映射
        for section in content_result.get('sections', []):
            heading = section.get('heading', '')
            content = section.get('content', '')

            # 如果没有内容，则跳过
            if not content:
                continue

            # 遍历所有字段映射
            for field_name, match_headings in self.content_extractor.field_mapping.items():
                # 匹配标题
                if any(match_heading in heading for match_heading in match_headings):
                    field_data[field_name] = content
                    self.logger.info(f"字段{field_name}匹配到标题'{heading}'")
                    break

        # 合并表格提取的结果
        field_data.update(table_result)

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

        # 将数据更新到数据库
        if update_db:
            self.update_leader_info(leader_id, field_data)

        # 构建结果摘要
        result = {
            "id": leader_id,
            "name": leader_name,
            "title": content_result.get("title", ""),
            "description": description,
            "summary": summary,
            "table_fields": list(table_result.keys()),
            "section_count": len(content_result.get("sections", [])),
            "success": True,
            "updated_fields": list(field_data.keys())
        }

        return result

    def process_leaders(self, limit: Optional[int] = None, leader_id: Optional[int] = None, update_db: bool = True) -> \
    List[Dict]:
        """
        处理多个领导人信息

        Args:
            limit: 限制处理数量
            leader_id: 指定处理单个领导人ID
            update_db: 是否更新数据库

        Returns:
            处理结果列表
        """
        # 获取领导人列表
        leaders = self.get_leaders(limit, leader_id)

        if not leaders:
            self.logger.warning("没有找到领导人记录")
            return []

        # 处理每个领导人
        results = []
        for leader in leaders:
            try:
                result = self.process_leader(leader, update_db)
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
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        summary_file = os.path.join(self.output_dir, f"extraction_summary_{timestamp}.json")
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        self.logger.info(f"已保存总结果到 {summary_file}")

        # 关闭数据库连接
        self.db_extractor.disconnect()

        return results


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='从数据库提取领导人的HTML内容并解析')
    parser.add_argument('--limit', type=int, help='限制处理的领导人数量')
    parser.add_argument('--leader_id', type=int, help='指定领导人ID')
    parser.add_argument('--output_dir', default='./data/extracted_leaders', help='输出目录')
    parser.add_argument('--no_update_db', action='store_true', help='不更新数据库，仅生成提取结果')

    args = parser.parse_args()

    # 创建提取器并处理
    extractor = LeaderInfoExtractor(args.output_dir)
    results = extractor.process_leaders(args.limit, args.leader_id, not args.no_update_db)

    # 打印摘要
    success_count = sum(1 for r in results if r.get('success', False))
    print(f"\n提取完成! 总共处理了 {len(results)} 个领导人，成功: {success_count}，失败: {len(results) - success_count}")
    print(f"结果保存在目录: {args.output_dir}")


if __name__ == "__main__":
    main()