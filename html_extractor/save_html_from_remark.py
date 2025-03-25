#!/usr/bin/env python3
"""
save_html_from_db.py
从数据库表的remark字段提取HTML内容并保存到本地文件，以便调试HTML解析问题
"""

import os
import sys
import pymysql
from datetime import datetime
from typing import Dict, List, Any, Optional

# 将项目根目录添加到模块搜索路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import get_logger
from utils.file_utils import ensure_dir, safe_filename

# 获取日志器
logger = get_logger(__name__)

# 数据库配置 - 请根据实际情况修改
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'wlh3338501',
    'database': 'cnfic_leader',
    'charset': 'utf8mb4'
}

# 固定配置
DB_TABLE = "c_org_leader_info"
REMARK_FIELD = "remark"
ID_FIELD = "id"
NAME_FIELD = "leader_name"
OUTPUT_DIR = "../data/person_data_pc"


class HtmlDumper:
    """从数据库提取HTML内容并保存到文件的类"""

    def __init__(self, output_dir: str = OUTPUT_DIR):
        """
        初始化HTML保存器

        Args:
            output_dir: 输出目录路径
        """
        self.output_dir = output_dir
        ensure_dir(self.output_dir)
        self.connection = None
        self.cursor = None

        logger.info(f"初始化HtmlDumper，输出目录: {self.output_dir}")

    def connect(self) -> bool:
        """
        连接到数据库

        Returns:
            bool: 是否成功连接
        """
        try:
            self.connection = pymysql.connect(**DB_CONFIG)
            self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)
            logger.info("成功连接到数据库")
            return True
        except Exception as e:
            logger.error(f"连接数据库失败: {str(e)}")
            return False

    def disconnect(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("关闭数据库连接")

    def dump_html_from_db(self, condition: str = None, limit: int = None) -> int:
        """
        从数据库表提取HTML内容并保存到文件

        Args:
            condition: SQL WHERE条件，例如 "id > 100"
            limit: 限制提取的记录数，例如 10

        Returns:
            int: 成功保存的记录数
        """
        if not self.connect():
            return 0

        try:
            # 构建查询
            query = f"SELECT {ID_FIELD}, {NAME_FIELD}, {REMARK_FIELD} FROM {DB_TABLE} WHERE {REMARK_FIELD} IS NOT NULL"
            if condition:
                query += f" AND {condition}"

            # 添加限制条件
            if limit:
                query += f" LIMIT {limit}"

            # 执行查询
            self.cursor.execute(query)
            records = self.cursor.fetchall()

            logger.info(f"查询到 {len(records)} 条记录")

            # 统计信息
            saved_count = 0

            # 保存每条记录的HTML内容
            for record in records:
                try:
                    record_id = record[ID_FIELD]
                    record_name = record[NAME_FIELD]
                    html_content = record[REMARK_FIELD]

                    # 跳过空内容
                    if not html_content:
                        logger.warning(f"记录 {record_name}(ID={record_id}) HTML内容为空，跳过")
                        continue

                    # 生成安全的文件名
                    safe_name = safe_filename(str(record_name))
                    timestamp = datetime.now().strftime("%Y%m%d")
                    filename = f"{safe_name}_{record_id}_{timestamp}.html"
                    file_path = os.path.join(self.output_dir, filename)

                    # 保存HTML内容
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(html_content)

                    logger.info(f"保存记录 {record_name}(ID={record_id}) 到文件: {filename}")
                    saved_count += 1

                except Exception as e:
                    logger.error(f"保存记录 {record.get(ID_FIELD, 'unknown')} HTML内容时出错: {str(e)}")

            return saved_count

        except Exception as e:
            logger.error(f"从数据库提取HTML内容时出错: {str(e)}")
            return 0

        finally:
            self.disconnect()

    def dump_html_by_id(self, record_id: int) -> bool:
        """
        保存指定ID的记录HTML内容

        Args:
            record_id: 记录ID

        Returns:
            bool: 是否成功保存
        """
        if not self.connect():
            return False

        try:
            # 查询特定记录
            query = f"SELECT {ID_FIELD}, {NAME_FIELD}, {REMARK_FIELD} FROM {DB_TABLE} WHERE {ID_FIELD} = %s"
            self.cursor.execute(query, (record_id,))
            record = self.cursor.fetchone()

            if not record:
                logger.warning(f"未找到ID为 {record_id} 的记录")
                return False

            record_name = record[NAME_FIELD]
            html_content = record[REMARK_FIELD]

            # 检查HTML内容
            if not html_content:
                logger.warning(f"记录 {record_name}(ID={record_id}) HTML内容为空")
                return False

            # 生成文件名和路径
            safe_name = safe_filename(str(record_name))
            timestamp = datetime.now().strftime("%Y%m%d")
            filename = f"{safe_name}_{record_id}_{timestamp}.html"
            file_path = os.path.join(self.output_dir, filename)

            # 保存HTML内容
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

            logger.info(f"保存记录 {record_name}(ID={record_id}) 到文件: {filename}")

            # 保存额外的调试信息文件
            debug_path = os.path.join(self.output_dir, f"{safe_name}_{record_id}_{timestamp}_debug.txt")
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(f"记录ID: {record_id}\n")
                f.write(f"记录名称: {record_name}\n")
                f.write(f"HTML内容长度: {len(html_content)} 字符\n")
                f.write(f"HTML内容前500字符: {html_content[:500]}\n")
                f.write("=" * 50 + "\n")
                f.write("HTML标签分析:\n")

                # 简单分析HTML结构
                import re
                tags = re.findall(r'<([a-zA-Z0-9]+)[^>]*>', html_content)
                tag_count = {}
                for tag in tags:
                    tag_count[tag] = tag_count.get(tag, 0) + 1

                for tag, count in sorted(tag_count.items(), key=lambda x: x[1], reverse=True):
                    f.write(f"  {tag}: {count} 个\n")

            logger.info(f"保存调试信息到文件: {os.path.basename(debug_path)}")
            return True

        except Exception as e:
            logger.error(f"保存记录 {record_id} HTML内容时出错: {str(e)}")
            return False

        finally:
            self.disconnect()


def main():
    """主函数"""
    import argparse

    # 命令行参数
    parser = argparse.ArgumentParser(description='从数据库提取HTML内容并保存到文件')
    parser.add_argument('--id', type=int, help='指定记录ID')
    parser.add_argument('--limit', type=int, default=10, help='限制提取的记录数，默认为10')
    parser.add_argument('--condition', help='SQL WHERE条件')
    parser.add_argument('--output-dir', help='输出目录路径')

    args = parser.parse_args()

    # 初始化HTML提取器
    output_dir = args.output_dir if args.output_dir else OUTPUT_DIR
    dumper = HtmlDumper(output_dir)

    # 根据参数执行相应操作
    if args.id:
        # 提取特定ID的记录
        success = dumper.dump_html_by_id(args.id)
        if success:
            print(f"成功保存ID为 {args.id} 的记录HTML内容")
        else:
            print(f"保存ID为 {args.id} 的记录HTML内容失败")
    else:
        # 批量提取记录
        saved_count = dumper.dump_html_from_db(args.condition, args.limit)
        print(f"共保存 {saved_count} 条记录的HTML内容")


if __name__ == "__main__":
    main()