"""
html_extractor.py
用于从数据库中提取HTML内容并解析特定信息
"""

import re
import json
from typing import Dict, List, Any
from bs4 import BeautifulSoup
import mysql.connector

from config.settings import Config

# 获取日志器
from utils.logger import get_logger
logger = get_logger(__name__)


class DBExtractor:
    """从数据库获取HTML并提取信息的类"""

    def __init__(self, db_config: Dict[str, str] = None):
        """
        初始化数据库提取器

        Args:
            db_config: 数据库配置信息，包含host、user、password、database
                      如果为None，则从配置文件中读取
        """
        if db_config is None:
            # 优先从配置文件中读取数据库设置
            try:
                config_path = './config.yaml'
                config = Config.from_file(config_path)
                self.db_config = config.db_config
                logger.info(f"从配置文件 {config_path} 加载数据库配置")
            except Exception as e:
                # 如果配置文件加载失败，使用默认配置
                logger.warning(f"加载配置文件失败: {str(e)}，使用默认配置")
                config = Config()
                self.db_config = config.db_config
        else:
            self.db_config = db_config

        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        """
        连接到数据库

        Returns:
            是否成功连接
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database']
            )
            self.cursor = self.connection.cursor(dictionary=True)
            logger.info(f"已连接到数据库: {self.db_config['database']}")
            return True
        except mysql.connector.Error as e:
            logger.error(f"连接数据库失败: {str(e)}")
            return False

    def disconnect(self) -> None:
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("已断开数据库连接")

    def get_html_by_org_id(self, org_id: int) -> str:
        """
        通过组织ID获取HTML内容

        Args:
            org_id: 组织ID

        Returns:
            HTML内容
        """
        try:
            query = """
            SELECT remark 
            FROM c_org_info 
            WHERE id = %s AND is_deleted = 0
            """
            self.cursor.execute(query, (org_id,))
            result = self.cursor.fetchone()

            if result and result.get('remark'):
                logger.info(f"成功获取组织ID={org_id}的HTML内容")
                return result['remark']
            else:
                logger.warning(f"未找到组织ID={org_id}的HTML内容或内容为空")
                return ""

        except mysql.connector.Error as e:
            logger.error(f"获取HTML内容时出错: {str(e)}")
            return ""

    def get_html_by_org_uuid(self, org_uuid: str) -> str:
        """
        通过组织UUID获取HTML内容

        Args:
            org_uuid: 组织UUID

        Returns:
            HTML内容
        """
        try:
            query = """
            SELECT remark 
            FROM c_org_info 
            WHERE uuid = %s AND is_deleted = 0
            """
            self.cursor.execute(query, (org_uuid,))
            result = self.cursor.fetchone()

            if result and result.get('remark'):
                logger.info(f"成功获取组织UUID={org_uuid}的HTML内容")
                return result['remark']
            else:
                logger.warning(f"未找到组织UUID={org_uuid}的HTML内容或内容为空")
                return ""

        except mysql.connector.Error as e:
            logger.error(f"获取HTML内容时出错: {str(e)}")
            return ""

    def get_html_by_org_name(self, org_name: str) -> str:
        """
        通过组织名称获取HTML内容

        Args:
            org_name: 组织名称

        Returns:
            HTML内容
        """
        try:
            query = """
            SELECT remark 
            FROM c_org_info 
            WHERE org_name = %s AND is_deleted = 0
            """
            self.cursor.execute(query, (org_name,))
            result = self.cursor.fetchone()

            if result and result.get('remark'):
                logger.info(f"成功获取组织'{org_name}'的HTML内容")
                return result['remark']
            else:
                logger.warning(f"未找到组织'{org_name}'的HTML内容或内容为空")
                return ""

        except mysql.connector.Error as e:
            logger.error(f"获取HTML内容时出错: {str(e)}")
            return ""

    def get_all_organizations(self) -> List[Dict]:
        """
        获取所有组织记录

        Returns:
            组织记录列表（不包含HTML内容）
        """
        try:
            query = """
            SELECT id, uuid, org_name
            FROM c_org_info 
            WHERE is_deleted = 0
            """
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            logger.error(f"获取组织记录时出错: {str(e)}")
            return []

    def update_extraction_result(self, org_id: int, field_name: str, field_value: str) -> bool:
        """
        更新提取结果到数据库

        Args:
            org_id: 组织ID
            field_name: 字段名
            field_value: 字段值

        Returns:
            是否成功更新
        """
        try:
            # 检查字段是否存在
            check_query = f"""
            SHOW COLUMNS FROM c_org_info LIKE %s
            """
            self.cursor.execute(check_query, (field_name,))
            if not self.cursor.fetchone():
                logger.warning(f"字段'{field_name}'不存在于数据库中")
                return True

            # 更新字段值
            update_query = f"""
            UPDATE c_org_info
            SET {field_name} = %s
            WHERE id = %s
            """
            self.cursor.execute(update_query, (field_value, org_id))
            self.connection.commit()

            logger.info(f"成功更新组织ID={org_id}的{field_name}")
            return True

        except mysql.connector.Error as e:
            logger.error(f"更新提取结果时出错: {str(e)}")
            return False


class HTMLExtractor:
    """HTML内容提取处理器"""

    def __init__(self, field_mapping_file: str = None):
        """
        初始化HTML提取器

        Args:
            field_mapping_file: 字段映射配置JSON文件的路径，如果为None则使用默认配置
        """
        self.db_extractor = None
        self.field_mapping = {}

        # 如果未提供映射文件，则使用默认映射文件
        if field_mapping_file is None:
            import os
            default_mapping_file = os.path.join(os.path.dirname(__file__), 'org_table_schema.json')
            self.load_field_mapping(default_mapping_file)
        else:
            self.load_field_mapping(field_mapping_file)

    def load_field_mapping(self, mapping_file: str) -> bool:
        """
        从JSON文件加载字段映射配置

        Args:
            mapping_file: 映射配置文件路径

        Returns:
            是否成功加载
        """
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                simple_mapping = json.load(f)

            # 生成完整的字段映射配置
            self.field_mapping = self._generate_field_mapping(simple_mapping)
            logger.info(f"成功加载字段映射配置: {mapping_file}")
            return True
        except Exception as e:
            logger.error(f"加载字段映射文件时出错: {str(e)}")
            return False

    def _generate_extraction_rules(self, match_text_list: List[str]) -> List[Dict[str, Any]]:
        """
        根据匹配文本列表生成三种提取规则

        Args:
            match_text_list: 匹配文本列表

        Returns:
            提取规则列表
        """
        cleaned_match_text = [re.sub(r'\s+', '', text) for text in match_text_list]
        return [
            # 规则1: 使用info-title和info-content类
            {
                "selector_type": "class",
                "selector": "info-title",
                "match_text": cleaned_match_text,
                "sibling_selector": {"type": "class", "value": "info-content"}
            },
            # 规则2: 使用dt和dd标签
            {
                "selector_type": "tag",
                "selector": "dt",
                "match_text": cleaned_match_text,
                "sibling_selector": {"type": "tag", "value": "dd"}
            },
            # 规则3: 使用basicInfo-item类
            {
                "selector_type": "class",
                "selector": "basicInfo-item",
                "match_text": cleaned_match_text,
                "sibling_selector": {"type": "class", "value": "basicInfo-item"}
            }
        ]

    def _generate_field_mapping(self, simple_mapping: Dict[str, List[str]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        将简化的映射转换为完整的字段映射配置

        Args:
            simple_mapping: 简化的映射，格式为 {"字段名": ["匹配文本1", "匹配文本2", ...]}

        Returns:
            完整的字段映射配置
        """
        field_mapping = {}
        for field_name, match_text_list in simple_mapping.items():
            field_mapping[field_name] = self._generate_extraction_rules(match_text_list)
        return field_mapping

    def _clean_text(self, text: str) -> str:
        """
        清理文本，去除HTML标签、多余空白、引用标记等

        Args:
            text: 要清理的文本

        Returns:
            清理后的文本
        """
        # 移除HTML标签
        text = re.sub(r'<[^>]+>', '', text)
        # 移除多余空白
        text = re.sub(r'\s+', ' ', text).strip()
        # 移除引用标记 [1] 等
        text = re.sub(r'\[\d+(-\d+)?\]', '', text).strip()
        return text

    def extract_info_from_html(self, html_content: str, field_mapping: Dict[str, List[Dict[str, Any]]]) -> Dict[str, str]:
        """
        从HTML内容中提取指定字段的信息

        Args:
            html_content: HTML内容字符串
            field_mapping: 字段映射配置

        Returns:
            包含提取信息的字典
        """
        if not html_content:
            return {field: "" for field in field_mapping.keys()}

        # 初始化结果字典
        result = {field: "" for field in field_mapping.keys()}

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # 遍历每个要提取的字段
            for field_name, extraction_rules in field_mapping.items():
                # 遍历该字段的所有提取规则
                for rule in extraction_rules:
                    # 如果已经找到该字段的值，则跳过后续规则
                    if result[field_name]:
                        break

                    selector_type = rule.get("selector_type", "")
                    selector = rule.get("selector", "")
                    match_text = rule.get("match_text", [])
                    sibling_selector = rule.get("sibling_selector", {})
                    is_direct_content = rule.get("is_direct_content", False)

                    # 根据选择器类型查找元素
                    elements = []
                    if selector_type == "class":
                        elements = soup.find_all(class_=selector)
                    elif selector_type == "id":
                        element = soup.find(id=selector)
                        elements = [element] if element else []
                    elif selector_type == "tag":
                        elements = soup.find_all(selector)
                    elif selector_type == "xpath":
                        # 简单模拟xpath查找，实际使用可能需要lxml库
                        selector_parts = selector.split('/')
                        if len(selector_parts) > 1:
                            current_elements = soup.find_all(selector_parts[1])
                            for part in selector_parts[2:]:
                                if not current_elements:
                                    break
                                next_elements = []
                                for elem in current_elements:
                                    next_elements.extend(elem.find_all(part))
                                current_elements = next_elements
                            elements = current_elements

                    # 处理找到的元素
                    for element in elements:
                        element_text = element.get_text().strip()
                        element_text = re.sub(r'\s+', '', element_text)

                        # 如果有匹配文本条件，检查当前元素文本是否包含其中之一
                        if match_text and not any(text in element_text for text in match_text):
                            continue

                        # 决定从哪里获取内容
                        content = ""
                        if is_direct_content:
                            # 直接从当前元素获取内容
                            content = element_text
                        elif sibling_selector:
                            # 从相邻元素获取内容
                            sibling_type = sibling_selector.get("type", "")
                            sibling_value = sibling_selector.get("value", "")

                            sibling = None
                            if sibling_type == "class":
                                sibling = element.find_next(class_=sibling_value)
                            elif sibling_type == "tag":
                                sibling = element.find_next(sibling_value)
                            elif sibling_type == "id":
                                sibling = element.find_next(id=sibling_value)

                            if sibling:
                                content = sibling.get_text().strip()

                        # 清理内容并更新结果
                        if content:
                            content = self._clean_text(content)
                            result[field_name] = content
                            break

            # 记录提取结果
            log_message = "提取到信息: " + ", ".join([f"{key}={value}" for key, value in result.items() if value])
            if not any(result.values()):
                log_message = "未能提取到任何信息"

            logger.info(log_message)

        except Exception as e:
            logger.error(f"提取信息时出错: {str(e)}")

        return result

    def process_organization(self, org_id: int, update_db: bool = False) -> Dict[str, str]:
        """
        处理单个组织的信息提取

        Args:
            org_id: 组织ID
            update_db: 是否更新提取结果到数据库

        Returns:
            提取的信息字典
        """
        if not self.db_extractor:
            self.db_extractor = DBExtractor()
            if not self.db_extractor.connect():
                logger.error("无法连接到数据库，无法处理组织信息")
                return {field: "" for field in self.field_mapping.keys()}

        # 获取HTML内容
        html_content = self.db_extractor.get_html_by_org_id(org_id)
        if not html_content:
            logger.warning(f"组织ID={org_id}没有HTML内容，跳过提取")
            return {field: "" for field in self.field_mapping.keys()}

        # 提取信息
        extraction_result = self.extract_info_from_html(html_content, self.field_mapping)

        # 如果需要，更新提取结果到数据库
        if update_db:
            for field_name, field_value in extraction_result.items():
                if field_value:  # 只更新非空值
                    self.db_extractor.update_extraction_result(org_id, field_name, field_value)

        return extraction_result

    def process_all_organizations(self, update_db: bool = False) -> List[Dict[str, Any]]:
        """
        处理所有组织的信息提取

        Args:
            update_db: 是否更新提取结果到数据库

        Returns:
            处理结果列表
        """
        results = []

        if not self.db_extractor:
            self.db_extractor = DBExtractor()
            if not self.db_extractor.connect():
                logger.error("无法连接到数据库，无法处理组织信息")
                return results

        # 获取所有组织
        organizations = self.db_extractor.get_all_organizations()
        logger.info(f"找到 {len(organizations)} 个组织")

        for org in organizations:
            org_id = org['id']
            org_name = org['org_name']
            logger.info(f"处理组织: {org_name} (ID: {org_id})")

            result = self.process_organization(org_id, update_db)
            results.append({
                "org_id": org_id,
                "org_name": org_name,
                "extraction_result": result
            })

        return results

    def save_results_to_file(self, results: List[Dict[str, Any]], output_file: str) -> bool:
        """
        将处理结果保存到文件

        Args:
            results: 处理结果列表
            output_file: 输出文件路径

        Returns:
            是否成功保存
        """
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            logger.info(f"结果已保存到文件: {output_file}")
            return True
        except Exception as e:
            logger.error(f"保存结果到文件时出错: {str(e)}")
            return False

    def close(self):
        """关闭连接和资源"""
        if self.db_extractor:
            self.db_extractor.disconnect()