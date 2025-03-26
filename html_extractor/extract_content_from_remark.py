#!/usr/bin/env python3
"""
extract_content_from_remark.py
用于从HTML内容中提取百度百科结构化内容
"""

import re
import os
import json
import logging
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional, Union, Tuple

# 尝试导入logger，如果不存在则使用内置logging
try:
    from utils.logger import get_logger

    logger = get_logger(__name__)
except ImportError:
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

# 尝试导入DBExtractor
try:
    from html_extractor.extract_table_from_remark import DBExtractor
except ImportError:
    try:
        from extract_table_from_remark import DBExtractor
    except ImportError:
        import mysql.connector
        from typing import Dict, List

        logger.warning("无法导入DBExtractor，使用内置实现")


        class DBExtractor:
            """从数据库获取HTML并提取信息的类的简化版本"""

            def __init__(self, db_config: Dict[str, str] = None):
                """初始化数据库提取器"""
                self.db_config = db_config or {
                    'host': 'localhost',
                    'user': 'root',
                    'password': '',
                    'database': 'default_db'
                }
                self.connection = None
                self.cursor = None

            def connect(self) -> bool:
                """连接到数据库"""
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
                """通过组织ID获取HTML内容"""
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

            def get_org_name_by_id(self, org_id: int) -> str:
                """通过组织ID获取组织名称"""
                try:
                    query = """
                    SELECT org_name 
                    FROM c_org_info 
                    WHERE id = %s AND is_deleted = 0
                    """
                    self.cursor.execute(query, (org_id,))
                    result = self.cursor.fetchone()

                    if result and result.get('org_name'):
                        return result['org_name']
                    else:
                        return f"未知组织(ID={org_id})"

                except mysql.connector.Error as e:
                    logger.error(f"获取组织名称时出错: {str(e)}")
                    return f"未知组织(ID={org_id})"

            def get_all_organizations(self) -> List[Dict]:
                """获取所有组织记录"""
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
                """更新提取结果到数据库"""
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


class BaiduBaikeExtractor:
    """从百度百科HTML内容提取结构化数据的类"""

    def __init__(self, field_mapping_file: str = None):
        """
        初始化提取器

        Args:
            field_mapping_file: 字段映射配置JSON文件的路径，如果为None则使用默认配置
        """
        self.db_extractor = None
        self.field_mapping = {}

        # 如果未提供映射文件，则使用默认映射文件
        if field_mapping_file is None:
            default_mapping_file = os.path.join(os.path.dirname(__file__), 'org_content_schema.json')
            if not os.path.exists(default_mapping_file):
                # 如果默认映射文件不存在，则创建它
                default_mapping = {
                    "province_profile": ["省情概况"],
                    "org_duty": ["主要职责"],
                    "internal_dept": ["内设机构", "机构设置"],
                    "org_history": ["历史沿革"],
                    "org_coop": ["战略合作"],
                    "org_staff": ["人员编制"],
                    "org_honor": ["获得荣誉"],
                    "office_addr": ["交通位置", "地理位置"]
                }
                with open(default_mapping_file, 'w', encoding='utf-8') as f:
                    json.dump(default_mapping, f, ensure_ascii=False, indent=2)
                logger.info(f"已创建默认字段映射文件: {default_mapping_file}")

            self.load_field_mapping(default_mapping_file)
        else:
            self.load_field_mapping(field_mapping_file)

    def load_field_mapping(self, mapping_file: str) -> bool:
        """从JSON文件加载字段映射配置"""
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                self.field_mapping = json.load(f)
            logger.info(f"成功加载字段映射配置: {mapping_file}")
            return True
        except Exception as e:
            logger.error(f"加载字段映射文件时出错: {str(e)}")
            return False

    def extract_from_html(self, html_content):
        """从HTML内容中提取标题、描述、简介和内容结构"""
        if not html_content:
            return {"title": "", "description": "", "summary": "", "sections": []}

        # 使用BeautifulSoup解析HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # 提取主标题
        main_title = self._extract_main_title(soup)

        # 提取描述信息
        description = self._extract_description(soup)

        # 提取简介内容
        summary = self._extract_summary(soup)

        # 提取所有章节内容
        sections = self._extract_content_structure(soup)

        # 如果没有找到任何章节，尝试通过其他方式提取内容
        if not sections:
            sections = self._extract_fallback_content(soup)

        return {
            "title": main_title,
            "description": description,
            "summary": summary,
            "sections": sections
        }

    def _extract_main_title(self, soup):
        """提取页面主标题"""
        # 先尝试从title标签获取
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.text.strip()
            # 百度百科标题通常格式为"主标题_百度百科"
            if "_百度百科" in title_text:
                return self._clean_text(title_text.split("_百度百科")[0])
            return self._clean_text(title_text)

        # 尝试从h1标签获取
        h1_tag = soup.find('h1')
        if h1_tag:
            return self._clean_text(h1_tag.text)

        # 尝试从lemma-title类获取
        lemma_title = soup.select_one('.lemma-title')
        if lemma_title:
            return self._clean_text(lemma_title.text)

        return ""

    def _extract_summary(self, soup):
        """提取百科页面的简介内容"""
        summary_text = ""
        # 查找具有特定类名的简介div
        summary_div = soup.select_one('div.lemmaSummary_s9vD3.J-summary')

        if summary_div:
            # 提取所有text_tjAKh类的span元素
            paragraphs = []

            # 遍历所有段落
            para_divs = summary_div.find_all('div', class_='para_WzwJ3')
            for para_div in para_divs:
                # 收集段落中所有文本元素
                para_texts = []

                # 处理所有类型为text_tjAKh的span
                for span in para_div.find_all('span', class_='text_tjAKh'):
                    # 清理文本
                    span_text = self._clean_text(span.text)
                    if span_text:
                        para_texts.append(span_text)

                # 提取和处理链接文本
                for link in para_div.find_all('a', class_='innerLink_qJN0J'):
                    link_text = self._clean_text(link.text)
                    if link_text and link_text not in " ".join(para_texts):
                        para_texts.append(link_text)

                # 组合段落文本
                if para_texts:
                    paragraphs.append(" ".join(para_texts))

            # 如果使用上面的方法没有找到文本，则尝试直接提取div的文本
            if not paragraphs:
                for para_div in para_divs:
                    para_text = self._clean_text(para_div.text)
                    if para_text:
                        paragraphs.append(para_text)

            # 合并所有段落
            if paragraphs:
                summary_text = "\n\n".join(paragraphs)

        return summary_text

    def _extract_description(self, soup):
        """提取百科页面的描述信息"""
        description_text = ""

        # 尝试查找lemmaDescText_BItKh类的元素
        desc_element = soup.find('div', class_='lemmaDescText_BItKh')
        if desc_element:
            description_text = self._clean_text(desc_element.text)

        # 如果没有找到，尝试查找lemmaDesc类
        if not description_text:
            desc_element = soup.find('div', class_='lemmaDesc')
            if desc_element:
                description_text = self._clean_text(desc_element.text)

        # 再尝试查找lemmadesc-nt1jK类
        if not description_text:
            desc_element = soup.find('div', class_='lemmaDesc_nt1jK')
            if desc_element:
                description_text = self._clean_text(desc_element.text)

        return description_text

    def _extract_content_structure(self, content_div):
        """提取内容结构，包括标题和段落"""
        sections = []

        # 首先尝试提取所有带name属性的h2标签（这是最直接的标题标识）
        h2_elements = content_div.find_all('h2', attrs={'name': True})
        if h2_elements:
            # 按照文档顺序排序h2标签
            h2_elements.sort(key=lambda x: x.sourceline if hasattr(x, 'sourceline') else 0)

            # 遍历每个h2标签，提取标题和内容
            for i, h2 in enumerate(h2_elements):
                # 提取标题
                heading = self._clean_text(h2.text)
                if not heading:
                    continue

                # 确定内容范围
                next_h2 = None if i >= len(h2_elements) - 1 else h2_elements[i + 1]

                # 提取段落内容
                content = self._extract_h2_content(h2, next_h2, content_div)

                if content:
                    sections.append({
                        "heading": heading,
                        "content": content
                    })

            return sections

        # 如果没有找到带name属性的h2标签，尝试通过paraTitle_c7Isv类提取标题和内容
        para_titles = content_div.select('div.paraTitle_c7Isv[data-level="1"]')
        if not para_titles:
            para_titles = content_div.select('div.paraTitle_c7Isv.level-1_gngtl')

        if para_titles:
            # 按照文档顺序排序
            para_titles.sort(key=lambda x: x.sourceline if hasattr(x, 'sourceline') else 0)

            # 遍历每个标题div，提取标题和内容
            for i, title_div in enumerate(para_titles):
                # 提取标题
                h2_tag = title_div.find('h2')
                heading = self._clean_text(h2_tag.text) if h2_tag else self._clean_text(title_div.text)
                if not heading:
                    continue

                # 确定内容范围
                next_title = None if i >= len(para_titles) - 1 else para_titles[i + 1]

                # 提取段落内容
                content = self._extract_div_content(title_div, next_title, content_div)

                if content:
                    sections.append({
                        "heading": heading,
                        "content": content
                    })

            return sections

        # 查找数据标签为header的元素作为标题
        header_divs = content_div.select('div[data-tag="header"]')
        if header_divs:
            for header_div in header_divs:
                h2_tag = header_div.find('h2')
                if not h2_tag:
                    continue

                heading = self._clean_text(h2_tag.text)
                if not heading:
                    continue

                # 查找紧跟在标题后的表格
                table_module = header_div.find_next_sibling('div', {'data-module-type': 'table'})
                if table_module:
                    content = self._extract_table(table_module)
                    if content:
                        sections.append({
                            "heading": heading,
                            "content": content
                        })

        return sections

    def _extract_h2_content(self, h2_element, next_h2, content_div):
        """提取h2标签之后、下一个h2标签之前的内容"""
        paragraphs = []

        # 获取h2标签的父元素
        parent = h2_element.parent
        if parent and parent.name == 'div' and 'paraTitle_c7Isv' in parent.get('class', []):
            # h2在paraTitle_c7Isv中，处理后续的兄弟元素
            current_element = parent.next_sibling

            # 收集内容直到下一个标题或空白
            while current_element:
                # 检查是否到达下一个标题的父元素
                if next_h2:
                    next_parent = next_h2.parent
                    if next_parent and current_element == next_parent:
                        break

                # 检查是否为段落内容
                if current_element.name == 'div':
                    # 检查是否是段落
                    if current_element.get('class') and (
                            'para_WzwJ3' in current_element.get('class') or 'content_XwoLS' in current_element.get(
                            'class')):
                        para_text = self._clean_text(current_element.text)
                        if para_text:
                            paragraphs.append(para_text)

                    # 检查是否是表格模块
                    elif current_element.get('data-module-type') == 'table':
                        table_text = self._extract_table(current_element)
                        if table_text:
                            paragraphs.append(table_text)

                    # 检查是否包含表格
                    elif current_element.find('table'):
                        table_text = self._extract_table(current_element)
                        if table_text:
                            paragraphs.append(table_text)

                    # 检查是否是二级标题
                    elif current_element.get('class') and 'paraTitle_c7Isv' in current_element.get('class') and (
                            'level-2' in ' '.join(current_element.get('class')) or current_element.get(
                        'data-level') == '2'):
                        # 提取二级标题文本
                        sub_h2 = current_element.find('h2')
                        subheading = self._clean_text(sub_h2.text) if sub_h2 else self._clean_text(current_element.text)

                        # 添加子标题作为段落的一部分
                        if subheading:
                            paragraphs.append(subheading)

                current_element = current_element.next_sibling
        else:
            # 直接的h2标签，使用sourceline查找后续内容
            h2_pos = h2_element.sourceline if hasattr(h2_element, 'sourceline') else 0
            next_pos = next_h2.sourceline if next_h2 and hasattr(next_h2, 'sourceline') else float('inf')

            # 查找所有可能的段落和标题元素
            all_elements = []

            # 收集所有内容元素并按sourceline排序
            for elem in content_div.find_all(['div', 'p', 'h2', 'h3']):
                elem_pos = elem.sourceline if hasattr(elem, 'sourceline') else 0
                if h2_pos < elem_pos < next_pos:
                    all_elements.append((elem_pos, elem))

            # 按位置排序
            all_elements.sort(key=lambda x: x[0])

            # 处理每个元素
            for pos, elem in all_elements:
                # 检查是否为段落
                if elem.name == 'div':
                    if elem.get('class') and (
                            'para_WzwJ3' in elem.get('class') or 'content_XwoLS' in elem.get('class')):
                        para_text = self._clean_text(elem.text)
                        if para_text:
                            paragraphs.append(para_text)

                    # 检查是否为表格模块
                    elif elem.get('data-module-type') == 'table':
                        table_text = self._extract_table(elem)
                        if table_text:
                            paragraphs.append(table_text)

                    # 检查是否包含表格
                    elif elem.find('table'):
                        table_text = self._extract_table(elem)
                        if table_text:
                            paragraphs.append(table_text)

                    # 检查是否为二级标题
                    elif elem.get('class') and 'paraTitle_c7Isv' in elem.get('class') and (
                            'level-2' in ' '.join(elem.get('class')) or elem.get('data-level') == '2'):
                        sub_h2 = elem.find('h2')
                        subheading = self._clean_text(sub_h2.text) if sub_h2 else self._clean_text(elem.text)
                        if subheading:
                            paragraphs.append(subheading)

                # 检查是否为二级标题(h2/h3)
                elif elem.name in ['h2', 'h3'] and elem != next_h2 and elem.get('name') and elem.get('name').startswith(
                        h2_element.get('name') + '-'):
                    subheading = self._clean_text(elem.text)
                    if subheading:
                        paragraphs.append(subheading)

                # 检查是否为段落(p)
                elif elem.name == 'p':
                    para_text = self._clean_text(elem.text)
                    if para_text:
                        paragraphs.append(para_text)

        return "\n\n".join(paragraphs)

    def _extract_div_content(self, title_div, next_title, content_div):
        """提取标题div之后、下一个标题div之前的内容"""
        paragraphs = []

        # 获取标题div的数据索引
        current_index = title_div.get('data-index')
        next_index = next_title.get('data-index') if next_title else None

        # 从标题div开始向后查找
        current_element = title_div.next_sibling

        # 收集内容直到下一个标题
        while current_element:
            # 检查是否到达下一个标题
            if next_title and (current_element == next_title or current_element.find(next_title)):
                break

            # 检查是否为段落div
            if current_element.name == 'div':
                if current_element.get('class') and (
                        'para_WzwJ3' in current_element.get('class') or 'content_XwoLS' in current_element.get(
                        'class')):
                    para_text = self._clean_text(current_element.text)
                    if para_text:
                        paragraphs.append(para_text)

                # 检查是否为表格模块
                elif current_element.get('data-module-type') == 'table':
                    table_text = self._extract_table(current_element)
                    if table_text:
                        paragraphs.append(table_text)

                # 检查是否包含表格
                elif current_element.find('table'):
                    table_text = self._extract_table(current_element)
                    if table_text:
                        paragraphs.append(table_text)

                # 检查是否为二级标题
                elif current_element.get('class') and 'paraTitle_c7Isv' in current_element.get('class') and (
                        'level-2' in ' '.join(current_element.get('class')) or current_element.get(
                    'data-level') == '2'):
                    # 提取二级标题文本
                    h2_tag = current_element.find('h2')
                    subheading = self._clean_text(h2_tag.text) if h2_tag else self._clean_text(current_element.text)

                    # 添加子标题作为段落的一部分
                    if subheading:
                        paragraphs.append(subheading)

                    # 获取子标题索引
                    sub_index = current_element.get('data-index')

                    # 找到这个子标题后的内容
                    sub_element = current_element.next_sibling

                    while sub_element:
                        # 检查是否到达下一个标题
                        if next_title and (sub_element == next_title or sub_element.find(next_title)):
                            break

                        # 检查是否为下一个二级标题
                        if sub_element.name == 'div' and sub_element.get(
                                'class') and 'paraTitle_c7Isv' in sub_element.get('class', []) and (
                                'level-2' in ' '.join(sub_element.get('class', [])) or sub_element.get(
                            'data-level') == '2'):
                            break

                        # 提取段落内容
                        if sub_element.name == 'div' and sub_element.get('class') and (
                                'para_WzwJ3' in sub_element.get('class', []) or 'content_XwoLS' in sub_element.get(
                            'class', [])):
                            sub_text = self._clean_text(sub_element.text)
                            if sub_text:
                                paragraphs.append(sub_text)

                        # 提取表格内容
                        elif sub_element.name == 'div':
                            if sub_element.get('data-module-type') == 'table':
                                table_text = self._extract_table(sub_element)
                                if table_text:
                                    paragraphs.append(table_text)
                            elif sub_element.find('table'):
                                table_text = self._extract_table(sub_element)
                                if table_text:
                                    paragraphs.append(table_text)

                        sub_element = sub_element.next_sibling

            current_element = current_element.next_sibling

        # 如果没有找到内容，尝试通过data-tag和data-index属性查找
        if not paragraphs and current_index:
            # 查找所有与当前索引相关的段落
            for para in content_div.find_all(attrs={'data-tag': 'paragraph'}):
                para_idx = para.get('data-idx', '')
                if para_idx and para_idx.startswith(current_index.split('-')[0]):
                    # 确保不是下一个标题下的内容
                    if not next_index or not para_idx.startswith(next_index.split('-')[0]):
                        para_text = self._clean_text(para.text)
                        if para_text:
                            paragraphs.append(para_text)

            # 查找与当前索引相关的表格
            for table_div in content_div.find_all(attrs={'data-tag': 'module', 'data-module-type': 'table'}):
                table_idx = table_div.get('data-idx', '')
                if table_idx and table_idx.startswith(current_index.split('-')[0]):
                    # 确保不是下一个标题下的内容
                    if not next_index or not table_idx.startswith(next_index.split('-')[0]):
                        table_text = self._extract_table(table_div)
                        if table_text:
                            paragraphs.append(table_text)

        return "\n\n".join(paragraphs)

    def _extract_table(self, element):
        """提取表格内容"""
        table_texts = []

        # 检查元素本身是否为表格
        if element.name == 'table':
            tables = [element]
        else:
            # 查找所有表格
            tables = element.find_all('table')

        # 如果没有找到表格，检查特殊的模块
        if not tables and element.get('data-module-type') == 'table':
            module_tables = element.select('div.moduleTable_ieSLV table, table.tableBox_kVyoj')
            if module_tables:
                tables = module_tables

        # 处理每个表格
        for table in tables:
            rows = []

            # 提取表格标题
            caption = table.find('caption')
            if caption:
                caption_text = self._clean_text(caption.text)
                if caption_text:
                    rows.append(f"表格标题: {caption_text}")

            # 提取表头和数据行
            for tr in table.find_all('tr'):
                # 获取所有单元格
                cells = tr.find_all(['td', 'th'])
                if not cells:
                    continue

                row_texts = []
                for cell in cells:
                    # 处理单元格内容
                    cell_content = ""

                    # 检查是否有带类的内容
                    text_spans = cell.select('span.text_tjAKh')
                    if text_spans:
                        span_texts = []
                        for span in text_spans:
                            span_text = self._clean_text(span.text)
                            if span_text:
                                span_texts.append(span_text)
                        cell_content = " ".join(span_texts)
                    else:
                        # 检查其他可能的内容元素
                        para_elements = cell.select('div.para_WzwJ3, div.table_FIFZE')
                        if para_elements:
                            para_texts = []
                            for para in para_elements:
                                para_text = self._clean_text(para.text)
                                if para_text:
                                    para_texts.append(para_text)
                            cell_content = " ".join(para_texts)
                        else:
                            # 如果没有特定元素，获取整个单元格的文本
                            cell_content = self._clean_text(cell.text)

                    # 处理链接
                    links = cell.find_all('a')
                    for link in links:
                        link_text = self._clean_text(link.text)
                        if link_text and link_text not in cell_content:
                            cell_content += f" [{link_text}]"

                    row_texts.append(cell_content)

                # 只添加非空行
                if row_texts and any(text.strip() for text in row_texts):
                    rows.append(" | ".join(row_texts))

            if rows:
                table_texts.append("\n".join(rows))

        return "\n\n".join(table_texts)

    def _extract_fallback_content(self, soup):
        """当无法提取结构化内容时的备选方法"""
        sections = []

        # 尝试查找独立的表格模块
        table_modules = soup.find_all(attrs={'data-module-type': 'table'})
        for table_module in table_modules:
            # 查找前面的标题
            header_div = table_module.find_previous('div', {'data-tag': 'header'})
            heading = ""
            if header_div:
                h2_tag = header_div.find('h2')
                if h2_tag:
                    heading = self._clean_text(h2_tag.text)

            if not heading:
                heading = "表格内容"

            # 提取表格内容
            content = self._extract_table(table_module)
            if content:
                sections.append({
                    "heading": heading,
                    "content": content
                })

        # 如果没有找到表格，尝试直接提取所有段落
        if not sections:
            paragraphs = []

            # 查找所有可能的段落元素
            para_elements = soup.select('div.para_WzwJ3, div.para, div.content_XwoLS, p')
            for para in para_elements:
                # 跳过包含标题的元素
                if para.find(['h1', 'h2', 'h3']):
                    continue

                para_text = self._clean_text(para.text)
                if para_text and len(para_text) > 5:
                    paragraphs.append(para_text)

            # 如果找到段落，返回一个默认的章节
            if paragraphs:
                sections.append({
                    "heading": "正文内容",
                    "content": "\n\n".join(paragraphs)
                })

        return sections

    def _clean_text(self, text):
        """清洗文本内容"""
        if not text:
            return ""

        # 移除引用标记和编辑标记
        text = re.sub(r'\[\d+(-\d+)?\]|\[编辑\]|\[详情\]', '', text)

        # 移除HTML标签
        text = re.sub(r'<[^>]+>', '', text)

        # 处理空白字符
        text = re.sub(r'\s+', ' ', text).strip()

        # 移除零宽字符
        text = re.sub(r'[\u200b\u200c\u200d\ufeff\u00A0]', '', text)

        return text.strip()

    def process_organization(self, org_id: int, update_db: bool = True) -> Dict[str, str]:
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
                return {}

        # 获取HTML内容
        html_content = self.db_extractor.get_html_by_org_id(org_id)
        if not html_content:
            logger.warning(f"组织ID={org_id}没有HTML内容，跳过提取")
            return {}

        # 提取信息
        extraction_result = self.extract_from_html(html_content)

        # 将提取结果映射到字段
        mapped_fields = self._map_extraction_to_fields(extraction_result)

        # 如果需要，更新提取结果到数据库
        if update_db:
            for field_name, field_value in mapped_fields.items():
                if field_value:  # 只更新非空值
                    # 对于office_addr字段特殊处理
                    if field_name == 'office_addr':
                        # 先检查字段是否为空
                        current_value = self._get_current_field_value(org_id, field_name)
                        if current_value:
                            logger.info(f"组织ID={org_id}的{field_name}已有值，跳过更新")
                            continue

                    self.db_extractor.update_extraction_result(org_id, field_name, field_value)

        return mapped_fields

    def _get_current_field_value(self, org_id: int, field_name: str) -> str:
        """获取组织当前的字段值"""
        try:
            query = f"""
            SELECT {field_name} 
            FROM c_org_info 
            WHERE id = %s AND is_deleted = 0
            """
            self.db_extractor.cursor.execute(query, (org_id,))
            result = self.db_extractor.cursor.fetchone()

            if result and field_name in result:
                return result[field_name] or ""
            else:
                return ""

        except Exception as e:
            logger.error(f"获取字段值时出错: {str(e)}")
            return ""

    def _map_extraction_to_fields(self, extraction_result: Dict) -> Dict[str, str]:
        """将提取结果映射到数据库字段"""
        mapped_fields = {}

        # 处理description映射到person_title
        if extraction_result.get('description'):
            mapped_fields['person_title'] = extraction_result['description']

        # 处理summary单独映射到org_profile
        if extraction_result.get('summary'):
            mapped_fields['org_profile'] = extraction_result['summary']

        # 处理sections与字段映射
        for section in extraction_result.get('sections', []):
            heading = section.get('heading', '')
            content = section.get('content', '')

            # 如果没有内容，则跳过
            if not content:
                continue

            # 遍历所有字段映射
            for field_name, match_headings in self.field_mapping.items():
                # 匹配标题
                if any(heading == match_heading for match_heading in match_headings):
                    mapped_fields[field_name] = content
                    logger.info(f"字段{field_name}匹配到标题'{heading}'")
                    break

        return mapped_fields

    def process_all_organizations(self, update_db: bool = True) -> List[Dict[str, Any]]:
        """处理所有组织的信息提取"""
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
        """将处理结果保存到文件"""
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