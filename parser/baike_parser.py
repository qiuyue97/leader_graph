import re
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup

# 获取日志器
from utils.logger import get_logger
logger = get_logger(__name__)


class BaikeParser:
    """专门负责解析百度百科页面内容的解析器"""

    def __init__(self):
        """初始化解析器"""
        # 确保依赖已安装
        try:
            from bs4 import BeautifulSoup
            self.BeautifulSoup = BeautifulSoup
        except ImportError:
            raise ImportError("请安装必要的依赖: pip install beautifulsoup4")

    def parse_page(self, html_content: str) -> Dict[str, Any]:
        """
        解析百科页面内容

        Args:
            html_content: HTML 内容

        Returns:
            解析结果字典
        """
        if not html_content:
            logger.error("无法解析空内容")
            return {
                "success": False,
                "title": "",
                "career_info": [],
                "summary": ""
            }

        try:
            # 提取各种信息
            title = self.extract_person_title(html_content)
            career_raw = self.parse_career_from_html(html_content)
            career_info = self.clean_career_info(career_raw)
            summary = self.extract_summary(html_content)

            # 记录日志
            logger.info(f"成功解析页面")
            logger.info(f"获取到职位: {title}")
            logger.info(f"获取到 {len(career_info)} 条履历信息")

            return {
                "success": True,
                "title": title,
                "career_info": career_info,
                "summary": summary
            }
        except Exception as e:
            logger.error(f"解析页面内容出错: {str(e)}")
            return {
                "success": False,
                "title": "",
                "career_info": [],
                "summary": "",
                "error": str(e)
            }

    def extract_person_title(self, html_content: str) -> str:
        """
        从百科页面提取人物职位

        Args:
            html_content: HTML 内容

        Returns:
            人物职位文本
        """
        if not html_content:
            return ""

        soup = BeautifulSoup(html_content, 'html.parser')

        # 移动版布局
        mobile_title_element = soup.find('li', class_='extra-list-item extra-lemma-desc')
        if mobile_title_element and mobile_title_element.text.strip():
            return mobile_title_element.text.strip()

        # 桌面版布局
        desktop_title_element = soup.find('div', class_='lemmaDescText_WLOIg')
        if desktop_title_element and desktop_title_element.text.strip():
            return desktop_title_element.text.strip()

        # 替代选择器
        additional_selectors = [
            'div.lemma-desc',
            'div.basic-info',
            'div.lemmaWgt-subjectNav'
        ]
        for selector in additional_selectors:
            element = soup.select_one(selector)
            if element and element.text.strip():
                title = element.text.strip()
                title = re.sub(r'\s+', ' ', title)
                return title

        return ""

    def parse_career_from_html(self, html_content: str) -> List[str]:
        """
        从百科页面提取职业生涯信息

        Args:
            html_content: HTML 内容

        Returns:
            包含职业生涯信息的文本列表
        """
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, 'html.parser')
        all_text_info = []

        career_headers = [
            "人物履历", "人物简介", "人物经历", "主要经历",
            "工作经历", "个人经历", "履历", "经历", "简历",
            "教育经历", "工作生涯", "简介"
        ]

        # 查找所有可能的标题
        headers = soup.find_all(['h2', 'h3'], class_=['title-level-2', 'title-level-3'])

        for header in headers:
            header_text = header.get_text().strip()
            if any(term in header_text for term in career_headers):
                logger.info(f"找到符合条件的标题: '{header_text}'")
                current_element = header
                section_content = []

                while current_element.next_sibling:
                    current_element = current_element.next_sibling
                    if current_element.name in ['h2', 'h3']:  # 遇到新标题就停止
                        break

                    # 收集段落内容
                    if current_element.name == 'div' and any(
                            cls in current_element.get('class', []) for cls in ['para', 'content']):
                        text = current_element.get_text().strip()
                        if text:
                            section_content.append(text)

                # 添加发现的内容
                all_text_info.extend(section_content)
                logger.info(f"从'{header_text}'部分提取了 {len(section_content)} 条信息")

        # 如果没有找到符合条件的标题，尝试直接查找内容区域
        if not all_text_info:
            logger.info("未找到符合条件的标题，尝试从正文区域直接提取")
            content_divs = soup.find_all('div', class_=['para', 'content'])
            for div in content_divs:
                text = div.get_text().strip()
                if text and len(text) > 15:  # 只收集长度合适的段落
                    all_text_info.append(text)

        # 移动版页面特殊处理
        if not all_text_info and ('extra-list-item' in html_content or 'content-wrapper' in html_content):
            logger.info("尝试从移动版页面提取内容")
            content_area = soup.find('div', class_=['content-wrapper', 'content'])
            if content_area:
                paragraphs = content_area.find_all(['p', 'div'], class_=['para'])
                for p in paragraphs:
                    text = p.get_text().strip()
                    if text and len(text) > 15:
                        all_text_info.append(text)

        return all_text_info

    def clean_career_info(self, career_info: List[str]) -> List[str]:
        """
        清理文本，移除引用标记、多余空白等

        Args:
            career_info: 原始文本列表

        Returns:
            清理后的文本列表
        """
        cleaned = []
        for info in career_info:
            # 清理各种引用标记和格式
            info = re.sub(r'\[\d+(-\d+)?\]', '', info)  # 引用编号 [1] 或 [1-3]
            info = re.sub(r'<sup.*?</sup>', '', info)  # 上标标签
            info = re.sub(r'\[编辑\]', '', info)  # 编辑标记
            info = re.sub(r'\s+', ' ', info).strip()  # 多余空白
            info = re.sub(r'[\u200b\u200c\u200d]', '', info)  # 零宽字符
            info = re.sub(r'<[^>]+>', '', info)  # HTML标签

            # 只保留有意义的文本
            if info and len(info) > 5:
                cleaned.append(info)

        return cleaned

    def extract_summary(self, html_content: str) -> str:
        """
        提取百科页面的摘要信息

        Args:
            html_content: HTML 内容

        Returns:
            摘要文本
        """
        if not html_content:
            return ""

        soup = BeautifulSoup(html_content, 'html.parser')

        # 尝试提取摘要部分
        summary_selectors = [
            'div.lemma-summary',  # 桌面版摘要
            'div.brief',  # 移动版摘要
            '.float-info .abstract',  # 另一种摘要格式
            '.lemmaWgt-lemmaSummary'  # 另一种摘要类名
        ]

        for selector in summary_selectors:
            summary_el = soup.select_one(selector)
            if summary_el:
                summary = summary_el.get_text().strip()
                summary = re.sub(r'\[\d+(-\d+)?\]', '', summary)  # 移除引用标记
                summary = re.sub(r'\s+', ' ', summary).strip()  # 整理空白
                if summary:
                    return summary

        # 如果没有找到专门的摘要，尝试提取第一段正文
        first_para = soup.find('div', class_='para')
        if first_para:
            first_text = first_para.get_text().strip()
            first_text = re.sub(r'\[\d+(-\d+)?\]', '', first_text)
            first_text = re.sub(r'\s+', ' ', first_text).strip()
            if first_text:
                return first_text

        return ""

    def extract_basic_info(self, html_content: str) -> Dict[str, str]:
        """
        提取人物基本信息表格中的内容

        Args:
            html_content: HTML内容

        Returns:
            基本信息字典
        """
        if not html_content:
            return {}

        soup = BeautifulSoup(html_content, 'html.parser')
        info_dict = {}

        # 桌面版
        basic_info = soup.find('div', class_='basic-info')
        if basic_info:
            items = basic_info.find_all(['dt', 'dd'])
            current_key = None

            for item in items:
                if item.name == 'dt':
                    current_key = item.get_text().strip()
                    current_key = re.sub(r'\s+', '', current_key)
                elif item.name == 'dd' and current_key:
                    value = item.get_text().strip()
                    value = re.sub(r'\[\d+(-\d+)?\]', '', value)  # 移除引用标记
                    value = re.sub(r'\s+', ' ', value).strip()
                    if current_key and value:
                        info_dict[current_key] = value
                    current_key = None

        # 移动版
        if not info_dict:
            info_items = soup.find_all('li', class_='extra-list-item')
            for item in info_items:
                # 尝试找到标题和值
                title_tag = item.find('p', class_='extra-list-item-title')
                value_tag = item.find('p', class_='extra-list-item-content')

                if title_tag and value_tag:
                    key = title_tag.get_text().strip()
                    value = value_tag.get_text().strip()
                    if key and value:
                        info_dict[key] = value

        return info_dict