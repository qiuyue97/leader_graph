import os
import random
import time
from typing import Dict, Optional, Any
from datetime import datetime

from .selenium_scraper import SeleniumScraper
from proxy.pool import ProxyPool

# 获取日志器
from utils.logger import get_logger
logger = get_logger(__name__)


class BaikeScraper:
    """专门负责获取百度百科页面内容的爬虫，不负责内容解析"""

    def __init__(self, proxy_pool: Optional[ProxyPool] = None,
                 output_dir: str = './person_data',
                 max_retries: int = 3):
        """
        初始化百科爬虫

        Args:
            proxy_pool: 代理池实例，用于获取代理
            output_dir: 输出目录，用于保存 HTML 文件
            max_retries: 重试次数
        """
        self.proxy_pool = proxy_pool
        self.output_dir = output_dir
        self.max_retries = max_retries
        self.selenium_scraper = None

        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"已创建输出目录: {self.output_dir}")

    def fetch_page(self, url: str, use_mobile: bool = True,
                   provided_proxy: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        使用 Selenium 获取页面 HTML 内容

        Args:
            url: 要抓取的 URL
            use_mobile: 是否使用移动设备模拟
            provided_proxy: 外部提供的代理

        Returns:
            成功时返回 HTML 内容，失败时返回 None
        """
        for attempt in range(self.max_retries):
            try:
                # 使用外部提供的代理，或者从代理池获取
                proxy = provided_proxy
                if not proxy and self.proxy_pool:
                    proxy = self.proxy_pool.get_proxy()

                # 初始化 Selenium 爬虫
                self.selenium_scraper = SeleniumScraper(
                    proxy=proxy,
                    headless=True,
                    mobile=use_mobile
                )

                # 获取页面内容
                html_content = self.selenium_scraper.fetch_page(url)

                if html_content:
                    return html_content
                else:
                    logger.warning(f"第 {attempt + 1} 次尝试获取页面失败: {url}")
                    raise Exception("获取页面失败")

            except Exception as e:
                logger.error(f"请求失败 (第 {attempt + 1} 次尝试): {str(e)}")

                # 如果使用了代理池中的代理且失败，标记为失败
                if proxy and self.proxy_pool and proxy != provided_proxy:
                    self.proxy_pool.return_proxy(proxy, mark_as_failed=True)

                if attempt == self.max_retries - 1:
                    logger.error(f"已达到最大重试次数: {url}")
                    return None

                time.sleep(random.uniform(1, 3))

            finally:
                # 确保关闭 Selenium 爬虫
                if self.selenium_scraper:
                    self.selenium_scraper.close()
                    self.selenium_scraper = None

        return None

    def save_html_content(self, url: str, html_content: str, name: str, person_id: str = None) -> Optional[str]:
        """
        保存网页 HTML 内容到文件

        Args:
            url: 页面 URL
            html_content: HTML 内容
            name: 人物姓名
            person_id: 原始 CSV 中的 person_id 字段

        Returns:
            成功时返回保存的文件路径，失败时返回 None
        """
        try:
            # 从标题中提取姓名
            safe_name = ''.join(c for c in name if c.isalnum() or c in '_-')  # 确保文件名安全

            # 构建文件名 (使用"姓名_id")
            if person_id:
                filename = f"{self.output_dir}/{safe_name}_{person_id}.html"
            else:
                # 如果没有提供 person_id，使用备选方案
                url_id = url.split('/')[-1]
                filename = f"{self.output_dir}/{safe_name}_{url_id}.html"

            # 保存 HTML 内容
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)

            logger.info(f"已保存 HTML 内容到: {filename}")
            return filename
        except Exception as e:
            logger.error(f"保存 HTML 内容失败: {str(e)}")
            return None

    def fetch_with_metadata(self, url: str, person_name: str = None, person_id: str = None,
                            use_mobile: bool = True,
                            provided_proxy: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        抓取页面并返回元数据（不进行解析）

        Args:
            url: 要抓取的 URL
            person_name: 人物姓名
            person_id: 人物 ID
            use_mobile: 是否使用移动设备模拟
            provided_proxy: 外部提供的代理

        Returns:
            包含 HTML 内容和元数据的字典
        """
        html_content = self.fetch_page(url, use_mobile, provided_proxy)

        if not html_content:
            logger.error(f"未能获取页面内容: {url}")
            return {
                "success": False,
                "html_content": "",
                "url": url,
                "timestamp": datetime.now().isoformat()
            }

        # 保存 HTML 内容
        saved_file = None
        if person_name:
            saved_file = self.save_html_content(url, html_content, person_name, person_id)

        return {
            "success": True,
            "html_content": html_content,
            "url": url,
            "person_name": person_name,
            "person_id": person_id,
            "saved_file": saved_file,
            "timestamp": datetime.now().isoformat()
        }