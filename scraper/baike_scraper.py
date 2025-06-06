import random
import time
from typing import Dict, Optional, Any
from datetime import datetime

from .selenium_scraper import SeleniumScraper
from proxy.pool import ProxyPool
from utils.content_validator import ContentValidator

# 获取日志器
from utils.logger import get_logger

logger = get_logger(__name__)


class BaikeScraper:
    """专门负责获取百度百科页面内容的爬虫，不负责内容解析"""

    def __init__(self, proxy_pool: Optional[ProxyPool] = None,
                 max_retries: int = 3,
                 min_content_size: int = 1024):
        """
        初始化百科爬虫

        Args:
            proxy_pool: 代理池实例，用于获取代理
            max_retries: 重试次数
            min_content_size: 有效HTML内容的最小字节数
        """
        self.proxy_pool = proxy_pool
        self.max_retries = max_retries
        self.min_content_size = min_content_size
        self.selenium_scraper = None

        # 初始化内容验证器
        self.content_validator = ContentValidator(min_content_size=min_content_size)

    def fetch_page(self, url: str, use_mobile: bool = False,
                   provided_proxy: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        使用 Selenium 获取页面 HTML 内容

        Args:
            url: 要抓取的 URL
            use_mobile: 是否使用移动设备模拟 (现在总是使用桌面版)
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
                    # 使用内容验证器验证内容
                    validation_result = self.content_validator.is_valid_content(html_content)

                    if validation_result["valid"]:
                        logger.info(f"成功获取页面，内容大小: {validation_result['content_size']} 字节")
                        return html_content
                    else:
                        reason = validation_result.get("reason", "未知原因")
                        logger.warning(f"第 {attempt + 1} 次尝试获取的页面无效: {reason}")

                        # 如果需要更换代理
                        if validation_result.get("need_proxy_change", False):
                            logger.info("需要更换代理")
                            if proxy and self.proxy_pool and proxy != provided_proxy:
                                if hasattr(self.proxy_pool, 'return_proxy'):
                                    self.proxy_pool.return_proxy(proxy, mark_as_failed=True)
                            # 强制下一次使用新代理
                            proxy = None

                        raise Exception(f"页面内容无效: {reason}")
                else:
                    logger.warning(f"第 {attempt + 1} 次尝试获取页面失败: {url}")
                    raise Exception("获取页面失败")

            except Exception as e:
                logger.error(f"请求失败 (第 {attempt + 1} 次尝试): {str(e)}")

                # 如果使用了代理池中的代理且失败，标记为失败
                if proxy and self.proxy_pool and proxy != provided_proxy:
                    if hasattr(self.proxy_pool, 'return_proxy'):
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

    def fetch_with_metadata(self, url: str, person_name: str = None, person_id: str = None,
                            use_mobile: bool = False,
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
                "timestamp": datetime.now().isoformat(),
                "error": "页面获取失败或内容为空"
            }

        # 使用内容验证器再次验证内容
        validation_result = self.content_validator.is_valid_content(html_content)

        if not validation_result["valid"]:
            reason = validation_result.get("reason", "未知原因")
            logger.warning(f"页面内容验证失败: {reason}")
            return {
                "success": False,
                "html_content": html_content,  # 仍然保留原始内容以供调试
                "content_size": validation_result.get("content_size", 0),
                "url": url,
                "timestamp": datetime.now().isoformat(),
                "error": f"页面内容验证失败: {reason}",
                "validation_result": validation_result
            }

        return {
            "success": True,
            "html_content": html_content,
            "content_size": validation_result.get("content_size", 0),
            "url": url,
            "person_name": person_name,
            "person_id": person_id,
            "timestamp": datetime.now().isoformat(),
            "validation_result": validation_result
        }