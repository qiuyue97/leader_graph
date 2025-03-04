import time
import random
import logging
from typing import Dict, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

# 尝试导入 fake_useragent，如果不可用则使用内置 UA 列表
try:
    from fake_useragent import UserAgent

    has_fake_useragent = True
except ImportError:
    has_fake_useragent = False

# 获取日志器
logger = logging.getLogger(__name__)


class SeleniumScraper:
    """包装 Selenium WebDriver 的爬虫类，支持代理和移动设备模拟"""

    # 常见的安卓设备 User-Agent 列表
    ANDROID_USER_AGENTS = [
        # 三星 Galaxy S21
        "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.105 Mobile Safari/537.36",
        # 谷歌 Pixel 5
        "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.91 Mobile Safari/537.36",
        # OPPO Find X3
        "Mozilla/5.0 (Linux; Android 11; PCAM00) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.105 Mobile Safari/537.36",
        # 小米 11
        "Mozilla/5.0 (Linux; Android 11; M2011K2C) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.105 Mobile Safari/537.36",
        # 华为 P40
        "Mozilla/5.0 (Linux; Android 10; ELS-AN00) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.93 Mobile Safari/537.36"
    ]

    def __init__(self, proxy: Optional[Dict[str, str]] = None, headless: bool = True,
                 mobile: bool = True, page_load_timeout: int = 30,
                 wait_time: int = 3):
        """
        初始化 Selenium 爬虫

        Args:
            proxy: 代理配置字典，包含 'http' 和 'https' 键
            headless: 是否以无头模式运行
            mobile: 是否模拟移动设备
            page_load_timeout: 页面加载超时时间（秒）
            wait_time: 页面加载后等待时间（秒）
        """
        self.proxy = proxy
        self.headless = headless
        self.mobile = mobile
        self.page_load_timeout = page_load_timeout
        self.wait_time = wait_time
        self.driver = None
        self._setup_driver()

    def _setup_driver(self):
        """设置并初始化 WebDriver"""
        self.options = Options()

        # 基本设置
        if self.headless:
            self.options.add_argument("--headless")

        # 设置代理
        if self.proxy:
            self.options.add_argument(f'--proxy-server={self.proxy["http"]}')

        # 通用优化设置
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')

        # 移动设备模拟
        if self.mobile:
            selected_ua = random.choice(self.ANDROID_USER_AGENTS)
            self.options.add_argument(f'--user-agent={selected_ua}')

            # 设置移动设备参数
            mobile_emulation = {
                "deviceMetrics": {"width": 360, "height": 740, "pixelRatio": 3.0},
                "userAgent": selected_ua
            }
            self.options.add_experimental_option("mobileEmulation", mobile_emulation)

            # 设置移动设备特性
            self.options.add_argument("--enable-features=NetworkService,NetworkServiceInProcess")
            self.options.add_argument("--force-device-scale-factor=2.0")
        else:
            # 非移动设备，使用随机 UA
            if has_fake_useragent:
                try:
                    ua = UserAgent()
                    self.options.add_argument(f'--user-agent={ua.random}')
                except Exception as e:
                    logger.warning(f"无法设置随机UA: {str(e)}")
                    # 如果失败，使用默认 UA
            else:
                # fake_useragent 不可用，使用默认 Chrome UA
                logger.info("fake_useragent 不可用，使用默认 Chrome UA")

        try:
            self.driver = webdriver.Chrome(options=self.options)
            self.driver.set_page_load_timeout(self.page_load_timeout)
            logger.info("WebDriver 初始化成功")
        except Exception as e:
            logger.error(f"WebDriver 初始化失败: {str(e)}")
            raise

    def fetch_page(self, url: str) -> Optional[str]:
        """
        使用 Selenium 获取页面内容

        Args:
            url: 要获取的页面 URL

        Returns:
            成功时返回页面 HTML 内容，失败时返回 None
        """
        if not self.driver:
            logger.error("WebDriver 未初始化")
            return None

        try:
            logger.info(f"正在获取页面: {url}")
            self.driver.get(url)

            # 等待 JavaScript 加载
            time.sleep(self.wait_time)

            # 获取页面内容
            html_content = self.driver.page_source
            logger.info(f"成功获取页面内容: {url}")
            return html_content
        except WebDriverException as e:
            logger.error(f"Selenium 错误: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"获取页面时发生错误: {str(e)}")
            return None

    def scroll_down(self, times: int = 3, delay: float = 0.5):
        """
        向下滚动页面

        Args:
            times: 滚动次数
            delay: 每次滚动后的延迟（秒）
        """
        if not self.driver:
            logger.error("WebDriver 未初始化")
            return

        try:
            for i in range(times):
                self.driver.execute_script("window.scrollBy(0, window.innerHeight);")
                time.sleep(delay)
            logger.debug(f"已向下滚动 {times} 次")
        except Exception as e:
            logger.error(f"滚动页面时发生错误: {str(e)}")

    def execute_javascript(self, script: str):
        """
        执行 JavaScript 代码

        Args:
            script: 要执行的 JavaScript 代码

        Returns:
            JavaScript 执行结果
        """
        if not self.driver:
            logger.error("WebDriver 未初始化")
            return None

        try:
            return self.driver.execute_script(script)
        except Exception as e:
            logger.error(f"执行 JavaScript 时发生错误: {str(e)}")
            return None

    def take_screenshot(self, filename: str) -> bool:
        """
        截取当前页面的屏幕截图

        Args:
            filename: 保存截图的文件名

        Returns:
            操作是否成功
        """
        if not self.driver:
            logger.error("WebDriver 未初始化")
            return False

        try:
            self.driver.save_screenshot(filename)
            logger.info(f"已保存截图: {filename}")
            return True
        except Exception as e:
            logger.error(f"截图时发生错误: {str(e)}")
            return False

    def close(self):
        """关闭 WebDriver 会话"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                logger.info("WebDriver 已关闭")
            except Exception as e:
                logger.error(f"关闭 WebDriver 时发生错误: {str(e)}")

    def __enter__(self):
        """支持上下文管理器模式"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """离开上下文时自动关闭 WebDriver"""
        self.close()