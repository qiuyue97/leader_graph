import logging
import time
import argparse
import random
from typing import Dict, List, Optional
import mysql.connector
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("html_fetcher.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("html_fetcher")


class SeleniumFetcher:
    """使用Selenium无头浏览器获取网页内容的类"""

    def __init__(self, headless: bool = True):
        """
        初始化Selenium抓取器

        Args:
            headless: 是否使用无头模式
        """
        self.driver = None
        self.headless = headless
        self.setup_driver()

    def setup_driver(self):
        """设置并初始化WebDriver"""
        try:
            options = Options()
            if self.headless:
                options.add_argument('--headless')

            # 基本设置
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--start-maximized')
            # options.add_argument('--proxy-server=60.179.229.241:3000')

            # 反自动化检测设置
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option('excludeSwitches', ['enable-automation'])
            options.add_experimental_option('useAutomationExtension', False)

            # 设置UA
            options.add_argument(
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36')

            # 创建并配置WebDriver
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)

            # 执行JS脚本绕过webdriver检测
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
            })

            logger.info("Selenium WebDriver 初始化成功")
        except Exception as e:
            logger.error(f"Selenium WebDriver 初始化失败: {str(e)}")
            raise

    def get_page_content(self, url: str, max_retries: int = 3) -> str:
        """
        获取页面HTML内容

        Args:
            url: 要获取的URL
            max_retries: 最大重试次数

        Returns:
            页面HTML内容
        """
        if not url:
            logger.warning("URL为空，无法获取内容")
            return ""

        for retry in range(max_retries):
            try:
                logger.info(f"正在访问页面 (尝试 {retry + 1}/{max_retries}): {url}")
                self.driver.get(url)

                # 等待页面加载
                time.sleep(5)

                # 检查是否遇到验证码或安全验证
                page_source = self.driver.page_source
                if "百度安全验证" in page_source or "网络异常" in page_source:
                    logger.warning("遇到安全验证...")

                    if retry == max_retries - 1:
                        if self.headless:
                            # 如果是最后一次重试且处于无头模式，尝试切换到有头模式
                            logger.info("尝试切换到有头模式解决验证问题")
                            self.close()
                            self.headless = False
                            self.setup_driver()
                            continue
                        else:
                            logger.error("无法自动解决安全验证，需要人工处理")
                            return ""
                    else:
                        # 如果不是最后一次重试，等待一段时间后再次尝试
                        wait_time = (retry + 1) * 5  # 递增等待时间
                        logger.info(f"等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue

                # 检查页面内容有效性
                if len(page_source) < 5000:
                    logger.warning(f"页面内容异常短 ({len(page_source)} 字节)，可能加载不完整")
                    if retry < max_retries - 1:
                        time.sleep(5)
                        continue

                logger.info(f"成功获取页面内容 ({len(page_source)} 字节)")
                return page_source

            except WebDriverException as e:
                logger.error(f"Selenium错误: {str(e)}")
                if retry < max_retries - 1:
                    time.sleep(5)
                else:
                    logger.error("达到最大重试次数，放弃获取页面")
            except Exception as e:
                logger.error(f"获取页面时出错: {str(e)}")
                if retry < max_retries - 1:
                    time.sleep(5)
                else:
                    logger.error("达到最大重试次数，放弃获取页面")

        return ""

    def close(self):
        """关闭WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                logger.info("已关闭Selenium WebDriver")
            except Exception as e:
                logger.error(f"关闭WebDriver时出错: {str(e)}")


class DBManager:
    """MySQL数据库管理类"""

    def __init__(self, db_config: Dict[str, str]):
        """
        初始化数据库管理器

        Args:
            db_config: 数据库配置信息，包含host、user、password、database
        """
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

    def get_all_organizations(self) -> List[Dict]:
        """
        获取所有组织记录

        Returns:
            组织记录列表
        """
        try:
            query = """
            SELECT id, uuid, org_name, source_url, remark 
            FROM c_org_info 
            WHERE is_deleted = 0
            """
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            logger.error(f"获取组织记录时出错: {str(e)}")
            return []

    def get_organizations_by_uuids(self, uuids: List[str]) -> List[Dict]:
        """
        根据UUID列表获取组织记录

        Args:
            uuids: UUID列表

        Returns:
            符合条件的组织记录列表
        """
        try:
            # 构造查询条件
            placeholders = ', '.join(['%s'] * len(uuids))
            query = f"""
            SELECT id, uuid, org_name, source_url, remark 
            FROM c_org_info 
            WHERE uuid IN ({placeholders}) AND is_deleted = 0
            """

            self.cursor.execute(query, uuids)
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            logger.error(f"根据UUID获取组织记录时出错: {str(e)}")
            return []

    def update_organization_remark(self, org_id: int, remark: str) -> bool:
        """
        更新组织的remark字段

        Args:
            org_id: 组织ID
            remark: 备注内容（HTML）

        Returns:
            是否成功更新
        """
        try:
            # 更新remark字段
            query = """
            UPDATE c_org_info
            SET remark = %s, update_time = NOW()
            WHERE id = %s
            """

            self.cursor.execute(query, (remark, org_id))
            self.connection.commit()

            logger.info(
                f"成功更新组织remark字段: id={org_id}, HTML内容大小={len(remark)}字节, 受影响行数={self.cursor.rowcount}")
            return True
        except mysql.connector.Error as e:
            logger.error(f"更新组织remark字段时出错: id={org_id}, 错误: {str(e)}")
            # 如果更新失败，尝试截断HTML内容再试一次
            if len(remark) > 65000:
                logger.warning(f"HTML内容过长({len(remark)}字节)，尝试截断为65000字符并重试")
                truncated_remark = remark[:65000]
                try:
                    self.cursor.execute(query, (truncated_remark, org_id))
                    self.connection.commit()
                    logger.info(f"使用截断的HTML内容成功更新组织remark字段: id={org_id}")
                    return True
                except mysql.connector.Error as trunc_error:
                    logger.error(f"使用截断的HTML内容更新时仍然出错: {str(trunc_error)}")
            return False


def fetch_and_store_html(db_config: Dict[str, str], org_uuids: Optional[List[str]] = None):
    """
    从URL获取HTML内容并存储到数据库的remark字段

    Args:
        db_config: 数据库配置
        org_uuids: 要处理的组织UUID列表，如果为None则处理所有记录
    """
    # 创建数据库管理器
    db_manager = DBManager(db_config)

    # 创建Selenium抓取器
    fetcher = SeleniumFetcher(headless=True)

    # 连接数据库
    if not db_manager.connect():
        logger.error("无法连接到数据库，退出处理")
        return

    try:
        # 获取待处理的组织记录
        if org_uuids:
            organizations = db_manager.get_organizations_by_uuids(org_uuids)
            logger.info(f"已获取 {len(organizations)} 条指定UUID的组织记录")
        else:
            organizations = db_manager.get_all_organizations()
            logger.info(f"已获取全部 {len(organizations)} 条组织记录")

        # 处理每条组织记录
        for org in organizations:
            org_id = org['id']
            org_uuid = org['uuid']
            org_name = org['org_name']
            source_url = org['source_url']
            remark = org['remark']

            logger.info(f"处理组织: {org_name} (ID: {org_id}, UUID: {org_uuid})")

            # 如果remark字段已有值，跳过
            if remark:
                logger.info(f"组织 {org_name} 的remark字段已有值，跳过")
                continue

            # 如果没有source_url，跳过
            if not source_url:
                logger.warning(f"组织 {org_name} 没有source_url，跳过")
                continue

            # 获取URL内容
            logger.info(f"从URL获取HTML内容: {source_url}")
            html_content = fetcher.get_page_content(source_url)

            # 更新remark字段
            if html_content:
                if db_manager.update_organization_remark(org_id, html_content):
                    logger.info(f"已更新组织 {org_name} 的remark字段")
                else:
                    logger.error(f"更新组织 {org_name} 的remark字段失败")
            else:
                logger.warning(f"未能获取组织 {org_name} 的HTML内容")

            # 等待一段随机时间，避免频繁请求
            wait_time = random.uniform(2, 5)
            logger.info(f"等待 {wait_time:.2f} 秒...")
            time.sleep(wait_time)

    finally:
        # 断开数据库连接
        db_manager.disconnect()
        # 关闭Selenium
        fetcher.close()
