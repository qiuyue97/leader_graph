import mysql.connector
from typing import List, Dict, Any
from utils.logger import get_logger

logger = get_logger(__name__)


class DBManager:
    """数据库管理类，处理与数据库的交互"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据库管理器

        Args:
            config: 数据库配置字典，包含host, user, password, database等字段
        """
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        """连接到数据库"""
        try:
            self.conn = mysql.connector.connect(**self.config)
            logger.info(f"成功连接到数据库: {self.config['database']}")
        except Exception as e:
            logger.error(f"数据库连接失败: {str(e)}")
            raise

    def ensure_connection(self):
        """确保数据库连接有效"""
        if self.conn is None or not self.conn.is_connected():
            logger.info("数据库连接已断开，尝试重新连接")
            self.connect()

    def fetch_urls(self) -> List[Dict[str, Any]]:
        """
        从数据库获取需要爬取的URL列表

        Returns:
            包含ID和URL的字典列表
        """
        self.ensure_connection()

        try:
            cursor = self.conn.cursor(dictionary=True)
            query = """
            SELECT id, leader_name, source_url 
            FROM c_org_leader_info 
            WHERE is_deleted = 0 AND source_url IS NOT NULL AND source_url != ''
            """
            cursor.execute(query)

            results = cursor.fetchall()
            logger.info(f"从数据库获取了 {len(results)} 条URL记录")

            cursor.close()
            return results
        except Exception as e:
            logger.error(f"获取URL失败: {str(e)}")
            return []

    def update_html_content(self, leader_id: int, html_content: str) -> bool:
        """
        更新数据库中的HTML内容

        Args:
            leader_id: 领导人记录ID
            html_content: HTML内容

        Returns:
            更新是否成功
        """
        self.ensure_connection()

        try:
            cursor = self.conn.cursor()
            query = "UPDATE c_org_leader_info SET remark = %s WHERE id = %s"
            cursor.execute(query, (html_content, leader_id))

            self.conn.commit()
            cursor.close()

            logger.info(f"成功更新ID为 {leader_id} 的HTML内容")
            return True
        except Exception as e:
            logger.error(f"更新HTML内容失败: {str(e)}")
            return False

    def check_html_exists(self, leader_id: int) -> bool:
        """
        检查指定ID的记录是否已经有HTML内容

        Args:
            leader_id: 领导人记录ID

        Returns:
            是否已存在HTML内容
        """
        self.ensure_connection()

        try:
            cursor = self.conn.cursor()
            query = "SELECT remark FROM c_org_leader_info WHERE id = %s AND remark IS NOT NULL AND LENGTH(remark) > 100"
            cursor.execute(query, (leader_id,))

            result = cursor.fetchone()
            cursor.close()

            # 如果查询结果不为空且remark有内容，则返回True
            exists = result is not None
            if exists:
                logger.info(f"ID为 {leader_id} 的记录已有HTML内容，无需重新爬取")
            else:
                logger.info(f"ID为 {leader_id} 的记录无HTML内容或内容不足，需要爬取")
            return exists

        except Exception as e:
            logger.error(f"检查HTML内容是否存在时出错: {str(e)}")
            return False

    def close(self):
        """关闭数据库连接"""
        if self.conn and self.conn.is_connected():
            self.conn.close()
            logger.info("数据库连接已关闭")