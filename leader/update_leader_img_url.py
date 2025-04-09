#!/usr/bin/env python3
"""
update_leader_img_url.py
从数据库中提取领导人HTML内容，并抽取领导人头像图片的URL
"""

import re
import argparse
import pymysql
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Dict, List, Optional, Tuple


class LeaderImageExtractor:
    """从HTML内容中提取领导人头像图片URL的类"""

    def __init__(self, db_config: Dict[str, str]):
        """
        初始化提取器

        Args:
            db_config: 数据库配置信息，包含host、user、password、database
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None

    def _is_valid_image_url(self, url: str) -> bool:
        """
        检查图片URL是否有效

        Args:
            url: 图片URL

        Returns:
            是否为有效的图片URL
        """
        # 检查是否是无效的URL或logo
        invalid_patterns = [
            "logo-baike.svg",
            "baike-react/common",
            "icon",
            "/img/"
        ]

        for pattern in invalid_patterns:
            if pattern in url:
                return False

        # 检查是否是有效的百度百科图片URL
        valid_patterns = [
            "bkimg.cdn.bcebos.com/pic/",
            "/pic/"
        ]

        for pattern in valid_patterns:
            if pattern in url:
                return True

        # 检查URL长度，太短的可能不是有效的图片URL
        if len(url) < 30:
            return False

        # 检查图片格式
        image_extensions = ['.jpg', '.jpeg', '.png']
        if not any(ext in url.lower() for ext in image_extensions) and "x-bce-process" not in url:
            return False

        return False

    def connect_db(self) -> bool:
        """连接到数据库"""
        try:
            self.connection = pymysql.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.connection.cursor()
            print(f"成功连接到数据库: {self.db_config['database']}")
            return True
        except Exception as e:
            print(f"连接数据库失败: {str(e)}")
            return False

    def disconnect_db(self):
        """断开数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("数据库连接已关闭")

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

            self.cursor.execute(query, params)
            leaders = self.cursor.fetchall()
            print(f"找到 {len(leaders)} 条领导人记录")
            return leaders
        except Exception as e:
            print(f"获取领导人记录时出错: {str(e)}")
            return []

    def extract_image_url(self, html_content: str) -> Optional[str]:
        """
        从HTML内容中提取领导人头像图片URL

        Args:
            html_content: HTML内容

        Returns:
            图片URL或None
        """
        if not html_content:
            return None

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # 方法1: 首先尝试查找abstractAlbum类的div中的img元素
            abstract_album = soup.find(
                'div',
                class_=lambda c: c and any(cls.startswith('abstractAlbum_') for cls in c.split())
            )
            if abstract_album:
                img = abstract_album.find('img')
                if img and img.get('src'):
                    url = img['src']
                    if self._is_valid_image_url(url):
                        return url

            # 方法2: 尝试查找lemmaWgt-lemmaTitle-title类附近的图片
            lemma_title = soup.find('div', class_='lemmaWgt-lemmaTitle-title')
            if lemma_title:
                # 查找附近的图片模块
                for element in lemma_title.find_all_next():
                    if element.name == 'div' and 'lemma-picture' in element.get('class', []):
                        img = element.find('img')
                        if img and img.get('src'):
                            url = img['src']
                            if self._is_valid_image_url(url):
                                return url

            # 方法3: 查找summary-pic类的元素
            summary_pic = soup.find('div', class_='summary-pic')
            if summary_pic:
                img = summary_pic.find('img')
                if img and img.get('src'):
                    url = img['src']
                    if self._is_valid_image_url(url):
                        return url

            # 方法4: 查找百科图片模块
            image_module = soup.find('div', {'data-module-type': 'image'})
            if image_module:
                img = image_module.find('img')
                if img and img.get('src'):
                    url = img['src']
                    if self._is_valid_image_url(url):
                        return url

            # 方法5: 查找img标签中包含领导人名字的图片
            leader_name = None
            # 尝试获取标题(用于确定领导人名字)
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.text.strip()
                if "_百度百科" in title_text:
                    leader_name = title_text.split("_百度百科")[0].strip()

            if leader_name:
                # 查找所有图片
                for img in soup.find_all('img'):
                    # 检查alt文本或图片URL是否包含领导人名字
                    alt_text = img.get('alt', '')
                    if (leader_name in alt_text or leader_name in img.get('src', '')) and img.get('src'):
                        url = img['src']
                        if self._is_valid_image_url(url):
                            return url

            # 方法6: 找不到特定结构时，获取第一张合适的图片
            # 在百度百科中，通常第一张图片就是人物头像
            all_images = soup.find_all('img')
            for img in all_images:
                src = img.get('src', '')
                if src and self._is_valid_image_url(src):
                    return src

            return None
        except Exception as e:
            print(f"提取图片URL时出错: {str(e)}")
            return None

    def update_leader_image_url(self, leader_id: int, image_url: Optional[str]) -> bool:
        """
        更新领导人的image_url字段

        Args:
            leader_id: 领导人ID
            image_url: 图片URL，如果为None则设置为NULL

        Returns:
            是否成功更新
        """
        try:
            # 检查字段是否存在
            check_query = """
            SHOW COLUMNS FROM c_org_leader_info LIKE 'image_url'
            """
            self.cursor.execute(check_query)
            if not self.cursor.fetchone():
                print(f"字段'image_url'不存在，创建该字段...")
                # 添加字段
                alter_query = """
                ALTER TABLE c_org_leader_info ADD COLUMN image_url VARCHAR(500) COMMENT '图像链接'
                """
                self.cursor.execute(alter_query)
                self.connection.commit()
                print("成功创建字段: image_url")

            # 更新字段值
            update_query = """
            UPDATE c_org_leader_info
            SET image_url = %s
            WHERE id = %s
            """
            self.cursor.execute(update_query, (image_url, leader_id))
            self.connection.commit()

            if image_url:
                print(f"成功更新领导人ID={leader_id}的image_url: {image_url}")
            else:
                print(f"成功将领导人ID={leader_id}的image_url设置为NULL")

            return True

        except Exception as e:
            print(f"更新image_url时出错: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False

    def process_leader(self, leader: Dict) -> Tuple[bool, Optional[str]]:
        """
        处理单个领导人的图片提取

        Args:
            leader: 领导人信息字典

        Returns:
            (是否成功, 提取的图片URL)
        """
        leader_id = leader['id']
        leader_name = leader['leader_name']
        html_content = leader.get('remark', '')

        if not html_content:
            print(f"领导人 {leader_name} (ID: {leader_id}) 没有HTML内容")
            return False, None

        # 尝试提取图片URL
        image_url = self.extract_image_url(html_content)

        if image_url:

            # 如果有x-bce-process参数但没有正确格式，规范化URL
            if "x-bce-process" in image_url and not image_url.endswith("m_lfit,limit_1,w_536"):
                # 确保URL包含必要的处理参数
                if "?" in image_url:
                    base_url = image_url.split("?")[0]
                else:
                    base_url = image_url

                # 添加统一的处理参数
                image_url = f"{base_url}?x-bce-process=image/format,f_auto/quality,Q_70/resize,m_lfit,limit_1,w_536"
                print(f"规范化URL格式: {image_url}")

            return True, image_url
        else:
            print(f"未找到领导人 {leader_name} 的图片URL")
            return False, None

    def process_leaders(self, limit: Optional[int] = None, leader_id: Optional[int] = None, update_db: bool = True) -> \
    Dict[int, str]:
        """
        处理多个领导人的图片提取

        Args:
            limit: 限制处理数量
            leader_id: 指定处理单个领导人ID
            update_db: 是否更新到数据库

        Returns:
            领导人ID到图片URL的映射字典
        """
        if not self.connect_db():
            return {}

        try:
            # 获取领导人列表
            leaders = self.get_leaders(limit, leader_id)

            if not leaders:
                print("没有找到领导人记录")
                return {}

            # 处理每个领导人
            results = {}
            for leader in leaders:
                try:
                    success, image_url = self.process_leader(leader)
                    if success and image_url:
                        results[leader['id']] = image_url

                        # 如果需要更新到数据库
                        if update_db:
                            self.update_leader_image_url(leader['id'], image_url)
                    else:
                        # 如果需要更新到数据库，即使没有找到图片URL也要更新为NULL
                        if update_db:
                            self.update_leader_image_url(leader['id'], None)

                except Exception as e:
                    print(f"处理领导人 {leader.get('leader_name', '')} (ID: {leader.get('id', '')}) 时出错: {str(e)}")
                    import traceback
                    print(traceback.format_exc())

            return results

        finally:
            self.disconnect_db()


def update_leader_img_url(db_config):
    # 创建提取器并处理
    extractor = LeaderImageExtractor(db_config)
    results = extractor.process_leaders()

    # 打印摘要
    print(f"\n提取完成! 总共处理了 {len(results)} 个领导人的图片URL")