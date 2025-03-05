import os
import glob
from typing import Dict, List, Optional, Set

# 获取日志器
from utils.logger import get_logger
from utils.file_utils import safe_filename

logger = get_logger(__name__)


class HTMLCacheManager:
    """管理本地HTML缓存文件的类"""

    def __init__(self, cache_dir: str = './data/person_data'):
        """
        初始化HTML缓存管理器

        Args:
            cache_dir: HTML缓存文件目录
        """
        self.cache_dir = cache_dir
        self._cached_files = None  # 缓存的文件列表，延迟加载

        # 确保缓存目录存在
        os.makedirs(cache_dir, exist_ok=True)
        logger.info(f"HTML缓存管理器初始化，缓存目录: {cache_dir}")

    def refresh_cache_index(self) -> int:
        """
        刷新缓存索引，返回缓存文件数量

        Returns:
            缓存文件数量
        """
        pattern = os.path.join(self.cache_dir, "*.html")
        self._cached_files = set(os.path.basename(f) for f in glob.glob(pattern))
        logger.info(f"刷新缓存索引，共找到 {len(self._cached_files)} 个HTML文件")
        return len(self._cached_files)

    def get_cached_files(self) -> Set[str]:
        """
        获取缓存文件列表

        Returns:
            缓存文件名集合
        """
        if self._cached_files is None:
            self.refresh_cache_index()
        return self._cached_files

    def is_cached(self, person_name: str, person_id: str) -> bool:
        """
        检查指定人物的HTML是否已缓存

        Args:
            person_name: 人物姓名
            person_id: 人物ID

        Returns:
            是否已缓存
        """
        if self._cached_files is None:
            self.refresh_cache_index()

        # 安全处理文件名
        safe_name = safe_filename(person_name)
        expected_filename = f"{safe_name}_{person_id}.html"

        return expected_filename in self._cached_files

    def get_cache_path(self, person_name: str, person_id: str) -> str:
        """
        获取缓存文件的完整路径

        Args:
            person_name: 人物姓名
            person_id: 人物ID

        Returns:
            缓存文件的完整路径
        """
        safe_name = ''.join(c for c in person_name if c.isalnum() or c in '_-')
        filename = f"{safe_name}_{person_id}.html"
        return os.path.join(self.cache_dir, filename)

    def get_html_content(self, person_name: str, person_id: str) -> Optional[str]:
        """
        获取已缓存的HTML内容

        Args:
            person_name: 人物姓名
            person_id: 人物ID

        Returns:
            HTML内容，如果不存在则返回None
        """
        cache_path = self.get_cache_path(person_name, person_id)

        if not os.path.exists(cache_path):
            logger.debug(f"缓存文件不存在: {cache_path}")
            return None

        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                content = f.read()
                logger.debug(f"从缓存读取HTML内容: {cache_path}")
                return content
        except Exception as e:
            logger.error(f"读取缓存文件出错: {cache_path}, 错误: {str(e)}")
            return None

    def filter_uncached_tasks(self, tasks: List) -> List:
        """
        过滤出未缓存的任务

        Args:
            tasks: 任务列表

        Returns:
            未缓存的任务列表
        """
        if self._cached_files is None:
            self.refresh_cache_index()

        uncached_tasks = []
        for task in tasks:
            if not self.is_cached(task.person_name, task.person_id):
                uncached_tasks.append(task)

        logger.info(f"任务过滤结果: 总任务 {len(tasks)}，未缓存任务 {len(uncached_tasks)}")
        return uncached_tasks