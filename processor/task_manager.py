import logging
import threading
import queue
import time
from typing import List, Dict, Any, Optional, Callable, Generic, TypeVar, Tuple

# 获取日志器
logger = logging.getLogger(__name__)

# 定义泛型类型变量
T = TypeVar('T')  # 任务类型
R = TypeVar('R')  # 结果类型


class TaskManager(Generic[T, R]):
    """任务管理器，处理任务调度和结果收集"""

    def __init__(self, max_queue_size: int = 100):
        """
        初始化任务管理器

        Args:
            max_queue_size: 队列最大大小
        """
        self.task_queue = queue.Queue(maxsize=max_queue_size)
        self.result_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.lock = threading.Lock()

        # 状态追踪
        self.total_tasks = 0
        self.completed_tasks = 0
        self.successful_tasks = 0
        self.failed_tasks = 0

    def add_task(self, task: T):
        """
        添加任务到队列

        Args:
            task: 要添加的任务
        """
        self.task_queue.put(task)
        with self.lock:
            self.total_tasks += 1

    def add_tasks(self, tasks: List[T]):
        """
        批量添加任务到队列

        Args:
            tasks: 要添加的任务列表
        """
        for task in tasks:
            self.add_task(task)

    def get_task(self, timeout: Optional[float] = None) -> Optional[T]:
        """
        从队列获取任务

        Args:
            timeout: 等待超时时间（秒）

        Returns:
            任务对象，如果队列为空或超时则返回None
        """
        try:
            return self.task_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def task_done(self):
        """标记一个任务已完成"""
        self.task_queue.task_done()
        with self.lock:
            self.completed_tasks += 1

    def add_result(self, result: R, success: bool = True):
        """
        添加处理结果

        Args:
            result: 处理结果
            success: 是否成功
        """
        self.result_queue.put((result, success))
        with self.lock:
            if success:
                self.successful_tasks += 1
            else:
                self.failed_tasks += 1

    def get_result(self, timeout: Optional[float] = None) -> Optional[Tuple[R, bool]]:
        """
        获取处理结果

        Args:
            timeout: 等待超时时间（秒）

        Returns:
            (结果对象, 是否成功)的元组，如果队列为空或超时则返回None
        """
        try:
            return self.result_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def result_done(self):
        """标记一个结果已处理"""
        self.result_queue.task_done()

    def stop(self):
        """停止任务管理器"""
        self.stop_event.set()

    def is_stopped(self) -> bool:
        """检查是否已停止"""
        return self.stop_event.is_set()

    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """
        等待所有任务完成

        Args:
            timeout: 等待超时时间（秒）

        Returns:
            是否所有任务已完成
        """
        try:
            self.task_queue.join()
            return True
        except Exception:
            return False

    def get_status(self) -> Dict[str, int]:
        """
        获取当前状态信息

        Returns:
            状态信息字典
        """
        with self.lock:
            return {
                "total_tasks": self.total_tasks,
                "completed_tasks": self.completed_tasks,
                "successful_tasks": self.successful_tasks,
                "failed_tasks": self.failed_tasks,
                "pending_tasks": self.total_tasks - self.completed_tasks,
                "task_queue_size": self.task_queue.qsize(),
                "result_queue_size": self.result_queue.qsize()
            }