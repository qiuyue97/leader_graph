import threading
import queue
import time
import random
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass

from proxy.pool import ProxyPool
from scraper.baike_scraper import BaikeScraper
from parser.baike_parser import BaikeParser
from utils.logger import get_logger
from utils.db_utils import DBManager

# 获取日志器
logger = get_logger(__name__)


@dataclass
class ProcessorTask:
    """表示单个处理任务的数据类"""
    idx: int  # 在原始数据中的索引
    url: str  # 要爬取的URL
    person_name: str = ""  # 人物姓名
    person_id: str = ""  # 人物ID
    data: Dict[str, Any] = None  # 任务相关的其他数据

    def __post_init__(self):
        if self.data is None:
            self.data = {}


class DataProcessor:
    """
    处理数据的类，使用生产者-消费者模式和线程池
    支持分离的爬取和解析阶段
    """

    def __init__(self, config, proxy_pool: Optional[ProxyPool] = None,
                 num_producers: int = 3, num_consumers: int = 2,
                 save_interval: int = 10,
                 min_content_size: int = 1024,
                 update: bool = False):
        """
        初始化数据处理器

        Args:
            config: 配置对象，包含数据库配置
            proxy_pool: 代理池实例
            num_producers: 生产者线程数
            num_consumers: 消费者线程数
            save_interval: 自动保存间隔（处理多少条记录后保存一次）
        """
        # 是否过滤掉已有信息的领导人
        self.filter_existing = not update

        self.db_manager = DBManager(config.db_config)
        self.proxy_pool = proxy_pool
        self.num_producers = num_producers
        self.num_consumers = num_consumers
        self.save_interval = save_interval

        # 任务队列和结果队列
        self.task_queue = queue.Queue(maxsize=100)
        self.result_queue = queue.Queue()

        # 控制信号
        self.stop_event = threading.Event()
        self.task_lock = threading.Lock()
        self.result_lock = threading.Lock()

        # 统计信息
        self.processed_count = 0
        self.total_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.retry_count = 0
        self.max_retries = 3
        self.start_time = None

        # 创建爬虫和解析器
        self.scraper = BaikeScraper(
            proxy_pool=proxy_pool,
            min_content_size=min_content_size
        )
        self.parser = BaikeParser()

        logger.info(
            f"数据处理器初始化完成，生产者:{num_producers}，消费者:{num_consumers}，最小内容大小:{min_content_size}字节")

    def load_tasks_from_db(self, filter_existing=True) -> List[ProcessorTask]:
        """
        从数据库加载任务

        Args:
            filter_existing: 是否过滤掉已有HTML内容的记录

        Returns:
            任务列表
        """
        try:
            logger.info("从数据库加载URL数据")
            url_records = self.db_manager.fetch_urls()

            # 创建任务列表
            tasks = []
            skipped_count = 0

            for record in url_records:
                url = record.get('source_url', '').strip()
                if not url:  # 跳过无URL的记录
                    continue

                person_name = str(record.get('person_name', '')).strip()
                person_id = str(record.get('id', '')).strip()

                # 检查是否已有HTML内容
                if filter_existing and person_id.isdigit():
                    leader_id = int(person_id)
                    if self.db_manager.check_html_exists(leader_id):
                        skipped_count += 1
                        continue

                # 创建任务
                task = ProcessorTask(
                    idx=int(person_id) if person_id.isdigit() else 0,
                    url=url,
                    person_name=person_name,
                    person_id=person_id,
                    data=dict(record)
                )
                tasks.append(task)

            logger.info(f"共加载 {len(tasks)} 个需要爬取的任务，跳过 {skipped_count} 个已有内容的记录")
            return tasks

        except Exception as e:
            logger.error(f"从数据库加载URL数据时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def split_tasks(self, tasks: List[ProcessorTask]) -> List[List[ProcessorTask]]:
        """
        将任务分成多个子任务列表，每个生产者处理一部分

        Args:
            tasks: 所有任务的列表

        Returns:
            任务分组列表
        """
        if not tasks:
            return [[] for _ in range(self.num_producers)]

        # 采用轮询方式分配，确保每个生产者的任务数量尽可能均衡
        chunks = [[] for _ in range(self.num_producers)]
        for i, task in enumerate(tasks):
            chunks[i % self.num_producers].append(task)

        for i, chunk in enumerate(chunks):
            logger.info(f"生产者 {i} 分配到 {len(chunk)} 个任务")

        return chunks

    def process_db_fetch_stage(self):
        """
        执行爬取阶段，从数据库读取URL，爬取HTML并存储到数据库
        """
        try:
            self.start_time = time.time()

            # 从数据库加载任务
            tasks = self.load_tasks_from_db(filter_existing=self.filter_existing)
            if not tasks:
                logger.warning("没有任务需要处理")
                return

            self.total_count = len(tasks)
            logger.info(f"准备爬取 {self.total_count} 个任务")

            # 分割任务
            task_chunks = self.split_tasks(tasks)

            # 启动消费者线程
            consumers = []
            logger.info(f"启动 {self.num_consumers} 个消费者线程")
            for i in range(self.num_consumers):
                t = threading.Thread(target=self.db_fetch_consumer, args=(i,), daemon=True)
                t.start()
                consumers.append(t)

            # 等待代理池初始化（如果有）
            if self.proxy_pool:
                logger.info("等待代理池准备就绪...")
                time.sleep(3)

            # 启动生产者线程，每隔20秒启动一个
            producers = []
            for i in range(self.num_producers):
                # 每个生产者负责处理一部分任务
                producer_tasks = task_chunks[i] if i < len(task_chunks) else []
                t = threading.Thread(target=self.only_fetch_producer, args=(i, producer_tasks), daemon=True)
                t.start()
                producers.append(t)
                logger.info(f"生产者 {i} 已启动")

                # 每启动一个生产者后等待20秒再启动下一个
                if i < self.num_producers - 1:  # 如果不是最后一个生产者
                    logger.info(f"等待20秒后启动下一个生产者...")
                    time.sleep(20)

            # 等待所有生产者完成
            for p in producers:
                p.join()

            logger.info("所有生产者已完成工作")

            # 设置停止标志
            self.stop_event.set()

            # 等待队列清空
            logger.info("等待队列处理完毕...")
            self.result_queue.join()

            # 等待所有消费者线程结束
            for c in consumers:
                c.join(timeout=5)  # 最多等待5秒

            # 计算总耗时
            elapsed_time = time.time() - self.start_time

            logger.info("所有消费者已完成工作")
            logger.info(
                f"HTML爬取阶段完成，共处理 {self.processed_count} 条记录，成功: {self.success_count}, 失败: {self.failure_count}, 重试: {self.retry_count}")
            logger.info(f"总耗时: {elapsed_time:.2f} 秒")

            # 关闭数据库连接
            if hasattr(self, 'db_manager'):
                self.db_manager.close()

        except Exception as e:
            logger.error(f"处理数据时出现未捕获的异常: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

            # 确保数据库连接关闭
            if hasattr(self, 'db_manager'):
                self.db_manager.close()

    def db_fetch_consumer(self, consumer_id: int):
        """
        爬取阶段的消费者函数，处理爬取结果并更新数据库

        Args:
            consumer_id: 消费者ID
        """
        logger.info(f"爬取消费者 {consumer_id} 开始工作")

        save_counter = 0  # 记录处理了多少条结果

        while not (self.stop_event.is_set() and self.result_queue.empty()):
            try:
                # 从结果队列获取项目，最多等待3秒
                try:
                    result = self.result_queue.get(timeout=3)
                except queue.Empty:
                    if self.stop_event.is_set():
                        logger.info(f"爬取消费者 {consumer_id} 队列为空且收到停止信号，退出")
                        break
                    continue

                try:
                    # 提取任务和结果信息
                    task = result["task"]
                    leader_id = int(task.person_id) if task.person_id.isdigit() else None
                    success = result["success"]
                    html_content = result.get("fetch_result", {}).get("html_content", "")

                    if success and html_content and leader_id:
                        # 将HTML内容更新到数据库
                        success = self.db_manager.update_html_content(leader_id, html_content)
                        if success:
                            with self.task_lock:
                                self.success_count += 1
                                logger.info(f"成功更新ID为 {leader_id} 的HTML内容")
                        else:
                            with self.task_lock:
                                self.failure_count += 1
                                logger.error(f"更新ID为 {leader_id} 的HTML内容失败")
                    else:
                        with self.task_lock:
                            self.failure_count += 1
                            if leader_id:
                                logger.error(f"处理ID为 {leader_id} 的记录失败，无法获取HTML内容")

                    # 处理计数器增加
                    save_counter += 1

                    # 定期输出进度信息
                    if save_counter >= self.save_interval:
                        logger.info(
                            f"爬取消费者 {consumer_id} 已处理 {save_counter} 个结果，成功: {self.success_count}, 失败: {self.failure_count}")
                        save_counter = 0

                except Exception as e:
                    logger.error(f"爬取消费者 {consumer_id} 处理结果时出错: {str(e)}")

                finally:
                    # 标记任务为完成
                    self.result_queue.task_done()

            except Exception as e:
                logger.error(f"爬取消费者 {consumer_id} 循环中出错: {str(e)}")

        logger.info(f"爬取消费者 {consumer_id} 结束工作")

    def only_fetch_producer(self, producer_id: int, tasks: List[ProcessorTask]):
        """
        仅爬取HTML的生产者函数

        Args:
            producer_id: 生产者ID
            tasks: 要处理的任务列表
        """
        logger.info(f"爬取生产者 {producer_id} 开始处理 {len(tasks)} 个任务")

        # 当前使用的代理和使用计数
        current_proxy = None

        # 创建一个任务队列，包含初始任务和失败后重新加入的任务
        task_queue = list(tasks)

        # 记录任务重试次数
        retry_counts = {}
        max_retries = self.max_retries

        while task_queue and not self.stop_event.is_set():
            # 从队列头部获取任务
            task = task_queue.pop(0)
            task_id = f"{task.person_id}_{task.idx}"

            # 初始化重试计数
            if task_id not in retry_counts:
                retry_counts[task_id] = 0

            # 如果需要代理但当前无代理，则等待获取新代理
            if self.proxy_pool and current_proxy is None:
                # 尝试获取新代理，如果失败则等待重试
                wait_time = 10
                logger.info(f"爬取生产者 {producer_id} 尝试获取新代理...")
                while current_proxy is None and not self.stop_event.is_set():
                    current_proxy = self.proxy_pool.get_proxy()
                    if current_proxy:
                        logger.info(f"爬取生产者 {producer_id} 获取了新代理")
                        break

                    # 代理获取失败，等待后重试
                    logger.warning(f"爬取生产者 {producer_id} 无法获取代理，等待 {wait_time} 秒后重试")
                    time.sleep(wait_time)
                    # 增加等待时间，避免频繁请求
                    wait_time = min(wait_time * 1.5, 60)  # 最长等待60秒

            # 更新代理使用计数
            if current_proxy:
                current_proxy['_usage_count'] = current_proxy.get('_usage_count', 0) + 1

            try:
                logger.info(
                    f"爬取生产者 {producer_id} 处理任务: {task.idx}, URL: {task.url}, 重试次数: {retry_counts[task_id]}")

                # 获取页面内容
                fetch_result = self.scraper.fetch_with_metadata(
                    url=task.url,
                    person_name=task.person_name,
                    person_id=task.person_id,
                    use_mobile=True,
                    provided_proxy=current_proxy  # 传递当前代理
                )

                # 检查获取是否成功
                if not fetch_result["success"]:
                    error_msg = fetch_result.get("error", "未知错误")

                    logger.warning(f"爬取生产者 {producer_id} 获取页面失败: {task.idx}, 错误: {error_msg}")

                    # 任务失败，无论什么原因都丢弃当前代理
                    if self.proxy_pool:
                        logger.warning(f"爬取生产者 {producer_id} 任务失败，丢弃当前代理并请求新代理")
                        current_proxy = None

                    # 增加重试计数
                    retry_counts[task_id] += 1

                    # 如果未达到最大重试次数，将任务添加回队列末尾
                    if retry_counts[task_id] < max_retries:
                        logger.info(
                            f"爬取生产者 {producer_id} 将任务 {task.idx} 添加到队列末尾以重试 ({retry_counts[task_id]}/{max_retries})")
                        task_queue.append(task)

                        # 等待一段随机时间后再次尝试
                        wait_time = random.uniform(2, 5)
                        logger.info(f"等待 {wait_time:.2f} 秒后重试")
                        time.sleep(wait_time)
                        continue
                    else:
                        # 达到最大重试次数，创建失败结果
                        logger.error(
                            f"爬取生产者 {producer_id} 任务 {task.idx} 已达到最大重试次数 {max_retries}，放弃处理")
                        result = {
                            "task": task,
                            "fetch_result": fetch_result,
                            "success": False,
                            "error": f"达到最大重试次数 {max_retries}: {error_msg}",
                            "timestamp": datetime.now().isoformat(),
                            "stage": "fetch"
                        }
                        self.result_queue.put(result)
                        continue

                # 获取成功，将HTML保存到文件
                result = {
                    "task": task,
                    "fetch_result": fetch_result,
                    "success": True,
                    "timestamp": datetime.now().isoformat(),
                    "stage": "fetch"
                }

                # 放入结果队列
                self.result_queue.put(result)
                logger.info(
                    f"爬取生产者 {producer_id} 成功处理任务: {task.idx}, 内容大小: {fetch_result.get('content_size', 0)} 字节")

            except Exception as e:
                logger.error(f"爬取生产者 {producer_id} 处理任务 {task.idx} 时出错: {str(e)}")

                # 任务异常，丢弃当前代理
                if self.proxy_pool:
                    logger.warning(f"爬取生产者 {producer_id} 任务异常，丢弃当前代理并请求新代理")
                    current_proxy = None

                # 增加重试计数
                retry_counts[task_id] += 1

                # 如果未达到最大重试次数，将任务添加回队列末尾
                if retry_counts[task_id] < max_retries:
                    logger.info(
                        f"爬取生产者 {producer_id} 将任务 {task.idx} 添加到队列末尾以重试 ({retry_counts[task_id]}/{max_retries})")
                    task_queue.append(task)
                else:
                    # 达到最大重试次数，创建失败结果
                    logger.error(f"爬取生产者 {producer_id} 任务 {task.idx} 已达到最大重试次数 {max_retries}，放弃处理")
                    result = {
                        "task": task,
                        "success": False,
                        "error": f"达到最大重试次数 {max_retries}: {str(e)}",
                        "timestamp": datetime.now().isoformat(),
                        "stage": "fetch"
                    }
                    self.result_queue.put(result)

            finally:
                # 只有当任务完成（成功或达到最大重试次数）时才更新处理计数
                if retry_counts[task_id] >= max_retries or fetch_result.get("success", False):
                    with self.task_lock:
                        self.processed_count += 1
                        if self.processed_count % 10 == 0 or self.processed_count == self.total_count:
                            progress = (self.processed_count / self.total_count) * 100
                            elapsed = time.time() - self.start_time
                            remaining = (elapsed / self.processed_count) * (
                                    self.total_count - self.processed_count) if self.processed_count > 0 else 0
                            logger.info(
                                f"爬取进度: {self.processed_count}/{self.total_count} ({progress:.2f}%), 已用时: {elapsed:.2f}秒, 预计剩余: {remaining:.2f}秒")