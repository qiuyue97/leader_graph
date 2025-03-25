import logging
import threading
import queue
import time
import random
import os
from typing import List, Dict, Any, Optional, Callable
import pandas as pd
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from proxy.pool import ProxyPool
from scraper.baike_scraper import BaikeScraper
from parser.baike_parser import BaikeParser
from utils.logger import get_logger
from utils.file_utils import ensure_dir
from utils.html_cache import HTMLCacheManager
from utils.db_utils import DBManager

# 获取日志器
logger = get_logger(__name__)


class ProcessStage(Enum):
    """处理阶段枚举"""
    FETCH = "fetch"  # 仅爬取HTML
    PARSE = "parse"  # 仅解析HTML
    FULL = "full"  # 完整处理(爬取+解析)


@dataclass
class ProcessorTask:
    """表示单个处理任务的数据类"""
    idx: int  # 在原始数据中的索引
    url: str  # 要爬取的URL
    person_name: str = ""  # 人物姓名
    person_id: str = ""  # 人物ID
    data: Dict[str, Any] = None  # 任务相关的其他数据
    cached: bool = False  # 是否已缓存
    html_path: str = ""  # HTML文件路径

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
                 output_dir: str = './data/person_data',
                 save_interval: int = 10,
                 min_content_size: int = 1024):
        """
        初始化数据处理器

        Args:
            config: 配置对象，包含数据库配置
            proxy_pool: 代理池实例
            num_producers: 生产者线程数
            num_consumers: 消费者线程数
            output_dir: 输出目录
            save_interval: 自动保存间隔（处理多少条记录后保存一次）
        """
        self.db_manager = DBManager(config.db_config)
        self.proxy_pool = proxy_pool
        self.num_producers = num_producers
        self.num_consumers = num_consumers
        self.output_dir = output_dir
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

        # HTML缓存管理器
        self.html_cache = HTMLCacheManager(output_dir)

        # 确保输出目录存在
        ensure_dir(self.output_dir)

        # 创建爬虫和解析器
        self.scraper = BaikeScraper(
            proxy_pool=proxy_pool,
            output_dir=output_dir,
            min_content_size=min_content_size
        )
        self.parser = BaikeParser()

        logger.info(
            f"数据处理器初始化完成，生产者:{num_producers}，消费者:{num_consumers}，最小内容大小:{min_content_size}字节")

    def _reorder_columns(self, df):
        """
        重新排列DataFrame的列，确保特定的元数据列位于最后

        Args:
            df: 原始DataFrame

        Returns:
            重新排序后的DataFrame
        """
        meta_columns = ['html_cached', 'html_path', 'parsed']
        other_columns = [col for col in df.columns if col not in meta_columns]
        return df[other_columns + meta_columns]

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
                    data=dict(record),
                    cached=False
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
            tasks = self.load_tasks_from_db()
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
                        # 再次检查数据库中是否已有内容（可能其他线程已经处理过）
                        if not self.db_manager.check_html_exists(leader_id):
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
                            logger.info(f"跳过更新ID为 {leader_id} 的记录，因为已有HTML内容")
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

    def only_parse_producer(self, producer_id: int, tasks: List[ProcessorTask]):
        """
        仅解析HTML的生产者函数

        Args:
            producer_id: 生产者ID
            tasks: 要处理的任务列表
        """
        logger.info(f"解析生产者 {producer_id} 开始处理 {len(tasks)} 个任务")

        # 创建解析器
        parser = BaikeParser()

        for task in tasks:
            if self.stop_event.is_set():
                logger.info(f"解析生产者 {producer_id} 收到停止信号")
                break

            # 确保任务有HTML文件
            if not task.cached or not task.html_path or not os.path.exists(task.html_path):
                logger.warning(f"解析生产者 {producer_id} 跳过无缓存任务: {task.idx}")
                continue

            try:
                logger.info(f"解析生产者 {producer_id} 处理任务: {task.idx}, HTML: {task.html_path}")

                # 读取HTML内容
                html_content = None
                try:
                    with open(task.html_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()
                except Exception as e:
                    logger.error(f"解析生产者 {producer_id} 读取HTML文件失败: {task.html_path}, 错误: {str(e)}")
                    result = {
                        "task": task,
                        "success": False,
                        "error": f"读取HTML文件失败: {str(e)}",
                        "timestamp": datetime.now().isoformat(),
                        "stage": "parse"
                    }
                    self.result_queue.put(result)
                    continue

                # 解析HTML内容
                if html_content:
                    parse_result = parser.parse_page(html_content)

                    # 检查解析是否成功
                    if parse_result["success"]:
                        result = {
                            "task": task,
                            "parse_result": parse_result,
                            "success": True,
                            "timestamp": datetime.now().isoformat(),
                            "stage": "parse"
                        }
                        logger.info(f"解析生产者 {producer_id} 成功解析任务: {task.idx}")
                    else:
                        error_msg = parse_result.get("error", "解析失败，未知原因")
                        logger.warning(f"解析生产者 {producer_id} 解析失败: {task.idx}, 错误: {error_msg}")
                        result = {
                            "task": task,
                            "parse_result": parse_result,
                            "success": False,
                            "error": error_msg,
                            "timestamp": datetime.now().isoformat(),
                            "stage": "parse"
                        }

                    # 放入结果队列
                    self.result_queue.put(result)

            except Exception as e:
                logger.error(f"解析生产者 {producer_id} 处理任务 {task.idx} 时出错: {str(e)}")
                result = {
                    "task": task,
                    "success": False,
                    "error": f"解析异常: {str(e)}",
                    "timestamp": datetime.now().isoformat(),
                    "stage": "parse"
                }
                self.result_queue.put(result)

            finally:
                # 更新处理计数
                with self.task_lock:
                    self.processed_count += 1
                    if self.processed_count % 10 == 0 or self.processed_count == self.total_count:
                        progress = (self.processed_count / self.total_count) * 100
                        elapsed = time.time() - self.start_time
                        remaining = (elapsed / self.processed_count) * (
                                self.total_count - self.processed_count) if self.processed_count > 0 else 0
                        logger.info(
                            f"解析进度: {self.processed_count}/{self.total_count} ({progress:.2f}%), 已用时: {elapsed:.2f}秒, 预计剩余: {remaining:.2f}秒")

                # 控制处理速度
                time.sleep(random.uniform(0.1, 0.3))  # 解析无需太多延迟

        logger.info(f"解析生产者 {producer_id} 已完成所有任务")

    def parse_consumer(self, consumer_id: int):
        """
        解析阶段的消费者函数，处理解析结果并更新CSV文件

        Args:
            consumer_id: 消费者ID
        """
        logger.info(f"解析消费者 {consumer_id} 开始工作")

        # 读取原始数据
        original_df = pd.read_csv(self.input_csv_path, encoding='utf-8-sig')

        save_counter = 0  # 记录处理了多少条结果

        while not (self.stop_event.is_set() and self.result_queue.empty()):
            try:
                # 从结果队列获取项目，最多等待3秒
                try:
                    result = self.result_queue.get(timeout=3)
                except queue.Empty:
                    if self.stop_event.is_set():
                        logger.info(f"解析消费者 {consumer_id} 队列为空且收到停止信号，退出")
                        break
                    continue

                try:
                    # 提取任务和结果信息
                    task = result["task"]
                    idx = task.idx
                    success = result["success"]
                    stage = result.get("stage", "parse")

                    # 确保是解析阶段的结果
                    if stage != "parse":
                        logger.warning(f"解析消费者 {consumer_id} 收到非解析阶段结果: {stage}")
                        continue

                    # 更新DataFrame中对应行的数据
                    # 更新DataFrame中对应行的数据
                    with self.result_lock:
                        if success and "parse_result" in result and result["parse_result"]:
                            parse_result = result["parse_result"]
                            original_df.at[idx, 'person_title'] = parse_result.get('title', '')
                            original_df.at[idx, 'person_bio_raw'] = '\n'.join(parse_result.get('career_info', []))

                            # 可以添加更多字段
                            if 'person_summary' not in original_df.columns:
                                original_df['person_summary'] = ''
                            original_df.at[idx, 'person_summary'] = parse_result.get('summary', '')

                            # 添加新的人物详细信息字段
                            person_details = parse_result.get('person_details', {})

                            # 确保所有需要的列都存在
                            for col in ['ethnicity', 'native_place', 'birth_date', 'alma_mater', 'political_status']:
                                if col not in original_df.columns:
                                    original_df[col] = ''
                                original_df.at[idx, col] = person_details.get(col, '')

                            # 标记为已解析
                            original_df.at[idx, 'parsed'] = 'Y'

                            with self.task_lock:
                                self.success_count += 1
                        else:
                            # 记录失败
                            original_df.at[idx, 'parsed'] = 'N'
                            with self.task_lock:
                                self.failure_count += 1

                        # 处理计数器增加
                        save_counter += 1

                        # 定期保存到CSV
                        if save_counter >= self.save_interval or self.result_queue.empty():
                            original_df = self._reorder_columns(original_df)
                            original_df.to_csv(self.input_csv_path, index=False, encoding='utf-8-sig')
                            logger.info(
                                f"解析消费者 {consumer_id} 已保存更新到CSV，成功: {self.success_count}, 失败: {self.failure_count}")
                            save_counter = 0

                    logger.info(f"解析消费者 {consumer_id} 已处理结果: {idx}, 成功: {success}")

                except Exception as e:
                    logger.error(f"解析消费者 {consumer_id} 处理结果时出错: {str(e)}")

                finally:
                    # 标记任务为完成
                    self.result_queue.task_done()

            except Exception as e:
                logger.error(f"解析消费者 {consumer_id} 循环中出错: {str(e)}")

        # 确保最后保存一次
        with self.result_lock:
            original_df = self._reorder_columns(original_df)
            original_df.to_csv(self.input_csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"解析消费者 {consumer_id} 最终保存，成功: {self.success_count}, 失败: {self.failure_count}")

        logger.info(f"解析消费者 {consumer_id} 结束工作")

    def process_parse_stage(self, filter_func: Optional[Callable[[pd.Series], bool]] = None):
        """
        执行解析阶段，仅解析已缓存的HTML文件

        Args:
            filter_func: 过滤函数，决定哪些行需要处理
        """
        try:
            self.start_time = time.time()

            # 加载任务
            all_tasks = self.load_tasks_from_csv(filter_func)
            if not all_tasks:
                logger.warning("没有任务需要处理")
                return

            # 过滤出已缓存但未解析的任务
            tasks = [task for task in all_tasks if task.cached and task.data.get('parsed') != 'Y']
            if not tasks:
                logger.info("没有需要解析的任务")
                return

            self.total_count = len(tasks)
            logger.info(f"准备解析 {self.total_count} 个任务")

            # 这里继续实现解析阶段的处理逻辑
            # 分割任务
            task_chunks = self.split_tasks(tasks)

            # 启动消费者线程
            consumers = []
            logger.info(f"启动 {self.num_consumers} 个消费者线程")
            for i in range(self.num_consumers):
                t = threading.Thread(target=self.parse_consumer, args=(i,), daemon=True)
                t.start()
                consumers.append(t)

            # 启动生产者线程
            producers = []
            for i in range(self.num_producers):
                # 每个生产者负责处理一部分任务
                producer_tasks = task_chunks[i] if i < len(task_chunks) else []
                t = threading.Thread(target=self.only_parse_producer, args=(i, producer_tasks), daemon=True)
                t.start()
                producers.append(t)
                logger.info(f"解析生产者 {i} 已启动")

            # 等待所有生产者完成
            for p in producers:
                p.join()

            logger.info("所有解析生产者已完成工作")

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

            logger.info("所有解析消费者已完成工作")
            logger.info(
                f"HTML解析阶段完成，共处理 {self.processed_count} 条记录，成功: {self.success_count}, 失败: {self.failure_count}")
            logger.info(f"总耗时: {elapsed_time:.2f} 秒")

        except Exception as e:
            logger.error(f"解析数据时出现未捕获的异常: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

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

            # 如果任务已缓存，跳过
            if task.cached or os.path.exists(self.html_cache.get_cache_path(task.person_name, task.person_id)):
                logger.info(f"爬取生产者 {producer_id} 跳过已缓存任务: {task.idx}")
                continue

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
                    content_size = fetch_result.get("content_size", 0)

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
                    "stage": "fetch",
                    "html_path": fetch_result.get("saved_file", "")
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