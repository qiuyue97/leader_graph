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

from proxy.pool import ProxyPool
from scraper.baike_scraper import BaikeScraper
from parser.baike_parser import BaikeParser
from utils.logger import get_logger
from utils.file_utils import ensure_dir

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
    """

    def __init__(self, input_csv_path: str, proxy_pool: Optional[ProxyPool] = None,
                 num_producers: int = 3, num_consumers: int = 2,
                 output_dir: str = './person_data',
                 save_interval: int = 10):
        """
        初始化数据处理器

        Args:
            input_csv_path: 输入CSV文件路径
            proxy_pool: 代理池实例
            num_producers: 生产者线程数
            num_consumers: 消费者线程数
            output_dir: 输出目录
            save_interval: 自动保存间隔（处理多少条记录后保存一次）
        """
        self.input_csv_path = input_csv_path
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

        # 确保输出目录存在
        ensure_dir(self.output_dir)

        # 创建爬虫和解析器
        self.scraper = BaikeScraper(proxy_pool=proxy_pool, output_dir=output_dir)
        self.parser = BaikeParser()

        logger.info(f"数据处理器初始化完成，生产者:{num_producers}，消费者:{num_consumers}")

    def load_tasks_from_csv(self, filter_func: Optional[Callable[[pd.Series], bool]] = None) -> List[ProcessorTask]:
        """
        从CSV文件加载任务

        Args:
            filter_func: 过滤函数，决定哪些行需要处理

        Returns:
            任务列表
        """
        try:
            logger.info(f"从 {self.input_csv_path} 加载数据")
            df = pd.read_csv(self.input_csv_path, encoding='utf-8-sig')

            # 检查并添加缺失的列
            for col in ['person_title', 'person_bio_raw']:
                if col not in df.columns:
                    logger.info(f"CSV中未找到'{col}'列，正在创建...")
                    df[col] = ''

            # 保存修改后的DataFrame
            df.to_csv(self.input_csv_path, index=False, encoding='utf-8-sig')

            # 创建任务列表
            tasks = []
            for idx, row in df.iterrows():
                # 如果提供了过滤函数，使用它决定是否处理此行
                if filter_func is not None and not filter_func(row):
                    continue

                url = row.get('person_url', '').strip()
                if url:  # 确保有URL
                    task = ProcessorTask(
                        idx=idx,
                        url=url,
                        person_name=str(row.get('person_name', '')).strip(),
                        person_id=str(row.get('person_id', '')).strip(),
                        data=dict(row)
                    )
                    tasks.append(task)

            logger.info(f"共加载 {len(tasks)} 个任务")
            return tasks

        except Exception as e:
            logger.error(f"加载CSV文件时出错: {str(e)}")
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

    def producer(self, producer_id: int, tasks: List[ProcessorTask]):
        """
        生产者函数，负责爬取数据并将结果放入队列
        失败的任务不会被丢弃，而是将其放回队列末尾重试

        Args:
            producer_id: 生产者ID
            tasks: 要处理的任务列表
        """
        logger.info(f"生产者 {producer_id} 开始处理 {len(tasks)} 个任务")

        # 当前使用的代理和使用计数
        current_proxy = None

        # 创建一个任务队列，包含初始任务和失败后重新加入的任务
        task_queue = list(tasks)

        # 记录任务重试次数
        retry_counts = {}
        max_retries = 3  # 每个任务最大重试次数

        while task_queue and not self.stop_event.is_set():
            # 从队列头部获取任务
            task = task_queue.pop(0)
            task_id = f"{task.person_id}_{task.idx}"

            # 初始化重试计数
            if task_id not in retry_counts:
                retry_counts[task_id] = 0

            try:
                logger.info(
                    f"生产者 {producer_id} 处理任务: {task.idx}, URL: {task.url}, 重试次数: {retry_counts[task_id]}")

                # 检查是否需要获取新代理
                if self.proxy_pool and (current_proxy is None or current_proxy.get('_usage_count', 0) >= 5):
                    # 尝试获取新代理，如果失败则等待重试
                    while current_proxy is None and not self.stop_event.is_set():
                        current_proxy = self.proxy_pool.get_proxy()
                        if current_proxy:
                            logger.info(f"生产者 {producer_id} 获取了新代理")
                            break

                        # 代理获取失败，等待后重试
                        wait_time = 10
                        logger.warning(f"生产者 {producer_id} 无法获取代理，等待 {wait_time} 秒后重试")
                        time.sleep(wait_time)

                # 更新代理使用计数
                if current_proxy:
                    current_proxy['_usage_count'] = current_proxy.get('_usage_count', 0) + 1
                    logger.debug(f"生产者 {producer_id} 代理使用计数: {current_proxy['_usage_count']}")

                # 获取页面内容
                fetch_result = self.scraper.fetch_with_metadata(
                    url=task.url,
                    person_name=task.person_name,
                    person_id=task.person_id,
                    use_mobile=True,
                    provided_proxy=current_proxy  # 传递当前代理
                )

                if fetch_result["success"]:
                    # 解析内容
                    html_content = fetch_result["html_content"]
                    parse_result = self.parser.parse_page(html_content)

                    # 将抓取和解析结果合并
                    result = {
                        "task": task,
                        "fetch_result": fetch_result,
                        "parse_result": parse_result,
                        "success": parse_result["success"],
                        "timestamp": datetime.now().isoformat()
                    }

                    # 放入结果队列
                    self.result_queue.put(result)
                    logger.info(f"生产者 {producer_id} 成功处理任务: {task.idx}")
                else:
                    error_msg = fetch_result.get("error", "未知错误")
                    logger.warning(f"生产者 {producer_id} 获取页面失败: {task.idx}, 错误: {error_msg}")

                    # 如果获取失败且使用了代理，可以考虑废弃此代理
                    if current_proxy and fetch_result.get("proxy_error", False):
                        logger.warning(f"生产者 {producer_id} 废弃可能失效的代理")
                        current_proxy = None

                    # 增加重试计数
                    retry_counts[task_id] += 1

                    # 如果未达到最大重试次数，将任务添加回队列末尾
                    if retry_counts[task_id] < max_retries:
                        logger.info(
                            f"生产者 {producer_id} 将任务 {task.idx} 添加到队列末尾以重试 ({retry_counts[task_id]}/{max_retries})")
                        task_queue.append(task)
                    else:
                        # 达到最大重试次数，创建失败结果
                        logger.error(f"生产者 {producer_id} 任务 {task.idx} 已达到最大重试次数 {max_retries}，放弃处理")
                        result = {
                            "task": task,
                            "fetch_result": fetch_result,
                            "parse_result": None,
                            "success": False,
                            "error": f"达到最大重试次数 {max_retries}: {error_msg}",
                            "timestamp": datetime.now().isoformat()
                        }
                        self.result_queue.put(result)

            except Exception as e:
                logger.error(f"生产者 {producer_id} 处理任务 {task.idx} 时出错: {str(e)}")

                # 增加重试计数
                retry_counts[task_id] += 1

                # 如果未达到最大重试次数，将任务添加回队列末尾
                if retry_counts[task_id] < max_retries:
                    logger.info(
                        f"生产者 {producer_id} 将任务 {task.idx} 添加到队列末尾以重试 ({retry_counts[task_id]}/{max_retries})")
                    task_queue.append(task)
                else:
                    # 达到最大重试次数，创建失败结果
                    logger.error(f"生产者 {producer_id} 任务 {task.idx} 已达到最大重试次数 {max_retries}，放弃处理")
                    result = {
                        "task": task,
                        "success": False,
                        "error": f"达到最大重试次数 {max_retries}: {str(e)}",
                        "timestamp": datetime.now().isoformat()
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
                                f"进度: {self.processed_count}/{self.total_count} ({progress:.2f}%), 已用时: {elapsed:.2f}秒, 预计剩余: {remaining:.2f}秒")

                # 控制请求频率
                time.sleep(random.uniform(1, 3))

        # 报告未完成的任务数量
        if task_queue:
            logger.warning(f"生产者 {producer_id} 因停止信号而退出，还有 {len(task_queue)} 个任务未完成")
        else:
            logger.info(f"生产者 {producer_id} 已完成所有任务")

    def consumer(self, consumer_id: int):
        """
        消费者函数，从队列获取结果并更新原始CSV文件

        Args:
            consumer_id: 消费者ID
        """
        logger.info(f"消费者 {consumer_id} 开始工作")

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
                        logger.info(f"消费者 {consumer_id} 队列为空且收到停止信号，退出")
                        break
                    continue

                try:
                    # 提取任务和结果信息
                    task = result["task"]
                    idx = task.idx
                    success = result["success"]

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

                            with self.task_lock:
                                self.success_count += 1
                        else:
                            # 记录失败
                            with self.task_lock:
                                self.failure_count += 1

                        # 处理计数器增加
                        save_counter += 1

                        # 定期保存到CSV
                        if save_counter >= self.save_interval or self.result_queue.empty():
                            original_df.to_csv(self.input_csv_path, index=False, encoding='utf-8-sig')
                            logger.info(
                                f"消费者 {consumer_id} 已保存更新到CSV，成功: {self.success_count}, 失败: {self.failure_count}")
                            save_counter = 0

                    logger.info(f"消费者 {consumer_id} 已处理结果: {idx}, 成功: {success}")

                except Exception as e:
                    logger.error(f"消费者 {consumer_id} 处理结果时出错: {str(e)}")

                finally:
                    # 标记任务为完成
                    self.result_queue.task_done()

            except Exception as e:
                logger.error(f"消费者 {consumer_id} 循环中出错: {str(e)}")

        # 确保最后保存一次
        with self.result_lock:
            original_df.to_csv(self.input_csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"消费者 {consumer_id} 最终保存，成功: {self.success_count}, 失败: {self.failure_count}")

        logger.info(f"消费者 {consumer_id} 结束工作")

    def process_data(self, filter_func: Optional[Callable[[pd.Series], bool]] = None):
        """
        处理数据的主函数

        Args:
            filter_func: 过滤函数，决定哪些行需要处理
        """
        try:
            self.start_time = time.time()

            # 加载任务
            tasks = self.load_tasks_from_csv(filter_func)
            if not tasks:
                logger.warning("没有任务需要处理")
                return

            self.total_count = len(tasks)
            logger.info(f"准备处理 {self.total_count} 个任务")

            # 分割任务
            task_chunks = self.split_tasks(tasks)

            # 启动消费者线程
            consumers = []
            logger.info(f"启动 {self.num_consumers} 个消费者线程")
            for i in range(self.num_consumers):
                t = threading.Thread(target=self.consumer, args=(i,), daemon=True)
                t.start()
                consumers.append(t)

            # 等待代理池初始化（如果有）
            if self.proxy_pool:
                logger.info("等待代理池准备就绪...")
                time.sleep(3)

            # 启动生产者线程
            producers = []
            for i in range(self.num_producers):
                # 每个生产者负责处理一部分任务
                producer_tasks = task_chunks[i] if i < len(task_chunks) else []
                t = threading.Thread(target=self.producer, args=(i, producer_tasks), daemon=True)
                t.start()
                producers.append(t)
                logger.info(f"生产者 {i} 已启动")

                # 稍微延迟启动，避免同时争抢代理
                time.sleep(0.5)

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
                f"数据处理完成，共处理 {self.processed_count} 条记录，成功: {self.success_count}, 失败: {self.failure_count}, 重试: {self.retry_count}")
            logger.info(f"总耗时: {elapsed_time:.2f} 秒")

        except Exception as e:
            logger.error(f"处理数据时出现未捕获的异常: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    @staticmethod
    def default_filter(row: pd.Series) -> bool:
        """
        默认的过滤函数，检查行是否需要处理

        Args:
            row: DataFrame中的一行

        Returns:
            是否需要处理此行
        """
        # 如果没有标题或没有履历原始信息，则需要处理
        if (pd.isna(row.get('person_title')) or not row.get('person_title', '').strip() or
                pd.isna(row.get('person_bio_raw')) or not row.get('person_bio_raw', '').strip()):
            # 确保有URL
            if row.get('person_url', '').strip():
                return True
        return False