import os
import time
import logging
import threading
import json
import random
from typing import Dict, Any, List, Optional, Tuple
from openai import AzureOpenAI
import openai
import pymysql
from concurrent.futures import ThreadPoolExecutor
import sys

from config.settings import Config

# 导入数据模型
from leader.schema import BiographicalEvents

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("./logs/bio_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("bio_processor")


class TokenCostTracker:
    """跟踪token使用和成本的类"""

    def __init__(self,
                 input_price_per_1m: float = 2.50,
                 cached_input_price_per_1m: float = 1.25,
                 output_price_per_1m: float = 10.0,
                 cost_limit: Optional[float] = None):
        """
        初始化token成本追踪器

        Args:
            input_price_per_1m: 输入token每1,000,000个的价格（美元）
            cached_input_price_per_1m: 缓存输入token每1,000,000个的价格（美元）
            output_price_per_1m: 输出token每1,000,000个的价格（美元）
        """
        self.input_price_per_1m = input_price_per_1m
        self.cached_input_price_per_1m = cached_input_price_per_1m
        self.output_price_per_1m = output_price_per_1m

        # 统计信息
        self.total_input_tokens = 0
        self.total_cached_input_tokens = 0  # 未实现，因为API响应中未提供此信息
        self.total_output_tokens = 0
        self.total_tokens = 0

        # 成本统计
        self.total_input_cost = 0.0
        self.total_cached_input_cost = 0.0
        self.total_output_cost = 0.0
        self.total_cost = 0.0

        # 成本限制
        self.cost_limit = cost_limit
        self.limit_reached = False

        # 线程安全的锁
        self.stats_lock = threading.Lock()

    def check_cost_limit_reached(self) -> bool:
        """
        检查是否达到API调用成本限制

        Returns:
            是否达到限制
        """
        if self.cost_limit is None:
            return False

        if self.total_cost >= self.cost_limit:
            if not self.limit_reached:
                logger.warning(f"已达到成本限制 (${self.cost_limit:.2f})，将停止处理")
                self.limit_reached = True
            return True
        return False

    def update_from_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """
        从API响应中更新token使用统计

        Args:
            response: Azure OpenAI API的响应

        Returns:
            包含此次请求token使用和成本的字典
        """
        try:
            # 提取token使用信息 - 适配实际Azure OpenAI API响应格式
            usage = response.get("usage", {})
            if not usage:
                logger.warning("API响应中没有找到token使用信息")
                return {}

            # 从实际响应格式中提取token使用信息
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            total_tokens = usage.get("total_tokens", 0)

            # 检查是否有缓存token信息
            prompt_tokens_details = usage.get("prompt_tokens_details", {})
            cached_tokens = prompt_tokens_details.get("cached_tokens", 0)

            # 实际输入token = 总输入token - 缓存token
            actual_input_tokens = input_tokens - cached_tokens

            # 计算成本 - 正确使用每百万tokens的价格
            input_cost = (actual_input_tokens / 1000000.0) * self.input_price_per_1m
            cached_cost = (cached_tokens / 1000000.0) * self.cached_input_price_per_1m
            output_cost = (output_tokens / 1000000.0) * self.output_price_per_1m
            total_cost = input_cost + cached_cost + output_cost

            # 使用示例：1096输入tokens，0缓存tokens，338输出tokens
            # 输入成本: (1096/1000000) * $2.50 = $0.00274
            # 输出成本: (338/1000000) * $10.00 = $0.00338
            # 总成本: $0.00274 + $0.00338 = $0.00612

            # 更新全局统计
            with self.stats_lock:
                self.total_input_tokens += actual_input_tokens
                self.total_cached_input_tokens += cached_tokens
                self.total_output_tokens += output_tokens
                self.total_tokens += total_tokens

                self.total_input_cost += input_cost
                self.total_cached_input_cost += cached_cost
                self.total_output_cost += output_cost
                self.total_cost += total_cost
                self.check_cost_limit_reached()

            # 返回此次请求的统计
            return {
                "input_tokens": actual_input_tokens,
                "cached_tokens": cached_tokens,
                "output_tokens": output_tokens,
                "total_tokens": total_tokens,
                "input_cost": input_cost,
                "cached_cost": cached_cost,
                "output_cost": output_cost,
                "total_cost": total_cost
            }

        except Exception as e:
            logger.error(f"计算token成本时出错: {str(e)}")
            return {}

    def get_stats(self) -> Dict[str, Any]:
        """
        获取累积的token使用和成本统计

        Returns:
            包含累积统计信息的字典
        """
        with self.stats_lock:
            return {
                "total_input_tokens": self.total_input_tokens,
                "total_cached_input_tokens": self.total_cached_input_tokens,
                "total_output_tokens": self.total_output_tokens,
                "total_tokens": self.total_tokens,
                "total_input_cost": self.total_input_cost,
                "total_cached_input_cost": self.total_cached_input_cost,
                "total_output_cost": self.total_output_cost,
                "total_cost": self.total_cost
            }

    def log_stats(self) -> None:
        """记录当前累积的token使用和成本统计"""
        stats = self.get_stats()
        logger.info(
            f"累积Token使用统计: 输入={stats['total_input_tokens']}, 缓存输入={stats['total_cached_input_tokens']}, 输出={stats['total_output_tokens']}, 总计={stats['total_tokens']}")
        logger.info(
            f"累积Token成本统计: 输入=${stats['total_input_cost']:.2f}, 缓存输入=${stats['total_cached_input_cost']:.2f}, 输出=${stats['total_output_cost']:.2f}, 总计=${stats['total_cost']:.2f}")


class BiographicalDataProcessor:
    """处理人物履历数据的类"""

    def __init__(self,
                 azure_endpoint: str,
                 api_key: str,
                 api_version: str = "2024-10-21",
                 db_config: Dict[str, str] = None,
                 max_threads: int = 10,
                 request_rate: int = 8,  # 每秒请求数，低于限制
                 token_limit: int = 90000,  # 每分钟令牌数，留出余量
                 input_price_per_1m: float = 2.50,
                 cached_input_price_per_1m: float = 1.25,
                 output_price_per_1m: float = 10.0,
                 cost_limit: Optional[float] = None):
        """
        初始化处理器

        Args:
            azure_endpoint: Azure OpenAI的端点URL
            api_key: Azure OpenAI的API密钥
            api_version: API版本
            db_config: 数据库配置，包含host、user、password、database
            max_threads: 最大线程数，默认为10
            request_rate: 每秒最大请求数，默认为8
            token_limit: 每分钟最大令牌数，默认为90000
            input_price_per_1m: 输入token每1,000,000个的价格（美元）
            cached_input_price_per_1m: 缓存输入token每1,000,000个的价格（美元）
            output_price_per_1m: 输出token每1,000,000个的价格（美元）
        """
        self.azure_endpoint = azure_endpoint
        self.api_key = api_key
        self.api_version = api_version
        self.db_config = db_config or {}

        # 每个线程使用独立的客户端实例
        self.clients = {}

        # 多线程和速率限制相关设置
        self.max_threads = max_threads
        self.request_rate = request_rate
        self.token_limit = token_limit

        # 用于速率控制的锁和计数器
        self.request_lock = threading.Lock()
        self.request_count = 0
        self.request_reset_time = time.time() + 60  # 60秒后重置
        self.last_request_time = time.time()

        # 处理结果统计
        self.stats_lock = threading.Lock()
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0

        # 初始化token成本追踪器
        self.token_tracker = TokenCostTracker(
            input_price_per_1m=input_price_per_1m,
            cached_input_price_per_1m=cached_input_price_per_1m,
            output_price_per_1m=output_price_per_1m,
            cost_limit=cost_limit
        )

        logger.info(
            f"初始化完成，使用{max_threads}个线程，Token价格配置：输入=${input_price_per_1m}/1M，缓存输入=${cached_input_price_per_1m}/1M，输出=${output_price_per_1m}/1M")

    def get_database_connection(self):
        """创建MySQL数据库连接"""
        try:
            connection = pymysql.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info(f"成功连接到数据库: {self.db_config['database']}")
            return connection
        except Exception as e:
            logger.error(f"连接数据库时出错: {e}")
            return None

    def close_database_connection(self, connection):
        """关闭数据库连接"""
        if connection:
            connection.close()
            logger.info("数据库连接已关闭")

    def check_career_history_structured_column(self):
        """检查career_history_structured字段是否存在，不存在则创建"""
        conn = self.get_database_connection()
        if not conn:
            return False

        try:
            with conn.cursor() as cursor:
                # 检查字段是否存在
                cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = 'c_org_leader_info' 
                AND column_name = 'career_history_structured'
                """, (self.db_config['database'],))

                if not cursor.fetchone():
                    # 字段不存在，创建字段
                    cursor.execute("""
                    ALTER TABLE c_org_leader_info 
                    ADD COLUMN career_history_structured TEXT COMMENT '结构化的履历数据（JSON格式）'
                    """)
                    conn.commit()
                    logger.info("成功创建 career_history_structured 字段")
                else:
                    logger.info("career_history_structured 字段已存在")
                return True

        except Exception as e:
            logger.error(f"检查/创建字段时出错: {e}")
            return False
        finally:
            self.close_database_connection(conn)

    def get_client(self):
        """为当前线程获取或创建OpenAI客户端"""
        thread_id = threading.get_ident()
        if thread_id not in self.clients:
            self.clients[thread_id] = AzureOpenAI(
                azure_endpoint=self.azure_endpoint,
                api_key=self.api_key,
                api_version=self.api_version
            )
        return self.clients[thread_id]

    def _wait_for_rate_limit(self):
        """等待以符合速率限制"""
        with self.request_lock:
            # 确保请求间隔符合每秒请求限制
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < 1.0 / self.request_rate:
                time.sleep(1.0 / self.request_rate - time_since_last)

            # 检查分钟级别的限制
            if current_time > self.request_reset_time:
                # 重置计数器
                self.request_count = 0
                self.request_reset_time = current_time + 60

            # 如果接近令牌限制，等待重置
            if self.request_count >= self.request_rate * 60:
                wait_time = self.request_reset_time - current_time
                logger.warning(f"接近速率限制，等待 {wait_time:.2f} 秒")
                time.sleep(wait_time)
                self.request_count = 0
                self.request_reset_time = time.time() + 60

            # 更新计数和时间
            self.request_count += 1
            self.last_request_time = time.time()

    def extract_biographical_events(self, bio_text: str) -> Dict[str, Any]:
        """
        提取文本中的人物履历信息

        Args:
            bio_text: 包含人物履历的文本

        Returns:
            Dict: 结构化的人物履历信息
        """
        if self.token_tracker.limit_reached:
            logger.warning("已达到成本限制，跳过API调用")
            return {"events": []}

        # 等待速率限制
        self._wait_for_rate_limit()

        # 获取当前线程的客户端
        client = self.get_client()

        # 创建详细的系统提示
        system_prompt = """
        你是一个能够提取人物履历信息的助手。请使用提供的工具结构化地返回信息。

        请严格遵循以下规范：
        1. 年份字段(startYear, endYear)必须在1900-2100之间
        2. 月份字段(startMonth, endMonth)必须在1-12之间

        学习经历规范：
        3. 学习经历(eventType="study")必须有school字段，该字段仅包含学校名称（不含学院信息）
        4. 当学校名称中包含院系信息时（如"北京大学计算机学院"），必须将其拆分：
           - school字段应只保留大学名称（如"北京大学"）
           - department字段应存放院系名称（如"计算机学院"）
        5. 学习经历中的place和position必须为null
        6. 学习经历可以包含department(院系)、major(专业)和degree(学位)字段

        工作经历规范：
        7. 工作经历(eventType="work")必须有place和position字段
        8. 工作经历中的school、department、major和degree必须为null

        其他规范：
        9. 当isEnd和hasEndDate都为true时，endYear字段必须有值
        10. 请正确区分学习经历和工作经历，包含"学生"、"学习"、"专业"、"学院"、"系"等内容的通常是学习经历
        11. 如果事件未明确结束年月，但已有后续事件，则设置isEnd=true，并根据后续事件的开始时间推断该事件的结束时间
        12. 对于最新事件，如果描述中含有"至今"、"现在"等词汇，则设置isEnd=false，hasEndDate=false

        特别注意：
        - 当遇到形如"XX大学XX学院"或"XX大学XX系"的表述时，务必将大学名称和院系名称分开存储
        - 以下是一些常见错误的示例：
          错误：school="对外经济贸易大学工商管理学院", department=null
          正确：school="对外经济贸易大学", department="工商管理学院"

          错误：school="美国金门大学洛杉矶税收管理学院", department=null
          正确：school="美国金门大学", department="洛杉矶税收管理学院"
        - 当设置isEnd=true和hasEndDate=true时，必须同时提供endYear值，否则数据将无法通过验证
        - 如果无法确定结束年份，但确定已结束，应设置isEnd=true, hasEndDate=false

        请确保每个字段严格符合上述规范，这对于数据验证非常重要。
        """

        # 准备工具和消息
        tools = [openai.pydantic_function_tool(BiographicalEvents)]

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": bio_text}
        ]

        try:
            # 为避免所有线程同时失败，添加随机退避时间
            retry_count = 0
            max_retries = 3

            while retry_count < max_retries:
                try:
                    # 调用API
                    logger.info(f"线程 {threading.get_ident()} 正在调用Azure OpenAI API...")
                    response = client.chat.completions.create(
                        model="gpt-4o",  # 替换为您的模型部署名称
                        messages=messages,
                        tools=tools,
                        parallel_tool_calls=False  # 使用结构化输出时需要设置为False
                        # 移除了不兼容的 response_format 参数
                    )

                    # 获取完整的响应对象（包含token使用情况）
                    full_response = response.model_dump()

                    # 计算并记录token使用成本
                    token_stats = self.token_tracker.update_from_response(full_response)
                    if token_stats:
                        logger.info(f"本次API调用token使用: 输入={token_stats['input_tokens']}, "
                                    f"缓存={token_stats['cached_tokens']}, "
                                    f"输出={token_stats['output_tokens']}, "
                                    f"成本=${token_stats['total_cost']:.4f}")

                    if token_stats and self.token_tracker.limit_reached:
                        logger.warning(
                            f"该请求后已达到成本限制(${self.token_tracker.total_cost:.2f}/${self.token_tracker.cost_limit:.2f})，将在处理完当前任务后停止")

                    # 解析返回结果
                    if response.choices and response.choices[0].message.tool_calls:
                        tool_call = response.choices[0].message.tool_calls[0]
                        result_json = json.loads(tool_call.function.arguments)
                        logger.info(f"线程 {threading.get_ident()} 成功获取结构化数据")

                        # 验证处理后的数据
                        try:
                            # 使用Pydantic模型进行额外验证
                            events_model = BiographicalEvents(**result_json)
                            logger.info(f"线程 {threading.get_ident()} 数据通过模型验证")
                            return result_json
                        except Exception as ve:
                            logger.error(f"线程 {threading.get_ident()} 数据验证失败: {str(ve)}")
                            return {"events": []}
                    else:
                        logger.error(f"线程 {threading.get_ident()} 未获取到有效的结构化数据")
                        return {"events": []}

                    # 成功就跳出重试循环
                    break

                except (openai.RateLimitError, openai.APITimeoutError) as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        raise

                    # 指数退避策略
                    backoff_time = (2 ** retry_count) + random.uniform(0, 1)
                    logger.warning(f"线程 {threading.get_ident()} 遇到速率限制或超时，等待 {backoff_time:.2f} 秒后重试")
                    time.sleep(backoff_time)

        except Exception as e:
            logger.error(f"线程 {threading.get_ident()} API调用出错: {str(e)}")
            return {"events": []}

    def process_leader(self, leader: Dict) -> bool:
        """处理单个领导人的履历数据并更新到数据库

        Args:
            leader: 领导人数据字典，包含id、leader_name和career_history等字段

        Returns:
            处理是否成功
        """
        try:
            logger.info(f"处理ID={leader['id']}的领导人: {leader['leader_name']}")

            # 提取履历信息
            bio_text = leader.get('career_history', '')
            # 增强检查，确保career_history不为空且有实际内容
            if not bio_text or bio_text.strip() == '':
                logger.warning(f"领导人ID={leader['id']}的履历信息为空，跳过处理")
                # 更新统计信息，但不算作错误
                with self.stats_lock:
                    self.processed_count += 1
                return True  # 返回True表示处理成功（跳过处理）

            # 调用GPT-4o进行结构化
            events_data = self.extract_biographical_events(bio_text)

            # 如果events为空，记录警告并返回
            if not events_data or not events_data.get("events"):
                logger.warning(f"领导人ID={leader['id']}的履历信息结构化后events为空")
                with self.stats_lock:
                    self.error_count += 1
                return False

            # 将JSON结构转为字符串
            structured_json = json.dumps(events_data, ensure_ascii=False)

            # 更新数据库
            conn = self.get_database_connection()
            if not conn:
                logger.error("无法连接到数据库")
                return False

            try:
                with conn.cursor() as cursor:
                    # 检查是否已有结构化数据
                    check_sql = """
                    SELECT career_history_structured 
                    FROM c_org_leader_info 
                    WHERE id = %s AND career_history_structured IS NOT NULL AND career_history_structured != ''
                    """
                    cursor.execute(check_sql, (leader['id'],))
                    existing = cursor.fetchone()

                    if existing and existing['career_history_structured']:
                        logger.info(f"领导人ID={leader['id']}已有结构化履历数据，跳过更新")
                        with self.stats_lock:
                            self.processed_count += 1
                            self.success_count += 1
                        return True

                    # 更新领导人的结构化履历数据
                    sql = """
                    UPDATE c_org_leader_info
                    SET career_history_structured = %s,
                        update_time = NOW()
                    WHERE id = %s
                    """
                    cursor.execute(sql, (structured_json, leader['id']))
                    conn.commit()

                    logger.info(f"成功更新领导人ID={leader['id']}的结构化履历数据")

                    # 更新统计信息
                    with self.stats_lock:
                        self.success_count += 1

                    return True
            except Exception as e:
                logger.error(f"更新领导人ID={leader['id']}的结构化履历数据时出错: {str(e)}")
                if conn:
                    conn.rollback()
                return False
            finally:
                self.close_database_connection(conn)

        except Exception as e:
            logger.error(f"处理领导人ID={leader['id']}时出错: {str(e)}")

            # 更新统计信息
            with self.stats_lock:
                self.error_count += 1

            return False

    def get_leaders(self, limit: Optional[int] = None, skip_processed: bool = True) -> List[Dict]:
        """
        从数据库获取领导人列表

        Args:
            limit: 限制结果数量
            skip_processed: 是否跳过已处理的记录

        Returns:
            领导人列表
        """
        conn = self.get_database_connection()
        if not conn:
            return []

        try:
            with conn.cursor() as cursor:
                # 构建SQL查询，只获取有career_history且不为空的记录
                if skip_processed:
                    sql = """
                    SELECT id, leader_name, career_history 
                    FROM c_org_leader_info 
                    WHERE career_history IS NOT NULL 
                    AND career_history != '' 
                    AND (career_history_structured IS NULL OR career_history_structured = '')
                    AND is_deleted = 0
                    """
                else:
                    sql = """
                    SELECT id, leader_name, career_history 
                    FROM c_org_leader_info 
                    WHERE career_history IS NOT NULL 
                    AND career_history != ''
                    AND is_deleted = 0
                    """

                if limit is not None:
                    sql += f" LIMIT {limit}"

                cursor.execute(sql)
                leaders = cursor.fetchall()
                logger.info(f"从数据库获取了 {len(leaders)} 条领导人记录")
                return leaders

        except Exception as e:
            logger.error(f"从数据库获取领导人列表时出错: {str(e)}")
            return []
        finally:
            self.close_database_connection(conn)

    def process_leaders(self, limit: Optional[int] = None, skip_processed: bool = True) -> None:
        """
        处理多个领导人的履历数据

        Args:
            limit: 限制处理数量
            skip_processed: 是否跳过已处理的记录
        """
        # 确保数据库表结构正确
        if not self.check_career_history_structured_column():
            logger.error("数据库表结构检查失败，退出处理")
            return

        # 获取领导人列表
        leaders = self.get_leaders(limit, skip_processed)
        if not leaders:
            logger.warning("没有找到需要处理的领导人记录")
            return

        total_count = len(leaders)
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0

        logger.info(f"开始处理 {total_count} 条领导人记录")
        start_time = time.time()

        # 使用线程池处理
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            # 提交所有任务
            futures = {executor.submit(self.process_leader, leader): leader for leader in leaders}

            # 处理完成的任务
            for future in futures:
                leader = futures[future]
                try:
                    success = future.result()
                    # 更新处理计数
                    with self.stats_lock:
                        self.processed_count += 1

                    # 输出进度
                    if self.processed_count % 10 == 0 or self.processed_count == total_count:
                        elapsed = time.time() - start_time
                        remaining = (elapsed / self.processed_count) * (
                                    total_count - self.processed_count) if self.processed_count > 0 else 0
                        logger.info(
                            f"进度: {self.processed_count}/{total_count}, 成功: {self.success_count}, 失败: {self.error_count}, 已用时: {elapsed:.2f}秒, 预计剩余: {remaining:.2f}秒")
                        self.token_tracker.log_stats()

                    # 检查是否达到成本限制
                    if self.token_tracker.limit_reached:
                        logger.warning(
                            f"已达到成本限制(${self.token_tracker.total_cost:.2f}/${self.token_tracker.cost_limit:.2f})，正在停止处理")
                        # 取消所有未完成的任务
                        for remaining_future in [f for f in futures if not f.done()]:
                            remaining_future.cancel()
                        break

                except Exception as e:
                    logger.error(f"处理领导人ID={leader['id']}时发生异常: {str(e)}")
                    with self.stats_lock:
                        self.processed_count += 1
                        self.error_count += 1

        # 打印最终统计
        elapsed_time = time.time() - start_time
        logger.info(
            f"处理完成. 总数: {total_count}, 成功: {self.success_count}, 失败: {self.error_count}, 总耗时: {elapsed_time:.2f}秒, 平均耗时: {elapsed_time / total_count:.2f}秒/条")


def bio_processor(config_path, cost_limit):
    # 从配置文件加载配置
    try:
        config = Config.from_file(config_path)
        logger.info(f"从 {config_path} 加载配置")
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}，使用默认配置")
        config = Config()

    # 数据库配置
    db_config = config.db_config.copy()

    # 线程配置
    max_threads = config.ai_max_threads

    # Azure OpenAI API 配置
    azure_endpoint = config.azure_openai_endpoint
    api_key = config.azure_openai_api_key
    api_version = config.azure_openai_api_version

    # 创建处理器 - 添加token价格参数
    processor = BiographicalDataProcessor(
        azure_endpoint=azure_endpoint,
        api_key=api_key,
        api_version=api_version,
        db_config=db_config,
        max_threads=max_threads,
        request_rate=config.ai_request_rate,
        token_limit=config.ai_token_limit,
        input_price_per_1m=2.50,
        cached_input_price_per_1m=1.25,
        output_price_per_1m=10.0,
        cost_limit=cost_limit
    )

    # 处理领导人履历数据
    processor.process_leaders()

    # 输出最终的token使用和成本统计
    processor.token_tracker.log_stats()

    logger.info("处理完成")
