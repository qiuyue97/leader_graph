"""
bio_processor_threaded.py
从CSV读取人物履历数据，使用多线程调用GPT-4o进行处理，并生成JSON文件
"""

import os
import csv
import json
import logging
import threading
import queue
import time
from typing import Dict, Any, List
from openai import AzureOpenAI
import openai
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import random

from config.settings import Config

# 导入数据模型
from schema import BiographicalEvents

# 设置日志
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../temp_code/bio_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("bio_processor")


class BiographicalDataProcessor:
    """处理人物履历数据的类"""

    def __init__(self,
                 azure_endpoint: str,
                 api_key: str,
                 api_version: str = "2024-10-21",
                 result_dir: str = "result",
                 max_threads: int = 10,
                 request_rate: int = 8,  # 每秒请求数，低于限制
                 token_limit: int = 90000):  # 每分钟令牌数，留出余量
        """
        初始化处理器

        Args:
            azure_endpoint: Azure OpenAI的端点URL
            api_key: Azure OpenAI的API密钥
            api_version: API版本
            result_dir: 结果文件保存目录，默认为"result"
            max_threads: 最大线程数，默认为10
            request_rate: 每秒最大请求数，默认为8
            token_limit: 每分钟最大令牌数，默认为90000
        """
        self.azure_endpoint = azure_endpoint
        self.api_key = api_key
        self.api_version = api_version

        # 每个线程使用独立的客户端实例
        self.clients = {}

        # 确保结果目录存在
        self.result_dir = Path(result_dir)
        self.result_dir.mkdir(exist_ok=True, parents=True)

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

        logger.info(f"初始化完成，使用{max_threads}个线程，结果将保存至 {self.result_dir.absolute()}")

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
                    )

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

    def process_item(self, row, row_num):
        """处理单个人物的履历数据项"""
        try:
            logger.info(f"线程 {threading.get_ident()} 正在处理第{row_num}行: {row['person_name']}({row['person_id']})")

            # 提取履历信息
            bio_text = row.get('person_bio_raw', '')
            if not bio_text:
                logger.warning(f"线程 {threading.get_ident()} 第{row_num}行履历信息为空，跳过处理")
                return False

            # 生成文件名并避免文件名中的非法字符
            safe_name = ''.join(c for c in row.get('person_name', 'unknown') if c.isalnum() or c in ' _-')
            filename = f"{safe_name}_{row.get('person_id', 'unknown')}.json"
            file_path = self.result_dir / filename

            # 检查目标文件是否已存在
            if file_path.exists():
                try:
                    # 读取现有文件
                    with open(file_path, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)

                    # 检查events是否为空
                    if existing_data.get('events') and len(existing_data.get('events')) > 0:
                        logger.info(f"线程 {threading.get_ident()} 跳过第{row_num}行: 文件已存在且events不为空")

                        # 更新统计信息
                        with self.stats_lock:
                            self.processed_count += 1
                            self.success_count += 1

                        return True
                    else:
                        logger.info(f"线程 {threading.get_ident()} 第{row_num}行: 文件已存在但events为空，重新处理")
                except (json.JSONDecodeError, FileNotFoundError) as e:
                    logger.warning(f"线程 {threading.get_ident()} 第{row_num}行: 读取现有文件出错，重新处理: {e}")
            else:
                logger.info(f"线程 {threading.get_ident()} 第{row_num}行: 开始处理")

            # 调用GPT-4o进行结构化
            events_data = self.extract_biographical_events(bio_text)

            # 创建完整的数据结构
            result = {
                "from": row.get("from", ""),
                "person_id": row.get("person_id", ""),
                "person_name": row.get("person_name", ""),
                "person_url": row.get("person_url", ""),
                "person_title": row.get("person_title", ""),
                "person_summary": row.get("person_summary", ""),
                "ethnicity": row.get("ethnicity", ""),
                "native_place": row.get("native_place", ""),
                "birth_date": row.get("birth_date", ""),
                "alma_mater": row.get("alma_mater", ""),
                "political_status": row.get("political_status", "")
            }

            # 添加events数据
            result.update(events_data)

            # 保存JSON文件
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            logger.info(f"线程 {threading.get_ident()} 成功处理并保存: {file_path}")

            # 更新统计信息
            with self.stats_lock:
                self.processed_count += 1
                self.success_count += 1

            return True

        except Exception as e:
            logger.error(f"线程 {threading.get_ident()} 处理第{row_num}行时出错: {str(e)}")

            # 更新统计信息
            with self.stats_lock:
                self.processed_count += 1
                self.error_count += 1

            return False

    def process_csv(self, csv_path: str) -> None:
        """
        处理CSV文件中的每一行数据

        Args:
            csv_path: CSV文件路径
        """
        logger.info(f"开始处理CSV文件: {csv_path}")

        try:
            # 读取CSV并预处理数据
            rows = []
            with open(csv_path, 'r', encoding='utf-8-sig') as csv_file:
                reader = csv.DictReader(csv_file)

                # 检查所需字段是否存在
                required_fields = ["from", "person_id", "person_name", "person_url",
                                  "person_title", "person_bio_raw", "person_summary",
                                  "ethnicity", "native_place", "birth_date",
                                  "alma_mater", "political_status"]
                csv_fields = reader.fieldnames if reader.fieldnames else []

                missing_fields = [field for field in required_fields if field not in csv_fields]
                if missing_fields:
                    logger.error(f"CSV缺少必要字段: {', '.join(missing_fields)}")
                    return

                # 将所有行加载到内存
                rows = list(reader)

            total_rows = len(rows)
            logger.info(f"CSV文件共有 {total_rows} 行数据")

            # 使用线程池处理数据
            start_time = time.time()
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                # 提交所有任务
                future_to_row = {executor.submit(self.process_item, row, i+1): (row, i+1)
                                for i, row in enumerate(rows)}

                # 处理完成的任务
                for future in future_to_row:
                    row, row_num = future_to_row[future]
                    try:
                        success = future.result()
                        if not success:
                            logger.warning(f"第{row_num}行处理失败: {row.get('person_name', 'unknown')}")
                    except Exception as exc:
                        logger.error(f"处理第{row_num}行时发生异常: {exc}")

            # 汇总统计
            end_time = time.time()
            process_time = end_time - start_time
            skipped_count = total_rows - (self.success_count + self.error_count)
            logger.info(f"CSV处理完成. 总行数: {total_rows}, 处理成功: {self.success_count}, "
                        f"处理失败: {self.error_count}, 跳过处理: {skipped_count}, "
                        f"总耗时: {process_time:.2f}秒, "
                        f"平均每行耗时: {process_time/total_rows:.2f}秒")

        except Exception as e:
            logger.error(f"处理CSV时出错: {str(e)}")


def main():
    """主函数"""
    config_path = '../config.yaml'
    config = Config.from_file(config_path)

    # 配置参数，推荐使用环境变量来存储敏感信息
    AZURE_ENDPOINT = config.azure_openai_endpoint
    API_KEY = config.azure_openai_api_key
    CSV_PATH = "../data/shanghai_leadership_list.csv"
    RESULT_DIR = "../data/result"

    # 设置线程数和限制 - 根据Azure限制策略调整
    MAX_THREADS = 10  # 根据计算得出的线程数
    REQUEST_RATE = 8  # 每秒请求数，稍低于限制以留余量
    TOKEN_LIMIT = 90000  # 每分钟令牌数，略低于限制

    # 创建处理器
    processor = BiographicalDataProcessor(
        azure_endpoint=AZURE_ENDPOINT,
        api_key=API_KEY,
        result_dir=RESULT_DIR,
        max_threads=MAX_THREADS,
        request_rate=REQUEST_RATE,
        token_limit=TOKEN_LIMIT
    )

    # 处理CSV
    processor.process_csv(CSV_PATH)


if __name__ == "__main__":
    main()