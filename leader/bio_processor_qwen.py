"""
bio_processor_qwen.py
从CSV读取人物履历数据，使用多线程调用Qwen模型进行处理，并生成JSON文件
"""
# TODO

import os
import csv
import json
import logging
import threading
import queue
import time
from typing import Dict, Any, List
from openai import OpenAI
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import random
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import Config

# 设置日志
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/bio_processing_qwen.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("bio_processor_qwen")


class BiographicalDataProcessorQwen:
    """处理人物履历数据的类，使用Qwen模型"""

    def __init__(self,
                 api_key: str,
                 base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
                 model_name: str = "qwen-max",
                 result_dir: str = "result",
                 max_threads: int = 10,
                 request_rate: int = 8,  # 每秒请求数，低于限制
                 token_limit: int = 90000):  # 每分钟令牌数，留出余量
        """
        初始化处理器

        Args:
            api_key: Qwen API密钥
            base_url: Qwen API基础URL
            result_dir: 结果文件保存目录，默认为"result"
            max_threads: 最大线程数，默认为10
            request_rate: 每秒最大请求数，默认为8
            token_limit: 每分钟最大令牌数，默认为90000
        """
        self.api_key = api_key
        self.base_url = base_url
        self.model_name = model_name

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

        # 加载示例数据，用于few-shot提示
        self.example_events = self._create_examples()

        logger.info(f"初始化完成，使用{max_threads}个线程，结果将保存至 {self.result_dir.absolute()}")

    def _create_examples(self):
        """创建few-shot示例"""
        # 示例1：标准学习经历
        example1 = {
            "events": [
                {
                    "eventType": "study",
                    "startYear": 2000,
                    "startMonth": 9,
                    "isEnd": True,
                    "hasEndDate": True,
                    "endYear": 2004,
                    "endMonth": 7,
                    "school": "北京大学",
                    "department": "信息科学技术学院",
                    "major": "计算机科学",
                    "degree": "学士",
                    "place": None,
                    "position": None
                }
            ]
        }

        # 示例2：标准工作经历
        example2 = {
            "events": [
                {
                    "eventType": "work",
                    "startYear": 2004,
                    "startMonth": 8,
                    "isEnd": True,
                    "hasEndDate": True,
                    "endYear": 2008,
                    "endMonth": 6,
                    "school": None,
                    "department": None,
                    "major": None,
                    "degree": None,
                    "place": "ABC公司",
                    "position": "软件工程师"
                }
            ]
        }

        # 示例3：多个事件和特殊情况
        example3 = {
            "events": [
                {
                    "eventType": "study",
                    "startYear": 1995,
                    "startMonth": 9,
                    "isEnd": True,
                    "hasEndDate": True,
                    "endYear": 1999,
                    "endMonth": 7,
                    "school": "清华大学",
                    "department": "经济管理学院",
                    "major": "经济学",
                    "degree": "学士",
                    "place": None,
                    "position": None
                },
                {
                    "eventType": "work",
                    "startYear": 1999,
                    "startMonth": 8,
                    "isEnd": False,
                    "hasEndDate": False,
                    "endYear": None,
                    "endMonth": None,
                    "school": None,
                    "department": None,
                    "major": None,
                    "degree": None,
                    "place": "国家发改委",
                    "position": "政策研究员"
                }
            ]
        }

        return {
            "example1": json.dumps(example1, ensure_ascii=False),
            "example2": json.dumps(example2, ensure_ascii=False),
            "example3": json.dumps(example3, ensure_ascii=False)
        }

    def get_client(self):
        """为当前线程获取或创建OpenAI客户端"""
        thread_id = threading.get_ident()
        if thread_id not in self.clients:
            self.clients[thread_id] = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url
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
        system_prompt = f"""
        你是一个能够提取人物履历信息的助手。请提取文本中的人物履历信息，并将其转换为标准JSON格式。

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

        示例1：
        输入：2000年9月至2004年7月就读于北京大学信息科学技术学院计算机科学专业，获得学士学位。
        输出：{self.example_events['example1']}

        示例2：
        输入：2004年8月到2008年6月在ABC公司担任软件工程师。
        输出：{self.example_events['example2']}

        示例3：
        输入：1995年9月至1999年7月就读于清华大学经济管理学院经济学专业，获得学士学位。毕业后于1999年8月入职国家发改委，至今担任政策研究员。
        输出：{self.example_events['example3']}

        请提取以下人物履历，并按照上述示例的JSON格式输出：
        """

        try:
            # 为避免所有线程同时失败，添加随机退避时间
            retry_count = 0
            max_retries = 3

            while retry_count < max_retries:
                try:
                    # 调用API
                    logger.info(f"线程 {threading.get_ident()} 正在调用Qwen API...")
                    response = client.chat.completions.create(
                        model=self.model_name,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": bio_text}
                        ],
                        response_format={"type": "json_object"}
                    )

                    # 解析返回结果
                    if response.choices and response.choices[0].message.content:
                        result_json_str = response.choices[0].message.content
                        try:
                            result_json = json.loads(result_json_str)
                            logger.info(f"线程 {threading.get_ident()} 成功获取结构化数据")

                            # 简单验证返回的JSON格式
                            if "events" not in result_json:
                                logger.error(f"线程 {threading.get_ident()} 返回的JSON中缺少'events'字段")
                                return {"events": []}

                            # 验证events是否为列表
                            if not isinstance(result_json["events"], list):
                                logger.error(f"线程 {threading.get_ident()} 返回的'events'不是列表类型")
                                return {"events": []}

                            # 手动验证一些基本规则
                            self._validate_events(result_json)

                            return result_json
                        except json.JSONDecodeError as je:
                            logger.error(f"线程 {threading.get_ident()} JSON解析失败: {str(je)}")
                            return {"events": []}
                    else:
                        logger.error(f"线程 {threading.get_ident()} 未获取到有效的结构化数据")
                        return {"events": []}

                    # 成功就跳出重试循环
                    break

                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        raise

                    # 指数退避策略
                    backoff_time = (2 ** retry_count) + random.uniform(0, 1)
                    logger.warning(f"线程 {threading.get_ident()} 遇到错误，等待 {backoff_time:.2f} 秒后重试: {str(e)}")
                    logger.warning(f"对应内容： {bio_text}")
                    time.sleep(backoff_time)

        except Exception as e:
            logger.error(f"线程 {threading.get_ident()} API调用出错: {str(e)}")
            return {"events": []}

    def _validate_events(self, result_json):
        """
        简单验证返回的事件数据

        Args:
            result_json: 返回的JSON结果
        """
        events = result_json.get("events", [])
        valid_events = []

        for event in events:
            try:
                # 检查必需字段
                if "eventType" not in event:
                    logger.warning("事件缺少eventType字段，跳过")
                    continue

                # 验证eventType
                event_type = event.get("eventType")
                if event_type not in ["study", "work"]:
                    logger.warning(f"无效的eventType: {event_type}，跳过")
                    continue

                # 验证学习经历
                if event_type == "study":
                    if not event.get("school"):
                        logger.warning("学习经历缺少school字段，跳过")
                        continue

                    # 确保place和position为null
                    event["place"] = None
                    event["position"] = None

                # 验证工作经历
                if event_type == "work":
                    if not event.get("place") or not event.get("position"):
                        logger.warning("工作经历缺少place或position字段，跳过")
                        continue

                    # 确保学习相关字段为null
                    event["school"] = None
                    event["department"] = None
                    event["major"] = None
                    event["degree"] = None

                # 验证年份范围
                if event.get("startYear") is not None and (event["startYear"] < 1900 or event["startYear"] > 2100):
                    logger.warning(f"无效的startYear: {event['startYear']}，调整为None")
                    event["startYear"] = None

                if event.get("endYear") is not None and (event["endYear"] < 1900 or event["endYear"] > 2100):
                    logger.warning(f"无效的endYear: {event['endYear']}，调整为None")
                    event["endYear"] = None

                # 验证月份范围
                if event.get("startMonth") is not None and (event["startMonth"] < 1 or event["startMonth"] > 12):
                    logger.warning(f"无效的startMonth: {event['startMonth']}，调整为None")
                    event["startMonth"] = None

                if event.get("endMonth") is not None and (event["endMonth"] < 1 or event["endMonth"] > 12):
                    logger.warning(f"无效的endMonth: {event['endMonth']}，调整为None")
                    event["endMonth"] = None

                # 验证isEnd和hasEndDate的一致性
                if event.get("isEnd") is True and event.get("hasEndDate") is True and event.get("endYear") is None:
                    logger.warning("isEnd和hasEndDate都为true但endYear为None，将hasEndDate调整为false")
                    event["hasEndDate"] = False

                valid_events.append(event)

            except Exception as e:
                logger.error(f"验证事件时发生错误: {str(e)}")
                continue

        # 更新事件列表
        result_json["events"] = valid_events

    def process_item(self, row, row_num):
        """处理单个人物的履历数据项"""
        try:
            logger.info(f"线程 {threading.get_ident()} 正在处理第{row_num}行: {row['person_name']}({row['person_id']})")

            # 提取履历信息
            bio_text = row.get('person_bio_raw', '')
            if not bio_text:
                logger.warning(f"{row['person_name']}({row['person_id']})履历信息为空，跳过处理")
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
                        logger.info(f"跳过{row['person_name']}({row['person_id']}): 文件已存在且events不为空")

                        # 更新统计信息
                        with self.stats_lock:
                            self.processed_count += 1
                            self.success_count += 1

                        return True
                    else:
                        logger.info(f"{row['person_name']}({row['person_id']}): 文件已存在但events为空，重新处理")
                except (json.JSONDecodeError, FileNotFoundError) as e:
                    logger.warning(f"{row['person_name']}({row['person_id']}): 读取现有文件出错，重新处理: {e}")
            else:
                logger.info(f"{row['person_name']}({row['person_id']}): 开始处理")

            # 调用Qwen进行结构化
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
            logger.error(f"处理{row['person_name']}({row['person_id']})时出错: {str(e)}")

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
                future_to_row = {executor.submit(self.process_item, row, i + 1): (row, i + 1)
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
                        f"平均每行耗时: {process_time / total_rows:.2f}秒")

        except Exception as e:
            logger.error(f"处理CSV时出错: {str(e)}")


def main():
    """主函数"""
    config_path = '../config.yaml'
    config = Config.from_file(config_path)

    # 配置参数，推荐使用环境变量来存储敏感信息
    API_KEY = config.qwen_api_key  # 从配置中获取Qwen API密钥
    BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    MODEL_NAME = "qwen-max"
    CSV_PATH = "../data/shanghai_leadership_list.csv"
    RESULT_DIR = "../data/result_qwen_max"

    # 设置线程数和限制 - 根据Qwen API限制策略调整
    MAX_THREADS = 10  # 根据计算得出的线程数
    REQUEST_RATE = 8  # 每秒请求数，稍低于限制以留余量
    TOKEN_LIMIT = 90000  # 每分钟令牌数，略低于限制

    # 创建处理器
    processor = BiographicalDataProcessorQwen(
        api_key=API_KEY,
        base_url=BASE_URL,
        model_name=MODEL_NAME,
        result_dir=RESULT_DIR,
        max_threads=MAX_THREADS,
        request_rate=REQUEST_RATE,
        token_limit=TOKEN_LIMIT
    )

    # 处理CSV
    processor.process_csv(CSV_PATH)


if __name__ == "__main__":
    main()