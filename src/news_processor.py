"""
news_processor.py
调用GPT-4o从新闻稿中提取结构化信息并生成JSON输出
"""

import os
import json
import logging
import time
from typing import Dict, Any
from openai import AzureOpenAI
import openai
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import Config

# 假设您已经创建了新的schema文件
from news_schema import NewsExtraction

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("news_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("news_processor")


class NewsDataProcessor:
    """处理新闻稿数据的类"""

    def __init__(self,
                 azure_endpoint: str,
                 api_key: str,
                 api_version: str = "2024-10-21"):
        """
        初始化处理器

        Args:
            azure_endpoint: Azure OpenAI的端点URL
            api_key: Azure OpenAI的API密钥
            api_version: API版本
        """
        self.azure_endpoint = azure_endpoint
        self.api_key = api_key
        self.api_version = api_version

        # 创建OpenAI客户端
        self.client = AzureOpenAI(
            azure_endpoint=self.azure_endpoint,
            api_key=self.api_key,
            api_version=self.api_version
        )

        logger.info("新闻处理器初始化完成")

    def extract_news_entities(self, news_text: str) -> Dict[str, Any]:
        """
        从新闻稿中提取结构化信息

        Args:
            news_text: 新闻稿文本

        Returns:
            Dict: 结构化的新闻信息
        """
        # 创建详细的系统提示
        system_prompt = """
        你是一个能够从新闻稿中提取关键信息的助手。请使用提供的工具结构化地返回信息。

        请严格遵循以下规范提取信息：
        1. 领导/主导人物：提取新闻中的主要领导或主导人物，包括姓名和职位
        2. 地点：提取事件发生的地点，可能包括具体场所
        3. 事件：提取领导人做了什么事情，尽可能详细描述
        4. 目标客体：提取事件的对象，可以是人、组织或其他实体（可能有多个）
        5. 陪同人物：提取陪同主要领导人的其他人物（可能有多个）

        特别注意：
        - 领导/主导人物应该是新闻中最主要的行动者
        - 如果某些信息在文本中未提及，相应字段可以为null或空列表
        - 请确保提取的信息准确反映新闻内容，不要添加未在文本中出现的信息
        - 当有多个目标客体或陪同人物时，请全部提取出来
        """

        # 准备工具和消息
        tools = [openai.pydantic_function_tool(NewsExtraction)]

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": news_text}
        ]

        try:
            # 调用API
            logger.info("正在调用Azure OpenAI API...")
            start_time = time.time()

            response = self.client.chat.completions.create(
                model="gpt-4o",  # 替换为您的模型部署名称
                messages=messages,
                tools=tools,
                parallel_tool_calls=False  # 使用结构化输出时需要设置为False
            )

            end_time = time.time()
            logger.info(f"API调用完成，耗时: {end_time - start_time:.2f}秒")

            # 解析返回结果
            if response.choices and response.choices[0].message.tool_calls:
                tool_call = response.choices[0].message.tool_calls[0]
                result_json = json.loads(tool_call.function.arguments)
                logger.info("成功获取结构化数据")

                # 验证处理后的数据
                try:
                    # 使用Pydantic模型进行额外验证
                    news_model = NewsExtraction(**result_json)
                    logger.info("数据通过模型验证")
                    return result_json
                except Exception as ve:
                    logger.error(f"数据验证失败: {str(ve)}")
                    return {}
            else:
                logger.error("未获取到有效的结构化数据")
                return {}

        except Exception as e:
            logger.error(f"API调用出错: {str(e)}")
            return {}


def main():
    """主函数"""
    # 从配置文件中加载配置
    config_path = '../config.yaml'
    config = Config.from_file(config_path)

    # 配置参数
    AZURE_ENDPOINT = config.azure_openai_endpoint
    API_KEY = config.azure_openai_api_key

    # 创建示例新闻稿
    news_text = """
        记者 孟群舒
        
        按照市委主题教育工作安排和大兴调查研究部署要求，市委副书记、市长龚正昨天赴浦东新区，深入新兴产业企业开展专题调研。龚正指出，要深入学习贯彻党的二十大和习近平总书记考察上海重要讲话精神，在市委领导下，大力营造一流的产业发展环境，积极引导各类资本加大投入，聚焦重点领域关键环节持续创新，加快培育更多掌握核心技术的创新企业，推动战略性新兴产业持续发展、促进发展新动能持续壮大。
        
        下午，龚正一行来到中微半导体设备（上海）股份有限公司，察看刻蚀设备展厅及生产线，听取企业发展情况介绍。在上海燧原科技有限公司，龚正走进企业展厅，深入了解新一代人工智能芯片设计和应用情况。在上海概伦电子股份有限公司，市领导察看产品展示中心，详细了解企业推进创新研发、市场拓展情况，并就行业未来趋势与企业负责人交流。龚正指出，当前数字经济发展速度前所未有，中国市场的机遇前所未有，要坚持不懈，勇攀高峰，加快突破，切实提高产业核心竞争力。
        
        龚正指出，战略性新兴产业是上海加快建设具有全球影响力的科技创新中心的关键支撑，也是率先构建现代化产业体系的重要组成部分。集成电路、生物医药、人工智能三大先导产业，具有前期投入大、研发周期长、成果产出难等特点，要遵循产业发展规律，用足用好各类优势，勇于突破瓶颈阻碍，加快形成一批具有标志性的先进技术、创新产品和龙头企业。要促进创新链与产业链深度融合，抓住科创中心建设带来的机遇，发挥浦东新区立法优势和自贸区临港新片区制度优势，提升产业基础高级化、产业链现代化水平，缩小与先进水平的差距，打造产业创新发展高地。政府部门要主动跨前做好服务，及时出台适应新兴产业发展需求的配套政策，提高精准度和有效性，更好释放上海的人才、产业、市场、场景、金融等综合优势，以优良的产业生态聚集更多市场化力量，加快国产设备和技术应用，持续激发新兴产业发展动力和活力。
        
        副市长李政参加调研。
    """

    # 创建处理器
    processor = NewsDataProcessor(
        azure_endpoint=AZURE_ENDPOINT,
        api_key=API_KEY
    )

    # 处理新闻文本
    result = processor.extract_news_entities(news_text)

    # 打印JSON结果
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()