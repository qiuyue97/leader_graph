import os
import json
import logging
import sys
from typing import Dict, Any, Optional
from openai import AzureOpenAI

# 导入所需模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import Config
from news_schema import NewsExtraction

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("news_extractor_demo")


class NewsExtractorDemo:
    """简化版新闻提取器演示，从文本中提取结构化的新闻信息"""

    def __init__(self, azure_endpoint: str, api_key: str, api_version: str = "2024-10-21"):
        """
        初始化提取器

        Args:
            azure_endpoint: Azure OpenAI的端点URL
            api_key: Azure OpenAI的API密钥
            api_version: API版本
        """
        self.azure_endpoint = azure_endpoint
        self.api_key = api_key
        self.api_version = api_version
        self.client = AzureOpenAI(
            azure_endpoint=azure_endpoint,
            api_key=api_key,
            api_version=api_version
        )
        # logger.info(f"初始化完成，使用Azure OpenAI端点: {azure_endpoint}")

    def classify_news_by_title(self, title: str) -> bool:
        """
        根据新闻标题判断是否是"领导前往某个地方进行某些行为"类型的新闻

        Args:
            title: 新闻标题

        Returns:
            bool: 是否符合条件
        """
        # 创建系统提示
        system_prompt = """
        你是一个能够分类新闻标题的助手。请判断给定的新闻标题是否描述了"领导前往某个地方进行某些行为"的内容。

        判断标准：
        1. 标题中应包含领导/官员（如书记、市长、主席、部长等）
        2. 标题中明确或暗示了领导前往某地、视察、调研、考察、检查等行为

        请只回复布尔值：
        - 如果符合条件，返回: true
        - 如果不符合条件，返回: false
        """

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": title}
        ]

        try:
            # 调用API
            logger.info(f"正在使用GPT-4o分类新闻标题: {title[:30]}...")

            response = self.client.chat.completions.create(
                model="gpt-4o",  # 使用GPT-4o进行分类
                messages=messages,
                temperature=0.1  # 使用低温度以获得更确定的回答
            )

            # 解析结果
            result_text = response.choices[0].message.content.strip().lower()

            # 检查结果是否为布尔值
            if result_text == "true":
                logger.info("标题分类结果: 符合领导活动条件")
                return True
            elif result_text == "false":
                logger.info("标题分类结果: 不符合领导活动条件")
                return False
            else:
                # 如果不是明确的true/false，尝试进一步解析
                if "true" in result_text or "符合" in result_text or "是" in result_text:
                    logger.info("标题分类结果: 符合领导活动条件 (通过内容解析)")
                    return True
                else:
                    logger.info("标题分类结果: 不符合领导活动条件 (通过内容解析)")
                    return False

        except Exception as e:
            logger.error(f"标题分类API调用出错: {str(e)}")
            return False  # 出错时默认为不符合条件

    def extract_news_entities(self, news_text: str) -> Optional[Dict[str, Any]]:
        """
        从新闻稿中提取结构化信息

        Args:
            news_text: 新闻稿文本

        Returns:
            Dict: 结构化的新闻信息，如果提取失败则返回None
        """
        # 创建详细的系统提示
        system_prompt = """
        你是一个能够从新闻稿中提取关键信息的助手。请使用提供的工具结构化地返回信息。

        请严格遵循以下规范提取信息：
        1. 领导/主导人物：提取新闻中的主要领导或主导人物，包括姓名和职位
        2. 地点：提取事件发生的地点，可能包括具体场所
        3. 事件：提取领导人做了什么事情，尽可能详细描述
        4. 目标客体：提取事件的对象，只能是个人、公司或组织（可能有多个），如果不在这三个之中则不提取
        5. 陪同人物：提取陪同主要领导人的其他人物（可能有多个）

        特别注意：
        - 领导/主导人物应该是新闻中最主要的行动者
        - 如果某些信息在文本中未提及，相应字段可以为null或空列表
        - 请确保提取的信息准确反映新闻内容，不要添加未在文本中出现的信息
        - 当有多个目标客体或陪同人物时，请全部提取出来
        """

        # 准备工具和消息
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "extract_news_entities",
                    "description": "提取新闻中的核心实体和事件信息",
                    "parameters": NewsExtraction.model_json_schema()
                }
            }
        ]

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": news_text}
        ]

        try:
            # 调用API
            logger.info("正在调用GPT-4o提取新闻实体...")

            response = self.client.chat.completions.create(
                model="gpt-4o",  # 使用GPT-4o进行信息提取
                messages=messages,
                tools=tools,
                tool_choice={"type": "function", "function": {"name": "extract_news_entities"}}
            )

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
                    return None
            else:
                logger.error("未获取到有效的结构化数据")
                return None

        except Exception as e:
            logger.error(f"实体提取API调用出错: {str(e)}")
            return None

    def process_news(self, title: str, content: str) -> Dict[str, Any]:
        """
        处理新闻，先根据标题判断类型，然后提取结构化信息

        Args:
            title: 新闻标题
            content: 新闻正文

        Returns:
            Dict: 包含处理结果的字典
        """
        result = {
            "title": title,
            "is_leader_activity": False,
            "structured_data": None
        }

        try:
            # 步骤1: 根据标题判断新闻类型
            is_leader_activity = self.classify_news_by_title(title)
            result["is_leader_activity"] = is_leader_activity

            # 如果不是领导活动新闻，跳过提取步骤
            if not is_leader_activity:
                logger.info(f"新闻不符合领导活动条件，跳过提取步骤")
                return result

            # 步骤2: 对符合条件的新闻进行结构化信息提取
            if content and content.strip():
                structured_data = self.extract_news_entities(content)
                if structured_data:
                    result["structured_data"] = structured_data
                    logger.info("成功提取新闻的结构化数据")
                else:
                    logger.warning("未能成功提取新闻的结构化数据")
            else:
                logger.warning("新闻正文为空，跳过提取步骤")

            return result

        except Exception as e:
            logger.error(f"处理新闻时出错: {str(e)}")
            return result


def main():
    """主函数"""
    try:
        # 从配置文件加载配置
        config_path = '../config.yaml'
        config = Config.from_file(config_path)
        logger.info(f"从 {config_path} 加载配置")

        # 获取Azure OpenAI API配置
        azure_endpoint = config.azure_openai_endpoint
        api_key = config.azure_openai_api_key
        api_version = config.azure_openai_api_version

        # 创建提取器
        extractor = NewsExtractorDemo(
            azure_endpoint=azure_endpoint,
            api_key=api_key,
            api_version=api_version
        )

        # 示例新闻 - 直接在代码中定义，您可以修改这里的内容进行测试
        sample_news = {
            "title": "省委书记赵明到重点企业调研产业发展情况",
            "content": """
            本报讯（记者李华）昨日上午，省委书记赵明深入我省重点企业调研产业发展情况，强调要坚持创新驱动，加快转型升级，推动高质量发展。

            赵明首先来到科技创新园区的华为技术有限公司，详细了解企业研发投入、人才引进、市场拓展等情况。他指出，创新是引领发展的第一动力，企业要加大研发投入，提升核心竞争力。

            随后，赵明来到新能源汽车生产基地比亚迪汽车有限公司，实地考察了生产线和新产品展示。他强调，新能源产业是未来发展的重要方向，要抢抓机遇，做大做强。

            调研中，赵明认真听取了企业负责人的情况介绍，并与一线员工亲切交流。他说，推动经济高质量发展，企业是主体，人才是关键。要大力支持企业创新发展，优化营商环境，为企业提供更好的服务。

            省委常委、秘书长王刚，副省长张健参加调研。
            """
        }

        logger.info("使用预设的新闻进行处理")
        logger.info(f"新闻标题: {sample_news['title']}")

        # 执行处理
        result = extractor.process_news(sample_news["title"], sample_news["content"])

        # 格式化输出
        print("\n处理结果:")
        print(f"新闻标题: {result['title']}")
        print(f"是否领导活动: {result['is_leader_activity']}")

        if result["structured_data"]:
            print("\n结构化数据:")
            print(json.dumps(result["structured_data"], ensure_ascii=False, indent=2))
        else:
            print("\n未提取到结构化数据")

    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()