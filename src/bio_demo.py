import os
import logging
import json
import sys
from typing import Dict, Any
from openai import AzureOpenAI

# 导入所需模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import Config
from leader.schema import BiographicalEvents

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("bio_extractor_demo")


class BiographicalExtractorDemo:
    """简化版履历提取器演示，从文本中提取结构化的履历数据"""

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

    def extract_biographical_events(self, bio_text: str) -> Dict[str, Any]:
        """
        提取文本中的人物履历信息

        Args:
            bio_text: 包含人物履历的文本

        Returns:
            Dict: 结构化的人物履历信息
        """
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
        - 当设置isEnd=true和hasEndDate=true时，必须同时提供endYear值，否则数据将无法通过验证
        - 如果无法确定结束年份，但确定已结束，应设置isEnd=true, hasEndDate=false
        """

        # 使用从schema导入的BiographicalEvents工具
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "extract_biographical_events",
                    "description": "提取并结构化人物履历信息",
                    "parameters": BiographicalEvents.model_json_schema()
                }
            }
        ]

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": bio_text}
        ]

        try:
            logger.info("正在调用Azure OpenAI API...")

            # 调用API
            response = self.client.chat.completions.create(
                model="gpt-4o",  # 替换为您的模型部署名称
                messages=messages,
                tools=tools,
                tool_choice={"type": "function", "function": {"name": "extract_biographical_events"}}
            )

            # 解析返回结果
            if response.choices and response.choices[0].message.tool_calls:
                tool_call = response.choices[0].message.tool_calls[0]
                result_json = json.loads(tool_call.function.arguments)
                logger.info("成功获取结构化数据")

                # 验证处理后的数据
                try:
                    # 使用Pydantic模型进行额外验证
                    events_model = BiographicalEvents(**result_json)
                    logger.info("数据通过模型验证")
                    return result_json
                except Exception as ve:
                    logger.error(f"数据验证失败: {str(ve)}")
                    return {"events": []}
            else:
                logger.error("未获取到有效的结构化数据")
                return {"events": []}

        except Exception as e:
            logger.error(f"API调用出错: {str(e)}")
            return {"events": []}


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
        extractor = BiographicalExtractorDemo(
            azure_endpoint=azure_endpoint,
            api_key=api_key,
            api_version=api_version
        )

        # 示例履历文本 - 直接在代码中定义，您可以修改这里的内容进行测试
        bio_text = """
        张伟，男，1985年3月生，中国科学院大学经济与管理学院毕业，经济学博士。
        2010年7月至2015年5月在中国银行总行工作，任分析师。
        2015年6月至2018年12月在中国人民银行研究局工作，任副处长。
        2019年1月至今在国家金融监督管理总局工作，任处长。
        """

        logger.info("使用预设的履历文本进行提取")
        logger.info(f"履历文本: {bio_text}")

        # 执行提取
        result = extractor.extract_biographical_events(bio_text)

        # 格式化输出
        print("\n结构化结果:")
        print(json.dumps(result, ensure_ascii=False, indent=2))

    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()