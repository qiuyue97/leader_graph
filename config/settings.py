import os
import json
import yaml
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass, field, asdict

logger = logging.getLogger(__name__)


@dataclass
class Config:
    """应用配置类"""

    # 输入/输出设置
    input_csv_path: str = "shanghai_leadership_list.csv"
    output_dir: str = "./data"

    # 线程设置
    num_producers: int = 10
    num_consumers: int = 1

    # 爬虫设置
    use_mobile: bool = True
    max_retries: int = 3
    min_content_size: int = 1024

    # 代理设置
    use_proxy: bool = True
    proxy_config: Dict[str, Any] = field(default_factory=lambda: {
        "providers": [],
        "refresh_interval": 15,
        "min_proxies": 20
    })

    # 保存设置
    save_interval: int = 10

    # 其他设置
    request_delay_min: float = 1.0
    request_delay_max: float = 3.0

    # Azure OpenAI API 设置
    azure_openai_endpoint: str = "your_url"
    azure_openai_api_key: str = "your_key"
    azure_openai_api_version: str = "2024-10-21"
    qwen_api_key: str = "your_key"

    # AI处理设置
    ai_max_threads: int = 10
    ai_request_rate: int = 8  # 每秒请求数
    ai_token_limit: int = 90000  # 每分钟令牌数限制

    neo4j_config: Dict[str, Any] = field(default_factory=lambda: {
        "uri": "bolt://localhost:27687",
        "user": "neo4j",
        "password": "your_password"
    })

    # 单例实例
    _instance = None

    def __new__(cls, *args, **kwargs):
        """实现单例模式"""
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    @classmethod
    def from_file(cls, filepath: str) -> 'Config':
        """
        从配置文件加载配置

        Args:
            filepath: 配置文件路径（支持JSON和YAML）

        Returns:
            配置对象
        """
        try:
            if not os.path.exists(filepath):
                logger.warning(f"配置文件不存在: {filepath}，使用默认配置")
                return cls()

            with open(filepath, 'r', encoding='utf-8') as f:
                if filepath.endswith('.json'):
                    config_data = json.load(f)
                elif filepath.endswith(('.yaml', '.yml')):
                    import yaml
                    config_data = yaml.safe_load(f)
                else:
                    logger.error(f"不支持的配置文件格式: {filepath}")
                    return cls()

            # 创建新的配置对象，这将替换单例实例
            instance = cls(**config_data)
            logger.info(f"从 {filepath} 加载配置")
            return instance

        except Exception as e:
            logger.error(f"加载配置文件出错: {str(e)}")
            return cls()

    def to_file(self, filepath: str) -> bool:
        """
        将配置保存到文件

        Args:
            filepath: 配置文件路径（支持JSON和YAML）

        Returns:
            是否成功保存
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                if filepath.endswith('.json'):
                    json.dump(asdict(self), f, indent=4, ensure_ascii=False)
                elif filepath.endswith(('.yaml', '.yml')):
                    import yaml
                    yaml.dump(asdict(self), f, default_flow_style=False)
                else:
                    logger.error(f"不支持的配置文件格式: {filepath}")
                    return False

            logger.info(f"配置已保存到 {filepath}")
            return True

        except Exception as e:
            logger.error(f"保存配置文件出错: {str(e)}")
            return False

    @classmethod
    def create_example_config(cls, filepath: str = "config.yaml") -> bool:
        """
        创建示例配置文件

        Args:
            filepath: 配置文件路径

        Returns:
            是否成功创建
        """
        config = cls(
            input_csv_path="./shanghai_leadership_list.csv",
            output_dir="./data",
            num_producers=10,
            num_consumers=1,
            use_mobile=True,
            max_retries=3,
            use_proxy=True,
            proxy_config={
                "providers": [
                    {
                        "type": "xiaoxiang",
                        "app_key": "xx",
                        "app_secret": "xx"
                    }
                ],
                "refresh_interval": 15,
                "min_proxies": 20
            },
            save_interval=10,
            request_delay_min=1.0,
            request_delay_max=3.0,
            min_content_size=1024
        )

        return config.to_file(filepath)