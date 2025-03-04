import logging
import os
from datetime import datetime


def setup_logger(name: str = None, log_file: str = None,
                 level=logging.INFO, console_output: bool = True) -> logging.Logger:
    """
    设置日志记录器

    Args:
        name: 日志记录器名称，如果为None则使用root记录器
        log_file: 日志文件路径，如果为None则只输出到控制台
        level: 日志级别
        console_output: 是否同时输出到控制台

    Returns:
        配置好的日志记录器
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 移除旧的处理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # 添加文件处理器
    if log_file:
        # 确保日志目录存在
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # 添加控制台处理器
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def get_logger(name: str = None) -> logging.Logger:
    """
    获取已配置的日志记录器，如果未配置则使用默认配置

    Args:
        name: 日志记录器名称

    Returns:
        日志记录器
    """
    logger = logging.getLogger(name)

    # 如果记录器没有处理器，使用默认配置
    if not logger.handlers:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f"logs/scraper_{timestamp}.log"
        return setup_logger(name, log_file)

    return logger