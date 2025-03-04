import logging
import os
from datetime import datetime


def setup_logger(name: str, log_file: str = None,
                 level=logging.INFO, console_output: bool = True) -> logging.Logger:
    """
    设置日志记录器

    Args:
        name: 日志记录器名称，必须提供，统一使用模块级日志器
        log_file: 日志文件路径，如果为None则只输出到控制台
        level: 日志级别
        console_output: 是否同时输出到控制台

    Returns:
        配置好的日志记录器
    """
    logger = logging.getLogger(name)

    # 防止日志重复：如果记录器已经有处理器，表示已配置过，直接返回
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # 确保不会向上传播到父记录器，避免重复日志
    logger.propagate = False

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


def get_logger(name: str) -> logging.Logger:
    """
    获取已配置的日志记录器，如果未配置则使用默认配置

    Args:
        name: 日志记录器名称，必须提供

    Returns:
        日志记录器
    """
    logger = logging.getLogger(name)

    # 如果记录器没有处理器，使用默认配置
    if not logger.handlers:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f"logs/scraper_{timestamp}.log"
        # 确保日志目录存在
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        return setup_logger(name, log_file)

    return logger