import os
import json
import re
from typing import Dict, Any, Optional


def ensure_dir(directory: str) -> str:
    """
    确保目录存在，如果不存在则创建

    Args:
        directory: 目录路径

    Returns:
        目录路径
    """
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    return directory


def safe_filename(filename: str) -> str:
    """
    清理文件名，移除不安全字符

    Args:
        filename: 原始文件名

    Returns:
        安全的文件名
    """
    # 移除不安全字符，只保留字母数字下划线和横杠
    safe_name = re.sub(r'[^\w\-\.]', '_', filename)

    # 如果文件名为空，使用默认名称
    if not safe_name or safe_name == '.':
        safe_name = 'unnamed_file'

    return safe_name


def read_json(filepath: str, default: Any = None) -> Any:
    """
    从JSON文件读取数据

    Args:
        filepath: 文件路径
        default: 如果文件不存在或读取失败时的默认值

    Returns:
        解析后的JSON数据
    """
    try:
        if not os.path.exists(filepath):
            return default

        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"读取JSON文件失败: {filepath}, 错误: {str(e)}")
        return default


def write_json(filepath: str, data: Any, indent: int = 4) -> bool:
    """
    将数据写入JSON文件

    Args:
        filepath: 文件路径
        data: 要写入的数据
        indent: 缩进空格数

    Returns:
        是否成功写入
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
        return True
    except Exception as e:
        print(f"写入JSON文件失败: {filepath}, 错误: {str(e)}")
        return False