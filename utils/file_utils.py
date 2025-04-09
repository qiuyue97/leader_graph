import os
import re


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
