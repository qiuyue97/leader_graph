import re
from typing import Dict, Any, Optional

# 获取日志器
from utils.logger import get_logger

logger = get_logger(__name__)


class ContentValidator:
    """用于验证获取的HTML内容是否有效的类"""

    def __init__(self, min_content_size: int = 1024):
        """
        初始化内容验证器

        Args:
            min_content_size: 有效内容的最小字节数
        """
        self.min_content_size = min_content_size
        logger.info(f"内容验证器初始化，最小内容大小: {min_content_size} 字节")

        # 用于检测百度安全验证页面的特征
        self.security_patterns = [
            r'百度安全验证',
            r'<title>百度安全验证</title>',
            r'class="passMod_dialog-header"',
            r'class="bioc_popup_wrap"',
            r'安全验证',
            r'请完成下方验证后继续操作',
            r'请向右滑动完成拼图',
            r'请依次点击',
            r'文字点选验证',
            r'滑动验证'
        ]

        # 用于检测网络错误页面的特征
        self.error_patterns = [
            r'<body class="neterror"',
            r'ERR_TIMED_OUT',
            r'ERR_CONNECTION_RESET',
            r'ERR_CONNECTION_REFUSED',
            r'无法访问此网站',
            r'icon-generic',
            r'main-frame-error',
            r'响应时间过长',
            r'网络连接中断',
            r'检查网络连接'
        ]

        # 用于检测有效百科页面的特征
        self.valid_patterns = [
            r'<div class="lemma-summary"',
            r'<div class="basic-info"',
            r'<h1 class="title"',
            r'<div class="para',
            r'<div class="lemmaWgt-subjectNav"',
            r'<div class="main-content".*?百度百科'
        ]

    def is_valid_content(self, html_content: str) -> Dict[str, Any]:
        """
        检查HTML内容是否有效

        Args:
            html_content: 要检查的HTML内容

        Returns:
            验证结果字典，包含：
            - valid: 是否有效
            - reason: 无效原因
            - content_size: 内容大小
        """
        if not html_content:
            logger.warning("验证失败: 内容为空")
            return {
                "valid": False,
                "reason": "内容为空",
                "content_size": 0
            }

        # 检查内容大小
        content_size = len(html_content.encode('utf-8'))
        if content_size < self.min_content_size:
            logger.warning(f"验证失败: 内容太小 ({content_size} 字节，最小要求 {self.min_content_size} 字节)")
            return {
                "valid": False,
                "reason": f"内容太小 ({content_size} 字节)",
                "content_size": content_size
            }

        # 检查是否是安全验证页面
        for pattern in self.security_patterns:
            if re.search(pattern, html_content, re.IGNORECASE):
                logger.warning("验证失败: 触发百度安全验证")
                return {
                    "valid": False,
                    "reason": "触发百度安全验证",
                    "content_size": content_size,
                    "need_proxy_change": True
                }

        # 检查是否是网络错误页面
        for pattern in self.error_patterns:
            if re.search(pattern, html_content, re.IGNORECASE):
                logger.warning("验证失败: 网络错误页面")
                return {
                    "valid": False,
                    "reason": "网络错误页面",
                    "content_size": content_size,
                    "need_proxy_change": True
                }

        # 检查是否包含有效百科页面的特征
        valid_features_count = 0
        for pattern in self.valid_patterns:
            if re.search(pattern, html_content, re.IGNORECASE):
                valid_features_count += 1

        # 如果没有找到足够的有效特征，可能是其他类型的错误页面
        if valid_features_count < 1:
            logger.warning(f"验证失败: 缺少百科页面特征 (只找到 {valid_features_count} 个特征)")
            return {
                "valid": False,
                "reason": "缺少百科页面特征",
                "content_size": content_size,
                "valid_features": valid_features_count,
                "need_proxy_change": True
            }

        # 内容看起来是有效的
        logger.info(f"内容验证通过: 大小 {content_size} 字节，找到 {valid_features_count} 个有效特征")
        return {
            "valid": True,
            "content_size": content_size,
            "valid_features": valid_features_count
        }