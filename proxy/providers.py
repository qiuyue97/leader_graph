from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import logging
import requests

# 获取日志器
logger = logging.getLogger(__name__)


class ProxyProvider(ABC):
    """Abstract base class for proxy providers"""

    def __init__(self, name: str = None):
        self.name = name or self.__class__.__name__

    @abstractmethod
    def get_proxies(self, count: int = 1) -> List[Dict[str, str]]:
        """Get a list of proxies

        Args:
            count: Number of proxies to fetch

        Returns:
            List of proxy dicts with 'http' and 'https' keys
        """
        pass

    def _format_proxy_url(self, ip: str, port: str) -> str:
        """Format IP and port into a proxy URL string"""
        return f"http://{ip}:{port}"

    def _create_proxy_dict(self, ip: str, port: str) -> Dict[str, str]:
        """Create a proxy dict with http and https entries"""
        proxy_url = self._format_proxy_url(ip, port)
        return {'http': proxy_url, 'https': proxy_url}


class ZDOpenProxyProvider(ProxyProvider):
    """代理提供者，适用于ZDOpen代理服务"""

    def __init__(self, api: str, akey: str, proxy_username: str, proxy_password: str, type: str = "3"):
        super().__init__(name="ZDOpen")
        self.api = api
        self.akey = akey
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.type = type
        self.api_url = f"http://www.zdopen.com/ShortProxy/GetIP?api={api}&akey={akey}&type={type}"

    def get_proxies(self, count: int = 5) -> List[Dict[str, str]]:
        """从ZDOpen API获取代理

        Args:
            count: 请求的代理数量（注意：实际返回数量由API决定）

        Returns:
            代理字典列表，每个字典包含'http'和'https'键
        """
        try:
            logger.info(f"从{self.name}获取代理: {self.api_url}")
            response = requests.get(self.api_url, timeout=10)

            if response.status_code == 200:
                data = response.json()

                if data.get("code") == "10001" and data.get("msg") == "获取成功":
                    proxy_list = data.get("data", {}).get("proxy_list", [])
                    proxies = []

                    for proxy_info in proxy_list:
                        proxy_url = self._format_proxy_url(proxy_info['ip'], proxy_info['port'])
                        proxies.append({'http': proxy_url, 'https': proxy_url})
                        logger.info(f"添加代理: {proxy_info['ip']}:{proxy_info['port']} (来自 {self.name})")

                    return proxies
            logger.warning(f"{self.name}未能获取代理")
            return []
        except Exception as e:
            logger.error(f"{self.name}获取代理时出错: {str(e)}")
            return []


class XiaoXiangProxyProvider(ProxyProvider):
    """Proxy provider for XiaoXiangDaili service"""

    def __init__(self, app_key: str, app_secret: str):
        super().__init__(name="XiaoXiangDaili")
        self.app_key = app_key
        self.app_secret = app_secret
        self.api_url = "https://api.xiaoxiangdaili.com/ip/get"

    def get_proxies(self, count: int = 1) -> List[Dict[str, str]]:
        """
        Get proxies from XiaoXiangDaili API

        Args:
            count: Number of proxies to fetch

        Returns:
            List of proxy dictionaries with 'http' and 'https' keys
        """
        try:
            logger.info(f"从 {self.name} 获取代理")
            params = {
                "appKey": self.app_key,
                "appSecret": self.app_secret,
                "count": count
            }
            response = requests.get(self.api_url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("data"):
                    proxies = []
                    for proxy_info in data["data"]:
                        proxies.append(self._create_proxy_dict(proxy_info['ip'], proxy_info['port']))
                        logger.info(f"添加代理: {proxy_info['ip']}:{proxy_info['port']} (来自 {self.name})")

                    return proxies
                else:
                    error_msg = data.get("message", "Unknown error")
                    logger.warning(f"{self.name} 获取代理失败: {error_msg}")
            else:
                logger.warning(f"{self.name} 请求失败，状态码: {response.status_code}")

            logger.warning(f"从 {self.name} 未能获取代理")
            return []

        except requests.exceptions.Timeout:
            logger.error(f"{self.name} 请求超时")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"{self.name} 请求异常: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"{self.name} 获取代理时发生未知错误: {str(e)}")
            return []


# 工厂函数，简化代理提供者的创建
def create_proxy_provider(provider_type: str, **kwargs) -> Optional[ProxyProvider]:
    """
    Factory function to create proxy providers

    Args:
        provider_type: Type of provider ("zdopen" or "xiaoxiang")
        **kwargs: Provider-specific parameters

    Returns:
        ProxyProvider instance or None if type is invalid
    """
    if provider_type.lower() == "zdopen":
        return ZDOpenProxyProvider(
            api=kwargs.get("api"),
            akey=kwargs.get("akey"),
            proxy_username=kwargs.get("proxy_username"),
            proxy_password=kwargs.get("proxy_password"),
            type=kwargs.get("type", "3")
        )
    elif provider_type.lower() == "xiaoxiang":
        return XiaoXiangProxyProvider(
            app_key=kwargs.get("app_key"),
            app_secret=kwargs.get("app_secret")
        )
    else:
        logger.error(f"未知的代理提供者类型: {provider_type}")
        return None