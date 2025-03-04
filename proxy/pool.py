import threading
import time
from typing import List, Dict, Union, Optional
import random
from .providers import ProxyProvider

# 获取日志器
from utils.logger import get_logger
logger = get_logger(__name__)


class ProxyPool:
    """Manages a pool of proxies from multiple providers"""

    def __init__(self, proxy_providers: Union[ProxyProvider, List[ProxyProvider]],
                 refresh_interval: int = 15,
                 min_proxies: int = 10):
        """
        Initialize the proxy pool

        Args:
            proxy_providers: One or more proxy providers
            refresh_interval: Time in seconds between proxy refreshes
            min_proxies: Minimum number of proxies to maintain in the pool
        """
        self.proxies = []
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.proxy_providers = proxy_providers if isinstance(proxy_providers, list) else [proxy_providers]
        self.refresh_interval = refresh_interval
        self.min_proxies = min_proxies

        # 启动定期刷新线程
        self.stop_refresh = False
        self.refresh_thread = threading.Thread(target=self._periodic_refresh, daemon=True)
        self.refresh_thread.start()
        logger.info(f"启动了代理池定期刷新线程 (每{refresh_interval}秒刷新一次)")

    def get_proxy(self) -> Optional[Dict[str, str]]:
        """
        Get a proxy from the pool and remove it

        Returns:
            A proxy dict or None if no proxies are available
        """
        with self.condition:
            # 如果代理池为空，等待条件变量通知
            if not self.proxies:
                logger.info("代理池为空，等待刷新...")
                self.condition.wait(30)  # 最多等待30秒

            # 如果等待后仍然为空，返回None
            if not self.proxies:
                logger.error("等待超时，代理池仍为空")
                return None

            # 获取代理并立即从池中移除
            proxy = self.proxies.pop()
            logger.info(f"获取并移除一个代理，剩余 {len(self.proxies)} 个代理")

            # 初始化代理使用计数
            proxy['_usage_count'] = 0

            return proxy

    def get_proxy_count(self) -> int:
        """返回当前可用代理数量"""
        with self.lock:
            count = len(self.proxies)
            return count

    def _refresh_proxies(self, count: int = 5) -> bool:
        """从多个代理提供者获取代理"""
        new_proxies = []

        for provider in self.proxy_providers:
            try:
                logger.info(f"从提供者 {provider.name} 获取代理...")
                provider_proxies = provider.get_proxies(count)

                if provider_proxies:
                    new_proxies.extend(provider_proxies)
                    logger.info(f"从提供者 {provider.name} 获取了 {len(provider_proxies)} 个代理")
                else:
                    logger.warning(f"提供者 {provider.name} 返回了空的代理列表")
            except Exception as e:
                logger.error(f"从提供者 {provider.name} 获取代理时发生错误: {str(e)}")

        with self.lock:
            if new_proxies:
                # 累积代理而不是替换
                self.proxies.extend(new_proxies)
                # 移除重复的代理
                unique_proxies = []
                seen = set()
                for proxy in self.proxies:
                    proxy_str = str(proxy)
                    if proxy_str not in seen:
                        seen.add(proxy_str)
                        unique_proxies.append(proxy)

                self.proxies = unique_proxies
                logger.info(f"刷新成功，代理池现在有 {len(self.proxies)} 个代理")

                # 通知等待的线程
                self.condition.notify_all()
                return True
            else:
                logger.warning("刷新失败，未能获取任何代理")
                return False

    def _periodic_refresh(self) -> None:
        """定期刷新代理池的线程函数"""
        while not self.stop_refresh:
            logger.info(f"定期刷新: 开始获取新代理...")
            success = self._refresh_proxies(self.min_proxies)  # 每次刷新至少获取min_proxies个代理

            if success:
                logger.info("定期刷新成功")
            else:
                logger.warning("定期刷新未能获取到代理")

            # 固定等待refresh_interval秒后再次刷新
            time.sleep(self.refresh_interval)

    def shutdown(self) -> None:
        """关闭代理池，停止刷新线程"""
        self.stop_refresh = True
        logger.info("代理池关闭，停止刷新线程")

    @classmethod
    def from_config(cls, config: dict) -> 'ProxyPool':
        """
        Create a proxy pool from configuration

        Args:
            config: Configuration dictionary with providers and pool settings

        Returns:
            Configured ProxyPool instance
        """
        providers = []

        # 创建提供者
        for provider_config in config.get('providers', []):
            provider_type = provider_config.get('type')
            if provider_type == 'zdopen':
                from .providers import ZDOpenProxyProvider
                provider = ZDOpenProxyProvider(
                    api=provider_config.get('api'),
                    akey=provider_config.get('akey'),
                    proxy_username=provider_config.get('proxy_username'),
                    proxy_password=provider_config.get('proxy_password'),
                    type=provider_config.get('type', '3')
                )
                providers.append(provider)
            elif provider_type == 'xiaoxiang':
                from .providers import XiaoXiangProxyProvider
                provider = XiaoXiangProxyProvider(
                    app_key=provider_config.get('app_key'),
                    app_secret=provider_config.get('app_secret')
                )
                providers.append(provider)

        # 创建代理池
        return cls(
            proxy_providers=providers,
            refresh_interval=config.get('refresh_interval', 15),
            min_proxies=config.get('min_proxies', 20)
        )