import threading
import time
from typing import List, Dict, Union, Optional
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

        # 添加失败代理黑名单
        self.failed_proxies = set()

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

    def return_proxy(self, proxy: Dict[str, str], mark_as_failed: bool = False) -> None:
        """
        返回代理到池中或标记为失败

        Args:
            proxy: 代理字典
            mark_as_failed: 是否标记为失败的代理
        """
        if not proxy:
            return

        with self.lock:
            proxy_str = str(proxy)

            if mark_as_failed:
                # 将代理添加到黑名单
                self.failed_proxies.add(proxy_str)
                logger.warning(f"代理已标记为失败，加入黑名单")
            else:
                # 检查代理是否在黑名单中
                if proxy_str in self.failed_proxies:
                    logger.info(f"代理在黑名单中，不会重新添加到池")
                    return

                # 重置使用计数并返回到池中
                proxy['_usage_count'] = 0
                self.proxies.append(proxy)
                logger.info(f"代理已返回到池，当前池中有 {len(self.proxies)} 个代理")

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
                # 移除重复的代理和黑名单代理
                unique_proxies = []
                seen = set()
                for proxy in self.proxies:
                    proxy_str = str(proxy)
                    if proxy_str not in seen and proxy_str not in self.failed_proxies:
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

    def clear_failed_proxies(self) -> int:
        """
        清除失败代理黑名单

        Returns:
            清除的代理数量
        """
        with self.lock:
            count = len(self.failed_proxies)
            self.failed_proxies.clear()
            logger.info(f"已清除 {count} 个失败代理记录")
            return count