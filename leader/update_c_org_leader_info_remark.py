import logging
import os
from config.settings import Config
from proxy.pool import ProxyPool
from processor.data_processor import DataProcessor
from utils.logger import setup_logger

def update_c_org_leader_info_remark(config_path, update):
    # 设置日志
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    # 使用模块级日志器
    logger = setup_logger(
        name=__name__,
        log_file=f"{log_dir}/baike_scraper.log",
        level=logging.INFO,
        console_output=True
    )

    try:
        config = Config.from_file(config_path)
        logger.info(
            f"已加载配置: {config.num_producers} 个生产者, {config.num_consumers} 个消费者")

        # 创建代理池（如果启用）
        proxy_pool = None
        if config.use_proxy:
            from proxy import create_proxy_provider

            providers = []
            for provider_config in config.proxy_config.get('providers', []):
                provider_type = provider_config.get('type')
                provider = create_proxy_provider(provider_type, **provider_config)
                if provider:
                    providers.append(provider)

            if providers:
                proxy_pool = ProxyPool(
                    proxy_providers=providers,
                    refresh_interval=config.proxy_config.get('refresh_interval', 15),
                    min_proxies=config.proxy_config.get('min_proxies', 10)
                )
                logger.info(f"已初始化代理池，使用 {len(providers)} 个代理提供者")
            else:
                logger.warning("没有可用的代理提供者，将不使用代理")

        # 获取配置中的最小内容大小
        min_content_size = getattr(config, 'min_content_size', 1024)
        logger.info(f"设置最小内容大小: {min_content_size} 字节")

        # 初始化数据处理器
        processor = DataProcessor(
            config=config,
            proxy_pool=proxy_pool,
            num_producers=config.num_producers,
            num_consumers=config.num_consumers,
            save_interval=config.save_interval,
            min_content_size=min_content_size,
            update=update
        )

        processor.process_db_fetch_stage()

        logger.info("数据处理完成")

    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
