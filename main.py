import logging
import os
from config.settings import Config
from proxy.pool import ProxyPool
from processor.data_processor import DataProcessor
from utils.logger import setup_logger


def main():
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
    logger.info("程序启动")

    try:
        # 加载配置
        config_path = './config.yaml'

        # 如果配置文件不存在，创建一个示例配置
        if not os.path.exists(config_path):
            logger.info("配置文件不存在，创建示例配置")
            Config.create_example_config(config_path)
            logger.info(f"已创建示例配置文件: {config_path}，请修改后重新运行程序")
            return

        config = Config.from_file(config_path)
        logger.info(
            f"已加载配置: 输入文件 {config.input_csv_path}, {config.num_producers} 个生产者, {config.num_consumers} 个消费者")

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

        # 初始化数据处理器
        processor = DataProcessor(
            input_csv_path=config.input_csv_path,
            proxy_pool=proxy_pool,
            num_producers=config.num_producers,
            num_consumers=config.num_consumers,
            output_dir=config.output_dir,
            save_interval=config.save_interval
        )

        # 开始处理数据
        logger.info("开始处理数据...")
        processor.process_data()
        logger.info("数据处理完成")

    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()