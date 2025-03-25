import logging
import os
import argparse
from config.settings import Config
from proxy.pool import ProxyPool
from processor.data_processor import DataProcessor, ProcessStage
from utils.logger import setup_logger


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='百度百科爬虫和解析工具')
    parser.add_argument('--stage', type=str, default='db_fetch', choices=['fetch', 'parse', 'full', 'db_fetch'],
                        help='处理阶段: fetch(仅爬取), parse(仅解析), full(完整流程), db_fetch(从数据库爬取)')
    args = parser.parse_args()

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
    logger.info(f"程序启动，运行阶段: {args.stage}")

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

        # 获取配置中的最小内容大小
        min_content_size = getattr(config, 'min_content_size', 1024)
        logger.info(f"设置最小内容大小: {min_content_size} 字节")

        # 初始化数据处理器
        processor = DataProcessor(
            config=config,  # 传递整个配置对象
            proxy_pool=proxy_pool,
            num_producers=config.num_producers,
            num_consumers=config.num_consumers,
            output_dir=config.output_dir,
            save_interval=config.save_interval,
            min_content_size=min_content_size
        )

        # 根据指定的阶段执行相应的处理
        if args.stage == 'db_fetch':
            logger.info("开始执行从数据库爬取阶段...")
            processor.process_db_fetch_stage()
            logger.info("从数据库爬取阶段完成")

        elif args.stage == 'fetch' or args.stage == 'full':
            logger.info("开始执行爬取阶段...")
            processor.process_fetch_stage()
            logger.info("爬取阶段完成")

        if args.stage == 'parse' or args.stage == 'full':
            logger.info("开始执行解析阶段...")
            processor.process_parse_stage()
            logger.info("解析阶段完成")

        logger.info("数据处理完成")

    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()