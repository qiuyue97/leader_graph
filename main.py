from org import *
from leader import *
from config.settings import Config
import os
from utils.logger import setup_logger
import logging

def main():
    # 设置日志
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    # 使用模块级日志器
    logger = setup_logger(
        name=__name__,
        log_file=f"{log_dir}/main.log",
        level=logging.INFO,
        console_output=True
    )
    config_path = './config.yaml'
    # 如果配置文件不存在，创建一个示例配置
    if not os.path.exists(config_path):
        logger.info("配置文件不存在，创建示例配置")
        Config.create_example_config(config_path)
        logger.info(f"已创建示例配置文件: {config_path}，请修改后重新运行程序")
        return
    config = Config.from_file(config_path)
    # 1. 从input_data_dir创建2张表
    create_c_org_info(input_directory=config.input_data_dir, db_config=config.db_config)
    create_org_leader_info_table(db_config=config.db_config)
    # 2. 爬取部门信息
    fetch_and_store_html(db_config=config.db_config, update=False)
    # 3. 从爬取到的部门信息中抽取字段
    extract_org_info()
    # 4. 从部门信息中找到领导信息并更新领导表
    update_c_org_leader_info(db_config=config.db_config)
    # 5. 爬取领导人信息
    update_c_org_leader_info_remark(config_path=config_path, update=False)
    # 6. 抽取领导人信息
    extract_org_leader_info()
    update_leader_img_url(db_config=config.db_config)
    # 7. 结构化领导人履历信息
    bio_processor(config_path=config_path, cost_limit=config.cost_limit, update=False)

if __name__ == "__main__":
    main()