from org import *
from leader import *
from config.settings import Config

def main():
    config_path = './config.yaml'
    input_data_dir = './data/input_data'
    config = Config.from_file(config_path)
    # 1. 从input_data_dir创建2张表
    create_c_org_info(input_directory=input_data_dir, db_config=config.db_config)
    create_org_leader_info_table(db_config=config.db_config)
    # 2. 爬取部门信息
    fetch_and_store_html(db_config=config.db_config)
    # 3. 从爬取到的部门信息中抽取字段
    extract_org_info()
    # 4. 从部门信息中找到领导信息并更新领导表
    update_c_org_leader_info(db_config=config.db_config)
    # 4. 爬取领导人信息
    update_c_org_leader_info_remark(config_path=config_path)
    # 5. 抽取领导人信息
    extract_org_leader_info()
    update_leader_img_url(db_config=config.db_config)
    # 6. 结构化领导人履历信息
    bio_processor(config_path=config_path, cost_limit=5.0)

if __name__ == "__main__":
    main()