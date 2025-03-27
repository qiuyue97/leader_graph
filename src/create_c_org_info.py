import hashlib
import pandas as pd
import mysql.connector
from urllib.parse import quote


def generate_department_id(name):
    """
    根据部门名称生成一个一致的32位ID
    使用MD5哈希确保相同名称总是生成相同的ID

    参数:
        name (str): 部门名称

    返回:
        str: 32位十六进制ID
    """
    hash_object = hashlib.md5(name.encode())
    return hash_object.hexdigest()


def extract_department_info(input_file, primary_col="一级部门", secondary_col="二级部门", province_col="省份",
                            type_col="部门类型", url_col="URL"):
    """
    从输入文件中提取部门信息，处理表格中一级部门可能为空的情况

    参数:
        input_file (str): 输入文件路径
        primary_col (str): 一级部门列名
        secondary_col (str): 二级部门列名
        province_col (str): 省份列名
        type_col (str): 部门类型列名
        url_col (str): URL列名

    返回:
        list: 部门信息字典列表
    """
    # 读取输入文件
    if input_file.endswith('.csv'):
        df = pd.read_csv(input_file)
    elif input_file.endswith(('.xls', '.xlsx')):
        df = pd.read_excel(input_file)
    else:
        raise ValueError("不支持的文件格式。请使用CSV或Excel文件。")

    # 检查列是否存在
    if primary_col not in df.columns:
        raise ValueError(f"找不到一级部门列 '{primary_col}'")
    if secondary_col not in df.columns:
        raise ValueError(f"找不到二级部门列 '{secondary_col}'")

    # 检查其他可选列是否存在
    has_province = province_col in df.columns
    has_type = type_col in df.columns
    has_url = url_col in df.columns

    # 预处理：填充空缺的一级部门（向上填充）
    current_primary_dept = None
    filled_primary_depts = []

    for idx, row in df.iterrows():
        primary_dept = row.get(primary_col)
        if not pd.isna(primary_dept) and primary_dept != '/' and primary_dept != '':
            current_primary_dept = str(primary_dept).strip()
        filled_primary_depts.append(current_primary_dept)

    # 将填充后的一级部门添加到数据框
    df['filled_primary_dept'] = filled_primary_depts

    # 提取部门信息
    departments = []
    dept_id_map = {}  # 部门名称到ID的映射

    # 处理一级部门
    for _, row in df.iterrows():
        primary_dept = row.get('filled_primary_dept')
        province_value = row.get(province_col, "") if has_province else ""
        type_value = row.get(type_col, "") if has_type else ""
        url_value = row.get(url_col, "") if has_url else ""

        if pd.isna(primary_dept) or primary_dept is None:
            continue

        # 如果尚未生成部门ID，则生成
        if primary_dept not in dept_id_map:
            dept_id = generate_department_id(primary_dept)
            dept_id_map[primary_dept] = dept_id

            # 清理省份值
            if pd.isna(province_value):
                province_value = ""
            else:
                province_value = str(province_value).strip()

            # 清理部门类型值
            if pd.isna(type_value):
                type_value = ""
            else:
                type_value = str(type_value).strip()

            # 处理URL值
            if pd.isna(url_value):
                url_value = ""
            else:
                url_value = str(url_value).strip()

            # 创建部门信息
            dept_info = {
                "uuid": dept_id,  # 使用MD5哈希作为UUID
                "org_name": primary_dept,
                "org_region": province_value,
                "org_type": type_value,
                "source_url": url_value,  # 使用从文件读取的URL，不再生成
                "org_level": 1,
                "parent_uuid": None
            }
            departments.append(dept_info)

    # 处理二级部门
    for _, row in df.iterrows():
        secondary_dept = row.get(secondary_col)
        primary_dept = row.get('filled_primary_dept')  # 使用填充后的一级部门
        province_value = row.get(province_col, "") if has_province else ""
        type_value = row.get(type_col, "") if has_type else ""
        url_value = row.get(url_col, "") if has_url else ""

        if pd.isna(secondary_dept) or secondary_dept == '/' or secondary_dept == '':
            continue

        if pd.isna(primary_dept) or primary_dept is None:
            parent_id = None
        else:
            parent_id = dept_id_map.get(primary_dept, None)

        # 清理二级部门名称和省份
        secondary_dept = str(secondary_dept).strip()
        if pd.isna(province_value):
            province_value = ""
        else:
            province_value = str(province_value).strip()

        # 清理部门类型值
        if pd.isna(type_value):
            type_value = ""
        else:
            type_value = str(type_value).strip()

        # 处理URL值
        if pd.isna(url_value):
            url_value = ""
        else:
            url_value = str(url_value).strip()

        # 如果尚未生成部门ID，则生成
        if secondary_dept not in dept_id_map:
            dept_id = generate_department_id(secondary_dept)
            dept_id_map[secondary_dept] = dept_id

            # 创建部门信息
            dept_info = {
                "uuid": dept_id,  # 使用MD5哈希作为UUID
                "org_name": secondary_dept,
                "org_region": province_value,
                "org_type": type_value,
                "source_url": url_value,  # 使用从文件读取的URL，不再生成
                "org_level": 2,
                "parent_uuid": parent_id
            }
            departments.append(dept_info)
        else:
            # 检查是否有同名二级部门但父部门不同的情况
            existing_dept = None
            for dept in departments:
                if dept["org_name"] == secondary_dept and dept["org_level"] == 2:
                    existing_dept = dept
                    break

            # 如果该部门已存在，但父部门ID不同，说明这是同名部门在不同父部门下的情况
            if existing_dept and existing_dept["parent_uuid"] != parent_id and parent_id:
                # 为这个同名但父部门不同的二级部门创建新的ID
                unique_name = f"{secondary_dept}_{primary_dept}"
                dept_id = generate_department_id(unique_name)

                # 创建新的部门信息
                dept_info = {
                    "uuid": dept_id,  # 使用同名但父部门不同的情况生成的唯一ID
                    "org_name": secondary_dept,
                    "org_region": province_value,
                    "org_type": type_value,
                    "source_url": url_value,  # 使用从文件读取的URL，不再生成
                    "org_level": 2,
                    "parent_uuid": parent_id
                }
                departments.append(dept_info)

    return departments


def setup_database(db_config):
    """
    设置数据库和表结构

    参数:
        db_config (dict): 数据库配置信息
    """
    try:
        # 连接到MySQL服务器（不指定数据库）
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password']
        )
        cursor = conn.cursor()

        # 创建数据库SQL
        create_db_sql = f"""
        CREATE DATABASE IF NOT EXISTS {db_config['database']} 
        DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """
        cursor.execute(create_db_sql)
        print(f"数据库 '{db_config['database']}' 创建成功或已存在")

        # 使用数据库
        cursor.execute(f"USE {db_config['database']}")

        # 创建组织机构信息表
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS c_org_info (
            id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '自增id',
            uuid VARCHAR(36) NOT NULL COMMENT '唯一标识符',
            org_name VARCHAR(255) NOT NULL COMMENT '机构中文名',
            org_name_en VARCHAR(500) COMMENT '机构外文名',
            org_abbr VARCHAR(100) COMMENT '机构简称',
            org_alias VARCHAR(255) COMMENT '机构别名',
            org_type VARCHAR(100) COMMENT '机构类型',
            aff_org VARCHAR(255) COMMENT '隶属机构',
            parent_org VARCHAR(255) COMMENT '上级机构',
            sup_org VARCHAR(255) COMMENT '主管单位',
            establish_date VARCHAR(50) COMMENT '成立时间',
            office_addr VARCHAR(500) COMMENT '办公地址',
            org_nature VARCHAR(100) COMMENT '性质',
            adm_level VARCHAR(50) COMMENT '行政级别',
            current_leader VARCHAR(100) COMMENT '现任领导',
            org_region VARCHAR(100) COMMENT '所属地区',
            province_profile TEXT COMMENT '省情概况',
            org_profile TEXT COMMENT '机构简介',
            org_duty TEXT COMMENT '主要职责',
            internal_dept TEXT COMMENT '内设机构（机构设置）',
            org_history TEXT COMMENT '历史沿革',
            org_coop TEXT COMMENT '战略合作',
            org_staff TEXT COMMENT '人员编制',
            org_honor TEXT COMMENT '获得荣誉',
            remark LONGTEXT COMMENT '备注（网页文本）',
            source_url VARCHAR(1000) COMMENT '来源链接',
            is_deleted TINYINT(1) DEFAULT 0 COMMENT '是否删除(1-是；0-否)',
            create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
            org_level INT COMMENT '机构级别',
            parent_uuid VARCHAR(36) COMMENT '上级机构UUID',
            UNIQUE KEY idx_uuid (uuid) COMMENT 'UUID唯一索引'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='组织机构信息表';
        """
        cursor.execute(create_table_sql)
        print("表 'c_org_info' 创建成功或已存在")

    except mysql.connector.Error as error:
        print(f"数据库设置错误: {error}")
    finally:
        # 关闭连接
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("数据库连接已关闭")


def insert_into_database(departments, db_config):
    """
    将部门信息插入到MySQL数据库中

    参数:
        departments (list): 部门信息字典列表
        db_config (dict): 数据库配置信息
    """
    try:
        # 建立数据库连接
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        cursor = conn.cursor()

        # 插入数据
        for dept in departments:
            # 准备SQL插入语句
            sql = """
            INSERT INTO c_org_info (
                uuid, org_name, org_region, org_type, source_url, org_level, parent_uuid
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                dept['uuid'],
                dept['org_name'],
                dept['org_region'],
                dept['org_type'],
                dept['source_url'],
                dept['org_level'],
                dept['parent_uuid']
            )

            # 执行SQL
            cursor.execute(sql, values)

        # 提交事务
        conn.commit()
        print(f"成功导入 {len(departments)} 条组织机构记录到数据库")

    except mysql.connector.Error as error:
        print(f"数据库错误: {error}")
        # 回滚更改
        if conn.is_connected():
            conn.rollback()
    finally:
        # 关闭连接
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("数据库连接已关闭")


def main():
    """主函数"""
    # 输入文件参数
    input_file = "../data/input_安徽省_0327.xlsx"  # 输入文件路径
    primary_col = "一级部门"  # 一级部门列名
    secondary_col = "二级部门"  # 二级部门列名
    province_col = "省份"  # 省份列名
    type_col = "部门类型"  # 部门类型列名
    url_col = "URL"  # URL列名

    # 数据库配置
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'wlh3338501',
        'database': 'cnfic_leader'
    }

    # 设置数据库和表结构
    setup_database(db_config)

    # 提取部门信息
    departments = extract_department_info(
        input_file,
        primary_col=primary_col,
        secondary_col=secondary_col,
        province_col=province_col,
        type_col=type_col,
        url_col=url_col  # 添加URL列名参数
    )

    # 打印到控制台
    print(f"从 {input_file} 中提取了 {len(departments)} 条组织机构信息")

    # 将部门信息插入到数据库
    insert_into_database(departments, db_config)


if __name__ == "__main__":
    main()