import pymysql


def create_org_leader_info_table():
    """
    创建cnfic_leader库下的c_org_leader_info表
    """
    # 数据库连接配置
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'wlh3338501',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }

    # 建表DDL
    create_database_sql = "CREATE DATABASE IF NOT EXISTS cnfic_leader;"

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS cnfic_leader.c_org_leader_info (
        id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增id',
        uuid VARCHAR(64) NOT NULL COMMENT '生成uuid',
        org_info_id VARCHAR(255) COMMENT '机构信息id,多个用逗号分隔',
        org_info_uuid VARCHAR(255) COMMENT '机构信息uuid,多个用逗号分隔',
        org_name VARCHAR(255) COMMENT '机构中文名,多个用逗号分隔',
        leader_name VARCHAR(100) COMMENT '领导姓名',
        leader_position VARCHAR(255) COMMENT '职务',
        current_position TEXT COMMENT '现任职',
        leader_profile TEXT COMMENT '人物简介',
        gender VARCHAR(10) COMMENT '性别',
        nationality VARCHAR(50) COMMENT '国籍',
        ethnic_group VARCHAR(50) COMMENT '民族',
        birth_place VARCHAR(100) COMMENT '籍贯',
        birth_date VARCHAR(50) COMMENT '出生日期',
        alma_mater TEXT COMMENT '毕业院校',
        degree VARCHAR(100) COMMENT '学位',
        edu_level VARCHAR(50) COMMENT '学历',
        political_status VARCHAR(50) COMMENT '政治面貌',
        professional_title VARCHAR(100) COMMENT '职称',
        career_history TEXT COMMENT '人物履历',
        career_history_structured TEXT COMMENT '结构化人物履历',
        held_position TEXT COMMENT '担任职务',
        position_change TEXT COMMENT '职务任免',
        work_division TEXT COMMENT '工作分工',
        leader_honor TEXT COMMENT '所获荣誉',
        published_book TEXT COMMENT '出版图书',
        research_project TEXT COMMENT '科研项目',
        written_work TEXT COMMENT '人物著作',
        social_position TEXT COMMENT '社会兼职',
        achievement TEXT COMMENT '人物成就',
        image_url VARCHAR(500) COMMENT '图像链接',
        remark LONGTEXT COMMENT '备注(存放全量html)',
        source_url VARCHAR(500) COMMENT '来源链接',
        is_deleted TINYINT(1) DEFAULT 0 COMMENT '是否删除(0-未删除,1-已删除)',
        create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (id),
        UNIQUE KEY uk_uuid (uuid),
        KEY idx_org_info_id (org_info_id),
        KEY idx_leader_name (leader_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='组织领导人信息表';
    """

    try:
        # 连接MySQL
        conn = pymysql.connect(**db_config)

        try:
            with conn.cursor() as cursor:
                # 创建数据库
                cursor.execute(create_database_sql)
                print("数据库cnfic_leader创建成功或已存在")

                # 创建表
                cursor.execute(create_table_sql)
                conn.commit()
                print("表c_org_leader_info创建成功")

        finally:
            conn.close()

    except Exception as e:
        print(f"错误: {e}")


if __name__ == "__main__":
    create_org_leader_info_table()