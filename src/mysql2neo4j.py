#!/usr/bin/env python3
"""
sql2neo4j.py
从SQL数据库(cnfic_leader)中导入数据到Neo4j图数据库，建立领导人和组织的关系网络
"""

import os
import sys
import time
import logging
import pymysql
from neo4j import GraphDatabase
import json

# 添加项目根目录到模块搜索路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import Config as basicConfig

# 设置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sql2neo4j.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sql2neo4j")


# 配置信息
class Config:
    # 配置文件路径
    config_path = '../config.yaml'
    try:
        config = basicConfig.from_file(config_path)
        neo4j_config = config.neo4j_config
        db_config = config.db_config
    except Exception as e:
        logger.error(f"加载配置文件失败: {e}")


# 自定义进度条
def print_progress_bar(iteration, total, prefix='', decimals=1, length=50, fill='█'):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    sys.stdout.write(f'\r{prefix} |{bar}| {percent}% {iteration} / {total}')
    sys.stdout.flush()
    if iteration == total:
        sys.stdout.write('\n')
        sys.stdout.flush()


# 从MySQL获取数据
class MySQLClient:
    """与MySQL数据库交互的客户端类"""

    def __init__(self, config):
        """初始化MySQL客户端"""
        self.config = config
        self.connection = None
        self.connect()

    def connect(self):
        """连接到MySQL数据库"""
        try:
            self.connection = pymysql.connect(
                host=self.config['host'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                charset=self.config['charset'],
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info(f"成功连接到MySQL数据库: {self.config['database']}")
        except Exception as e:
            logger.error(f"连接MySQL数据库失败: {e}")
            raise

    def disconnect(self):
        """断开MySQL数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("MySQL数据库连接已关闭")

    def get_all_organizations(self):
        """获取所有组织记录"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT  uuid, org_name, org_name_en, org_abbr, org_alias, org_type, aff_org, parent_org,
                        sup_org, establish_date, office_addr, org_nature, adm_level, current_leader, org_region,
                        org_level, parent_uuid
                FROM c_org_info 
                WHERE is_deleted = 0
                """
                cursor.execute(sql)
                results = cursor.fetchall()
                logger.info(f"从数据库获取了 {len(results)} 条组织记录")
                return results
        except Exception as e:
            logger.error(f"获取组织记录失败: {e}")
            return []

    def get_all_leaders(self):
        """获取所有领导人记录"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT uuid, org_info_id, org_info_uuid, org_name, leader_name, 
                       leader_position, leader_profile, gender, 
                       nationality, ethnic_group, birth_place, birth_date, 
                       alma_mater, degree, edu_level, political_status, 
                       professional_title, image_url, career_history_structured
                FROM c_org_leader_info 
                WHERE is_deleted = 0
                """
                cursor.execute(sql)
                results = cursor.fetchall()
                logger.info(f"从数据库获取了 {len(results)} 条领导人记录")
                return results
        except Exception as e:
            logger.error(f"获取领导人记录失败: {e}")
            return []

    def get_departments(self):
        """获取部门数据"""
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT id, uuid, org_name, parent_uuid 
                FROM c_org_info 
                WHERE is_deleted = 0
                """
                cursor.execute(sql)
                results = cursor.fetchall()

                # 转换为字典形式以便快速查找
                departments = {}
                for dept in results:
                    departments[dept['id']] = dept

                logger.info(f"从数据库获取了 {len(departments)} 条部门数据")
                return departments
        except Exception as e:
            logger.error(f"获取部门数据失败: {e}")
            return {}


# Neo4j数据导入主类
class Neo4jImporter:
    """导入数据到Neo4j图数据库的类"""

    def __init__(self, neo4j_config, db_config):
        """初始化Neo4j导入器"""
        self.neo4j_config = neo4j_config
        self.db_config = db_config
        self.driver = None
        self.mysql_client = None

    def connect(self):
        """连接到Neo4j数据库"""
        try:
            self.driver = GraphDatabase.driver(
                self.neo4j_config['uri'],
                auth=(self.neo4j_config['user'], self.neo4j_config['password'])
            )
            logger.info(f"成功连接到Neo4j数据库: {self.neo4j_config['uri']}")

            # 连接到MySQL
            self.mysql_client = MySQLClient(self.db_config)

            return True
        except Exception as e:
            logger.error(f"连接数据库失败: {e}")
            return False

    def disconnect(self):
        """断开数据库连接"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j数据库连接已关闭")

        if self.mysql_client:
            self.mysql_client.disconnect()

    def create_indexes(self):
        """创建Neo4j索引以提高性能"""
        with self.driver.session() as session:
            indexes = [
                "CREATE INDEX person_id_index IF NOT EXISTS FOR (p:Person) ON (p.person_id)",
                "CREATE INDEX person_uuid_index IF NOT EXISTS FOR (p:Person) ON (p.uuid)",
                "CREATE INDEX organization_id_index IF NOT EXISTS FOR (o:Organization) ON (o.org_id)",
                "CREATE INDEX organization_uuid_index IF NOT EXISTS FOR (o:Organization) ON (o.uuid)"
            ]
            for index in indexes:
                session.run(index)
            logger.info("已创建Neo4j索引")

    def process_organization_hierarchy(self, organizations):
        """处理组织之间的层级关系"""
        logger.info("处理组织层级关系...")
        count = 0

        with self.driver.session() as session:
            for org in organizations:
                if org.get('parent_uuid'):
                    # 通过parent_uuid建立BELONGS_TO关系
                    cypher = """
                    MATCH (child:Organization {uuid: $child_uuid})
                    MATCH (parent:Organization {uuid: $parent_uuid})
                    MERGE (child)-[:BELONGS_TO]->(parent)
                    """

                    params = {
                        'child_uuid': org['uuid'],
                        'parent_uuid': org['parent_uuid']
                    }

                    session.run(cypher, params)
                    count += 1

        logger.info(f"已创建 {count} 个组织层级关系")

    def create_same_hometown_relationships(self):
        """创建人物之间的同乡关系，包含同市和同区县判断"""
        logger.info("创建人物之间的同乡关系...")

        with self.driver.session() as session:
            # 创建同乡关系
            cypher = """
            MATCH (p1:Person)
            WHERE p1.birth_place IS NOT NULL AND p1.birth_place <> ''
            WITH p1.birth_place AS place, COLLECT(p1) AS people
            UNWIND people AS p1
            UNWIND people AS p2
            WITH p1, p2, place
            WHERE id(p1) < id(p2)  // 确保只创建一个方向的关系
            MERGE (p1)-[r:SAME_HOMETOWN {
                birth_place: place, 
                same_city: "TODO", 
                same_district: "TODO"
            }]->(p2)
            RETURN count(r) AS rel_count
            """

            result = session.run(cypher)
            rel_count = result.single()["rel_count"]
            logger.info(f"已创建 {rel_count} 个同乡关系，包含同市和同区县属性")

    def create_schoolmates_relationships(self):
        """创建人物之间的同学关系，优化版，包含同院系和同专业判断"""
        logger.info("创建人物之间的同学关系...")

        with self.driver.session() as session:
            cypher = """
            // 查找符合条件的学习关系
            MATCH (p1:Person)-[r1:STUDIED_AT]->(s:Organization)<-[r2:STUDIED_AT]-(p2:Person)
            // 过滤条件：不是同一个人，学校不是中央党校，确保只处理一对人的一种方向
            WHERE p1 <> p2 
            AND s.org_name <> '中央党校'
            AND id(p1) < id(p2)

            WITH p1, p2, s, r1, r2,
                // 计算是否时间重叠
                CASE
                    WHEN r1.startYear IS NOT NULL AND r1.endYear IS NOT NULL AND 
                        r2.startYear IS NOT NULL AND r2.endYear IS NOT NULL
                    THEN (r1.startYear * 12 + COALESCE(r1.startMonth, 1)) <= (r2.endYear * 12 + COALESCE(r2.endMonth, 12)) AND
                        (r2.startYear * 12 + COALESCE(r2.startMonth, 1)) <= (r1.endYear * 12 + COALESCE(r1.endMonth, 12))
                    ELSE false
                END as atTheSameTime,
                s.org_name as school,

                // 计算重叠起始年月
                CASE
                    WHEN r1.startYear IS NOT NULL AND r2.startYear IS NOT NULL
                    THEN CASE WHEN r1.startYear > r2.startYear THEN r1.startYear ELSE r2.startYear END
                END as overlapStartYear,

                CASE
                    WHEN r1.startYear IS NOT NULL AND r2.startYear IS NOT NULL AND
                        r1.startYear = r2.startYear AND
                        r1.startMonth IS NOT NULL AND r2.startMonth IS NOT NULL
                    THEN CASE WHEN r1.startMonth > r2.startMonth THEN r1.startMonth ELSE r2.startMonth END
                    WHEN r1.startYear IS NOT NULL AND r2.startYear IS NOT NULL AND r1.startYear > r2.startYear
                    THEN COALESCE(r1.startMonth, 1)
                    WHEN r1.startYear IS NOT NULL AND r2.startYear IS NOT NULL AND r2.startYear > r1.startYear
                    THEN COALESCE(r2.startMonth, 1)
                END as overlapStartMonth,

                // 计算重叠结束年月
                CASE
                    WHEN r1.endYear IS NOT NULL AND r2.endYear IS NOT NULL
                    THEN CASE WHEN r1.endYear < r2.endYear THEN r1.endYear ELSE r2.endYear END
                END as overlapEndYear,

                CASE
                    WHEN r1.endYear IS NOT NULL AND r2.endYear IS NOT NULL AND
                        r1.endYear = r2.endYear AND
                        r1.endMonth IS NOT NULL AND r2.endMonth IS NOT NULL
                    THEN CASE WHEN r1.endMonth < r2.endMonth THEN r1.endMonth ELSE r2.endMonth END
                    WHEN r1.endYear IS NOT NULL AND r2.endYear IS NOT NULL AND r1.endYear < r2.endYear
                    THEN COALESCE(r1.endMonth, 12)
                    WHEN r1.endYear IS NOT NULL AND r2.endYear IS NOT NULL AND r2.endYear < r1.endYear
                    THEN COALESCE(r2.endMonth, 12)
                END as overlapEndMonth

            // 处理重叠时间段的格式化
            WITH p1, p2, school, atTheSameTime,
                overlapStartYear, overlapStartMonth, overlapEndYear, overlapEndMonth

            WITH p1, p2, school, atTheSameTime,
                CASE 
                    WHEN atTheSameTime AND overlapStartYear IS NOT NULL AND overlapEndYear IS NOT NULL
                    THEN toString(overlapStartYear) + '.' +
                        CASE WHEN overlapStartMonth < 10 THEN '0' + toString(overlapStartMonth) ELSE toString(overlapStartMonth) END +
                        '-' + toString(overlapEndYear) + '.' +
                        CASE WHEN overlapEndMonth < 10 THEN '0' + toString(overlapEndMonth) ELSE toString(overlapEndMonth) END
                END as overlapPeriod

            // 检查这对人物之间是否已存在相同属性的SCHOOLMATES关系
            OPTIONAL MATCH (p1)-[existing:SCHOOLMATES]->(p2)
            WHERE existing.school = school AND
                (
                    (existing.overlapPeriod IS NULL AND overlapPeriod IS NULL) OR
                    existing.overlapPeriod = overlapPeriod
                ) AND
                existing.atTheSameTime = atTheSameTime

            WITH p1, p2, school, atTheSameTime, overlapPeriod, COUNT(existing) AS existingCount
            WHERE existingCount = 0  // 只处理不存在相同关系的情况

            // 根据重叠期是否存在采用不同的CREATE语句，并添加新属性
            FOREACH(dummy IN CASE WHEN overlapPeriod IS NOT NULL THEN [1] ELSE [] END |
                CREATE (p1)-[r:SCHOOLMATES {
                    atTheSameTime: atTheSameTime, 
                    school: school, 
                    overlapPeriod: overlapPeriod,
                    same_department: "TODO",
                    same_major: "TODO"
                }]->(p2)
            )

            FOREACH(dummy IN CASE WHEN overlapPeriod IS NULL THEN [1] ELSE [] END |
                CREATE (p1)-[r:SCHOOLMATES {
                    atTheSameTime: atTheSameTime, 
                    school: school,
                    same_department: "TODO",
                    same_major: "TODO"
                }]->(p2)
            )

            RETURN count(*) as rel_count
            """

            try:
                result = session.run(cypher)
                rel_count = result.single()["rel_count"]
                logger.info(f"已创建 {rel_count} 个同学关系")
            except Exception as e:
                logger.error(f"创建同学关系时出错: {e}")
                import traceback
                logger.error(traceback.format_exc())
                rel_count = 0

            return rel_count

    def create_colleague_relationships(self):
        """创建人物之间的同事关系"""
        logger.info("创建人物之间的同事关系...")

        with self.driver.session() as session:
            # 第一部分：处理当前在职的同事关系（WORKS_FOR）
            current_colleagues_cypher = """
            // 查找当前在同一组织工作的人员
            MATCH (p1:Person)-[w1:WORKS_FOR]->(o:Organization)<-[w2:WORKS_FOR]-(p2:Person)
            WHERE p1 <> p2 AND id(p1) < id(p2)  // 确保只创建一个方向的关系
            AND NOT EXISTS((p1)-[:COLLEAGUES]-(p2))  // 检查是否已存在同事关系

            // 创建当前同事关系
            CREATE (p1)-[r:COLLEAGUES {
                workplace: o.org_name,
                overlapPeriod: "till now"
            }]->(p2)

            RETURN count(r) AS current_colleagues_count
            """

            # 第二部分：处理历史同事关系（WORKED_AT）
            historical_colleagues_cypher = """
            // 查找在同一组织有工作经历的人员，且时间完整
            MATCH (p1:Person)-[w1:WORKED_AT]->(o:Organization)<-[w2:WORKED_AT]-(p2:Person)
            WHERE p1 <> p2 AND id(p1) < id(p2)  // 确保只创建一个方向的关系
            AND w1.startYear IS NOT NULL AND w1.startMonth IS NOT NULL 
            AND w1.endYear IS NOT NULL AND w1.endMonth IS NOT NULL
            AND w2.startYear IS NOT NULL AND w2.startMonth IS NOT NULL 
            AND w2.endYear IS NOT NULL AND w2.endMonth IS NOT NULL
            AND NOT EXISTS((p1)-[:COLLEAGUES]-(p2))  // 检查是否已存在同事关系（避免与当前同事关系重复）

            // 计算时间重叠
            WITH p1, p2, o, w1, w2,
                // 计算开始和结束时间（转换为月份便于比较）
                w1.startYear * 12 + w1.startMonth AS w1_start_months,
                w1.endYear * 12 + w1.endMonth AS w1_end_months,
                w2.startYear * 12 + w2.startMonth AS w2_start_months,
                w2.endYear * 12 + w2.endMonth AS w2_end_months

            // 判断是否有时间重叠
            WHERE w1_start_months <= w2_end_months AND w2_start_months <= w1_end_months

            // 计算重叠时间段
            WITH p1, p2, o, w1, w2,
                // 重叠开始时间（取较晚的开始时间）
                CASE 
                    WHEN w1.startYear > w2.startYear THEN w1.startYear
                    WHEN w1.startYear < w2.startYear THEN w2.startYear
                    ELSE CASE WHEN w1.startMonth > w2.startMonth THEN w1.startYear ELSE w2.startYear END
                END AS overlap_start_year,

                CASE 
                    WHEN w1.startYear > w2.startYear THEN w1.startMonth
                    WHEN w1.startYear < w2.startYear THEN w2.startMonth
                    ELSE CASE WHEN w1.startMonth > w2.startMonth THEN w1.startMonth ELSE w2.startMonth END
                END AS overlap_start_month,

                // 重叠结束时间（取较早的结束时间）
                CASE 
                    WHEN w1.endYear < w2.endYear THEN w1.endYear
                    WHEN w1.endYear > w2.endYear THEN w2.endYear
                    ELSE CASE WHEN w1.endMonth < w2.endMonth THEN w1.endYear ELSE w2.endYear END
                END AS overlap_end_year,

                CASE 
                    WHEN w1.endYear < w2.endYear THEN w1.endMonth
                    WHEN w1.endYear > w2.endYear THEN w2.endMonth
                    ELSE CASE WHEN w1.endMonth < w2.endMonth THEN w1.endMonth ELSE w2.endMonth END
                END AS overlap_end_month

            // 格式化重叠时间段
            WITH p1, p2, o,
                toString(overlap_start_year) + '.' +
                CASE WHEN overlap_start_month < 10 THEN '0' + toString(overlap_start_month) ELSE toString(overlap_start_month) END +
                '-' + toString(overlap_end_year) + '.' +
                CASE WHEN overlap_end_month < 10 THEN '0' + toString(overlap_end_month) ELSE toString(overlap_end_month) END AS overlapPeriod

            // 创建历史同事关系
            CREATE (p1)-[r:COLLEAGUES {
                workplace: o.org_name,
                overlapPeriod: overlapPeriod
            }]->(p2)

            RETURN count(r) AS historical_colleagues_count
            """

            try:
                # 执行当前同事关系创建
                logger.info("正在创建当前在职同事关系...")
                result1 = session.run(current_colleagues_cypher)
                current_count = result1.single()["current_colleagues_count"]
                logger.info(f"已创建 {current_count} 个当前在职同事关系")

                # 执行历史同事关系创建
                logger.info("正在创建历史同事关系...")
                result2 = session.run(historical_colleagues_cypher)
                historical_count = result2.single()["historical_colleagues_count"]
                logger.info(f"已创建 {historical_count} 个历史同事关系")

                total_count = current_count + historical_count
                logger.info(f"同事关系创建完成，总计创建 {total_count} 个同事关系")
                return total_count

            except Exception as e:
                logger.error(f"创建同事关系时出错: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return 0

    def import_data(self):
        """导入数据到Neo4j"""
        if not self.connect():
            logger.error("无法连接到数据库，导入终止")
            return False

        try:
            start_time = time.time()

            # 创建索引
            self.create_indexes()

            # 1. 导入组织
            organizations = self.mysql_client.get_all_organizations()
            if not organizations:
                logger.warning("未获取到组织数据，跳过组织导入")
            else:
                self.import_organizations(organizations)

            # 2. 导入领导人
            leaders = self.mysql_client.get_all_leaders()
            if not leaders:
                logger.warning("未获取到领导人数据，跳过领导人导入")
            else:
                self.import_leaders(leaders)

            # 3. 处理组织之间的层级关系
            self.process_organization_hierarchy(organizations)

            # 4. 创建同乡关系
            self.create_same_hometown_relationships()

            # 5. 创建同学关系
            self.create_schoolmates_relationships()

            # 6. 创建同事关系
            self.create_colleague_relationships()

            end_time = time.time()
            logger.info(f"数据导入完成，耗时 {end_time - start_time:.2f} 秒")
            return True

        except Exception as e:
            logger.error(f"导入数据时出错: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

        finally:
            self.disconnect()

    def import_organizations(self, organizations):
        """导入组织数据到Neo4j"""
        logger.info("开始导入组织数据...")

        with self.driver.session() as session:
            total = len(organizations)
            for i, org in enumerate(organizations):
                try:
                    # 提取组织属性
                    params = {
                        'uuid': org['uuid'],
                        'org_name': org['org_name'],
                        'org_name_en': org.get('org_name_en', ''),
                        'org_abbr': org.get('org_abbr', ''),
                        'org_alias': org.get('org_alias', ''),
                        'org_type': org.get('org_type', ''),
                        'aff_org': org.get('aff_org', ''),
                        'parent_org': org.get('parent_org', ''),
                        'sup_org': org.get('sup_org', ''),
                        'establish_date': org.get('establish_date', ''),
                        'office_addr': org.get('office_addr', ''),
                        'org_nature': org.get('org_nature', ''),
                        'adm_level': org.get('adm_level', ''),
                        'current_leader': org.get('current_leader', ''),
                        'org_region': org.get('org_region', ''),
                        'org_level': org.get('org_level', 0),
                        'parent_uuid': org.get('parent_uuid', '')
                    }

                    # 创建组织节点
                    cypher = """
                    MERGE (o:Organization {uuid: $uuid})
                    ON CREATE SET 
                      o.org_name = $org_name,
                      o.org_name_en = $org_name_en,
                      o.org_abbr = $org_abbr,
                      o.org_alias = $org_alias,
                      o.org_type = $org_type,
                      o.aff_org = $aff_org,
                      o.parent_org = $parent_org,
                      o.sup_org = $sup_org,
                      o.establish_date = $establish_date,
                      o.office_addr = $office_addr,
                      o.org_nature = $org_nature,
                      o.adm_level = $adm_level,
                      o.current_leader = $current_leader,
                      o.org_region = $org_region,
                      o.org_level = $org_level
                    """
                    session.run(cypher, params)

                except Exception as e:
                    logger.error(f"导入组织数据时出错 (UUID={org['uuid']}): {e}")

                # 更新进度条
                if (i + 1) % 10 == 0 or (i + 1) == total:
                    print_progress_bar(i + 1, total, prefix='导入组织数据', length=50)

        logger.info(f"组织数据导入完成，共导入 {total} 条记录")

    def import_leaders(self, leaders):
        """导入领导人数据到Neo4j"""
        logger.info("开始导入领导人数据...")

        with self.driver.session() as session:
            total = len(leaders)
            for i, leader in enumerate(leaders):
                try:
                    # 1. 创建Person节点
                    self.create_person_node(session, leader)

                    # 2. 处理组织关系
                    self.process_leader_org_relationships(session, leader)

                    # 3. 处理工作和学习经历
                    self.process_career_history(session, leader)

                except Exception as e:
                    logger.error(f"导入领导人数据时出错 (ID={leader['id']}): {e}")

                # 更新进度条
                if (i + 1) % 10 == 0 or (i + 1) == total:
                    print_progress_bar(i + 1, total, prefix='导入领导人数据', length=50)

        logger.info(f"领导人数据导入完成，共导入 {total} 条记录")

    def create_person_node(self, session, leader):
        """创建人物节点"""
        cypher = """
        MERGE (p:Person {uuid: $uuid})
        ON CREATE SET 
          p.leader_name = $leader_name,
          p.leader_position = $leader_position,
          p.leader_profile = $leader_profile,
          p.gender = $gender,
          p.ethnic_group = $ethnic_group,
          p.birth_place = $birth_place,
          p.birth_date = $birth_date,
          p.alma_mater = $alma_mater,
          p.political_status = $political_status,
          p.nationality = $nationality,
          p.degree = $degree,
          p.edu_level = $edu_level,
          p.professional_title = $professional_title,
          p.image_url = $image_url
        """

        params = {
            'uuid': leader['uuid'],
            'leader_name': leader['leader_name'],
            'leader_position': leader.get('leader_position', ''),
            'leader_profile': leader.get('leader_profile', ''),
            'gender': leader.get('gender', ''),
            'ethnic_group': leader.get('ethnic_group', ''),
            'birth_place': leader.get('birth_place', ''),
            'birth_date': leader.get('birth_date', ''),
            'alma_mater': leader.get('alma_mater', ''),
            'political_status': leader.get('political_status', ''),
            'nationality': leader.get('nationality', ''),
            'degree': leader.get('degree', ''),
            'edu_level': leader.get('edu_level', ''),
            'professional_title': leader.get('professional_title', ''),
            'image_url': leader.get('image_url', '')
        }

        session.run(cypher, params)

    def process_leader_org_relationships(self, session, leader):
        """处理领导人和组织之间的关系"""
        # 处理领导人与组织的关系
        if leader.get('org_info_id') and leader.get('org_info_uuid'):
            org_ids = str(leader['org_info_id']).split(',')
            org_uuids = str(leader['org_info_uuid']).split(',')
            org_names = str(leader['org_name']).split(',') if leader.get('org_name') else []

            for i in range(min(len(org_ids), len(org_uuids))):
                if not org_ids[i] or not org_uuids[i]:
                    continue

                # 创建领导人与组织之间的关系
                cypher = """
                MATCH (p:Person {uuid: $leader_uuid})
                MATCH (o:Organization {uuid: $org_uuid})
                MERGE (p)-[r:WORKS_FOR]->(o)
                ON CREATE SET 
                  r.position = $position
                """

                params = {
                    'leader_uuid': leader['uuid'],
                    'org_uuid': org_uuids[i].strip(),
                    'position': leader.get('leader_position', '')
                }

                session.run(cypher, params)

    def create_work_relationship(self, session, person_uuid, event):
        """创建工作关系"""
        # 检查必要的字段
        if not event.get('place'):
            logger.warning(f"工作经历缺少place字段，无法创建关系")
            return

        # 构建关系属性
        props = []
        params = {
            'person_uuid': person_uuid,
            'place': event.get('place', ''),
            'position': event.get('position', ''),
            'isEnd': event.get('isEnd', True),
            'hasEndDate': event.get('hasEndDate', False)
        }

        # 添加可选属性
        if event.get('startYear') is not None:
            props.append("startYear: $startYear")
            params['startYear'] = event.get('startYear')

        if event.get('startMonth') is not None:
            props.append("startMonth: $startMonth")
            params['startMonth'] = event.get('startMonth')

        if event.get('endYear') is not None:
            props.append("endYear: $endYear")
            params['endYear'] = event.get('endYear')

        if event.get('endMonth') is not None:
            props.append("endMonth: $endMonth")
            params['endMonth'] = event.get('endMonth')

        # 添加必要的基本属性
        props.extend(["isEnd: $isEnd", "hasEndDate: $hasEndDate", "position: $position"])
        props_str = ", ".join(props)

        # 首先创建或查找组织节点
        org_cypher = """
        MERGE (o:Organization {org_name: $place})
        RETURN o
        """

        # 然后创建工作关系
        rel_cypher = f"""
        MATCH (p:Person {{uuid: $person_uuid}})
        MATCH (o:Organization {{org_name: $place}})
        MERGE (p)-[r:WORKED_AT {{{props_str}}}]->(o)
        """

        try:
            # 执行查询
            session.run(org_cypher, {'place': params['place']})
            session.run(rel_cypher, params)
            logger.debug(f"创建工作关系: {params['position']} at {params['place']}")
        except Exception as e:
            logger.error(f"创建工作关系时出错: {e}")

    def create_study_relationship(self, session, person_uuid, event):
        """创建学习关系"""
        # 检查必要的字段
        if not event.get('school'):
            logger.warning(f"学习经历缺少school字段，无法创建关系")
            return

        # 构建关系属性
        props = []
        params = {
            'person_uuid': person_uuid,
            'school': event.get('school', ''),
            'isEnd': event.get('isEnd', True),
            'hasEndDate': event.get('hasEndDate', False)
        }

        # 添加可选属性
        if event.get('department'):
            props.append("department: $department")
            params['department'] = event.get('department')

        if event.get('major'):
            props.append("major: $major")
            params['major'] = event.get('major')

        if event.get('degree'):
            props.append("degree: $degree")
            params['degree'] = event.get('degree')

        if event.get('startYear') is not None:
            props.append("startYear: $startYear")
            params['startYear'] = event.get('startYear')

        if event.get('startMonth') is not None:
            props.append("startMonth: $startMonth")
            params['startMonth'] = event.get('startMonth')

        if event.get('endYear') is not None:
            props.append("endYear: $endYear")
            params['endYear'] = event.get('endYear')

        if event.get('endMonth') is not None:
            props.append("endMonth: $endMonth")
            params['endMonth'] = event.get('endMonth')

        # 添加必要的基本属性
        props.extend(["isEnd: $isEnd", "hasEndDate: $hasEndDate"])
        props_str = ", ".join(props)

        # 首先创建或查找教育机构节点
        org_cypher = """
        MERGE (o:Organization {org_name: $school})
        ON CREATE SET 
            o.org_type = '学校'
        RETURN o
        """

        # 然后创建学习关系
        rel_cypher = f"""
        MATCH (p:Person {{uuid: $person_uuid}})
        MATCH (o:Organization {{org_name: $school}})
        MERGE (p)-[r:STUDIED_AT {{{props_str}}}]->(o)
        """

        try:
            # 执行查询
            session.run(org_cypher, {'school': params['school']})
            session.run(rel_cypher, params)
            logger.debug(f"创建学习关系: {params.get('major', '')} at {params['school']}")
        except Exception as e:
            logger.error(f"创建学习关系时出错: {e}")

    def process_career_history(self, session, leader):
        """处理工作和学习经历"""
        # 处理结构化的职业履历
        career_history_structured = leader.get('career_history_structured')
        if not career_history_structured:
            return

        try:
            # 如果是字符串，尝试解析JSON
            if isinstance(career_history_structured, str):
                career_data = json.loads(career_history_structured)
                events = career_data.get('events', [])
            else:
                # 已经是字典类型
                events = career_history_structured.get('events', [])

            if not events:
                logger.warning(
                    f"领导人 {leader.get('leader_name')} (UUID={leader.get('uuid')}) 的career_history_structured没有events数据")
                return

            # 处理每个事件
            for event in events:
                event_type = event.get('eventType')

                if event_type == 'work':
                    self.create_work_relationship(session, leader['uuid'], event)
                elif event_type == 'study':
                    self.create_study_relationship(session, leader['uuid'], event)
                else:
                    logger.warning(f"未知的事件类型: {event_type}")

            # logger.info(f"成功处理领导人 {leader.get('leader_name')} 的 {len(events)} 个履历事件")

        except json.JSONDecodeError as e:
            logger.error(f"解析JSON失败: {e}")
        except Exception as e:
            logger.error(f"处理领导人 {leader.get('leader_name')} (UUID={leader.get('uuid')}) 的职业履历时出错: {e}")
            import traceback
            logger.error(traceback.format_exc())


def main():
    """主函数"""
    start_time = time.time()
    print("开始导入数据到Neo4j...")

    # 创建导入器实例
    importer = Neo4jImporter(Config.neo4j_config, Config.db_config)

    # 导入数据
    success = importer.import_data()

    # 显示统计信息
    end_time = time.time()
    if success:
        print(f"数据导入完成，耗时 {end_time - start_time:.2f} 秒")
    else:
        print(f"数据导入失败，耗时 {end_time - start_time:.2f} 秒")


# 执行导入函数
if __name__ == "__main__":
    main()