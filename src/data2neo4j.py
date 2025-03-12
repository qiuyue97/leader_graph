#!/usr/bin/env python3
"""
直接将人物履历数据导入到Neo4j图数据库的脚本
"""

import json
import os
import csv
import sys
from neo4j import GraphDatabase
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import Config as basicConfig


# 配置信息
class Config:
    # Neo4j 数据库连接配置
    config_path = '/root/leader_graph/config.yaml'
    config = basicConfig.from_file(config_path)
    neo4j_config = config.neo4j_config

    # 数据文件路径
    person_data_dir = "/root/leader_graph/data/result/"
    department_csv = "/root/leader_graph/data/shanghai_departments.csv"


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


# 加载部门数据
def load_department_data():
    """从CSV文件加载部门数据"""
    departments = {}
    department_id_map = {}
    try:
        with open(Config.department_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                departments[row['department_id']] = row
                department_id_map[row['department_name']] = row['department_id']
        print(f"已加载 {len(departments)} 个部门数据")
    except FileNotFoundError:
        print(f"警告: 部门CSV文件 {Config.department_csv} 未找到")
    return departments, department_id_map


# 读取人物数据文件列表
def get_person_files():
    """获取人物数据JSON文件列表"""
    files = []
    if os.path.exists(Config.person_data_dir):
        for filename in os.listdir(Config.person_data_dir):
            if filename.endswith('.json'):
                files.append(os.path.join(Config.person_data_dir, filename))
        print(f"找到 {len(files)} 个人物数据文件")
    else:
        print(f"警告: 人物数据目录 {Config.person_data_dir} 未找到")
    return files


# 创建Neo4j索引
def create_indexes(session):
    """创建Neo4j数据库索引以提高性能"""
    indexes = [
        "CREATE INDEX person_id_index IF NOT EXISTS FOR (p:Person) ON (p.person_id)",
        "CREATE INDEX department_id_index IF NOT EXISTS FOR (d:Department) ON (d.department_id)",
        "CREATE INDEX department_name_index IF NOT EXISTS FOR (d:Department) ON (d.department_name)"
    ]
    for index in indexes:
        session.run(index)
    print("已创建索引")


def create_same_hometown_relationships(session):
    """创建人物之间的同乡关系，并添加native_place属性"""
    cypher = """
    MATCH (p1:Person), (p2:Person)
    WHERE p1.native_place IS NOT NULL 
      AND p1.native_place <> '' 
      AND p1.native_place = p2.native_place 
      AND p1 <> p2
    MERGE (p1)-[r:SAME_HOMETOWN {native_place: p1.native_place}]->(p2)
    RETURN count(r) as rel_count
    """

    result = session.run(cypher)
    rel_count = result.single()["rel_count"]
    print(f"已创建 {rel_count} 个同乡关系")


def create_schoolmates_relationships(session):
    """创建人物之间的同学关系"""
    cypher = """
    MATCH (p1:Person)-[r1:STUDIED_AT]->(s:Department)<-[r2:STUDIED_AT]-(p2:Person)
    WHERE p1 <> p2
    WITH p1, p2, s, r1, r2,
         // 计算是否时间重叠
         CASE
            WHEN r1.startYear IS NOT NULL AND r1.endYear IS NOT NULL AND 
                 r2.startYear IS NOT NULL AND r2.endYear IS NOT NULL
            THEN (r1.startYear * 12 + COALESCE(r1.startMonth, 1)) <= (r2.endYear * 12 + COALESCE(r2.endMonth, 12)) AND
                 (r2.startYear * 12 + COALESCE(r2.startMonth, 1)) <= (r1.endYear * 12 + COALESCE(r1.endMonth, 12))
            ELSE false
         END as atTheSameTime,
         s.department_name as school,

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
    WITH p1, p2, s.department_name as school, atTheSameTime,
         overlapStartYear, overlapStartMonth, overlapEndYear, overlapEndMonth

    WITH p1, p2, school, atTheSameTime,
         CASE 
            WHEN atTheSameTime AND overlapStartYear IS NOT NULL AND overlapEndYear IS NOT NULL
            THEN toString(overlapStartYear) + '.' +
                 CASE WHEN overlapStartMonth < 10 THEN '0' + toString(overlapStartMonth) ELSE toString(overlapStartMonth) END +
                 '-' + toString(overlapEndYear) + '.' +
                 CASE WHEN overlapEndMonth < 10 THEN '0' + toString(overlapEndMonth) ELSE toString(overlapEndMonth) END
         END as overlapPeriod

    // 根据重叠期是否存在采用不同的MERGE语句
    FOREACH(dummy IN CASE WHEN overlapPeriod IS NOT NULL THEN [1] ELSE [] END |
        MERGE (p1)-[r:SCHOOLMATES {atTheSameTime: atTheSameTime, school: school, overlapPeriod: overlapPeriod}]->(p2)
    )

    FOREACH(dummy IN CASE WHEN overlapPeriod IS NULL THEN [1] ELSE [] END |
        MERGE (p1)-[r:SCHOOLMATES {atTheSameTime: atTheSameTime, school: school}]->(p2)
    )

    RETURN count(*) as rel_count
    """

    result = session.run(cypher)
    rel_count = result.single()["rel_count"]
    print(f"已创建 {rel_count} 个同学关系")


# Neo4j数据导入主函数
def import_data_to_neo4j():
    """将数据导入到Neo4j图数据库"""
    # 连接到Neo4j数据库
    driver = GraphDatabase.driver(
        Config.neo4j_config['uri'],
        auth=(Config.neo4j_config['user'], Config.neo4j_config['password'])
    )

    # 加载部门数据
    departments, department_id_map = load_department_data()

    # 获取人物数据文件列表
    person_files = get_person_files()
    total_files = len(person_files)

    if total_files == 0:
        print("没有找到人物数据文件，程序退出")
        driver.close()
        return

    # 开始导入数据
    with driver.session() as session:
        # 创建索引
        create_indexes(session)

        # 处理每个人物文件
        for i, file_path in enumerate(person_files):
            try:
                # 读取人物数据
                with open(file_path, 'r', encoding='utf-8') as f:
                    person_data = json.load(f)

                # 创建人物节点并处理关系
                process_person(session, person_data, departments, department_id_map)

                # 更新进度条
                print_progress_bar(i + 1, total_files, prefix='导入人物数据', length=50)

            except Exception as e:
                print(f"\n处理文件 {file_path} 时出错: {e}")

        print("\n处理部门层级关系...")
        process_department_hierarchy(session, departments)

        print("\n创建同乡关系...")
        create_same_hometown_relationships(session)

        # print("\n创建同学关系...")
        # create_schoolmates_relationships(session)

    # 关闭Neo4j连接
    driver.close()
    print("\n数据导入完成")


# 处理单个人物数据
def process_person(session, person_data, departments, department_id_map):
    """处理单个人物数据，创建节点和关系"""
    # 获取人物ID
    person_id = person_data.get('person_id', '')
    if not person_id:
        return

    # 创建人物节点
    create_person_node(session, person_data)

    # 处理'from'字段中的部门关联
    from_ids = person_data.get('from', '')
    if from_ids:
        process_from_relationships(session, person_id, from_ids, departments)

    # 处理工作和学习经历
    events = person_data.get('events', [])
    process_events(session, person_id, events, department_id_map)


# 创建人物节点
def create_person_node(session, person_data):
    """创建人物节点"""
    cypher = """
    MERGE (p:Person {person_id: $person_id})
    ON CREATE SET 
      p.person_name = $person_name,
      p.person_url = $person_url,
      p.person_title = $person_title,
      p.person_summary = $person_summary,
      p.ethnicity = $ethnicity,
      p.native_place = $native_place,
      p.birth_date = $birth_date,
      p.alma_mater = $alma_mater,
      p.political_status = $political_status
    """

    params = {
        'person_id': person_data.get('person_id', ''),
        'person_name': person_data.get('person_name', ''),
        'person_url': person_data.get('person_url', ''),
        'person_title': person_data.get('person_title', ''),
        'person_summary': person_data.get('person_summary', ''),
        'ethnicity': person_data.get('ethnicity', ''),
        'native_place': person_data.get('native_place', ''),
        'birth_date': person_data.get('birth_date', ''),
        'alma_mater': person_data.get('alma_mater', ''),
        'political_status': person_data.get('political_status', '')
    }

    session.run(cypher, params)


# 处理部门节点
def create_department_node(session, dept_name, dept_id=None, province=None, level=None):
    """创建部门节点"""
    cypher = """
    MERGE (d:Department {department_name: $dept_name})
    ON CREATE SET 
      d.department_id = $dept_id,
      d.province = $province,
      d.department_level = $level
    """

    if not dept_id:
        # 使用名称作为ID的一部分，确保唯一性
        import hashlib
        hash_obj = hashlib.md5(dept_name.encode())
        dept_id = f"dept_{hash_obj.hexdigest()[:8]}"

    params = {
        'dept_name': dept_name,
        'dept_id': dept_id,
        'province': province or "",
        'level': level or "未知"
    }

    session.run(cypher, params)
    return dept_id


# 处理工作和学习经历
def process_events(session, person_id, events, department_id_map):
    """处理人物的工作和学习经历"""
    for event in events:
        event_type = event.get('eventType')

        if event_type == 'work':
            place = event.get('place')
            if place:
                # 创建部门节点
                dept_id = department_id_map.get(place)
                create_department_node(session, place, dept_id)

                # 创建工作关系
                create_work_relationship(session, person_id, place, event)

        elif event_type == 'study':
            school = event.get('school')
            if school:
                # 创建学校节点作为部门
                dept_id = department_id_map.get(school)
                create_department_node(session, school, dept_id, level="学校")

                # 创建学习关系
                create_study_relationship(session, person_id, school, event)


# 创建工作关系
def create_work_relationship(session, person_id, dept_name, event):
    """创建工作关系"""
    # 构建基本属性，排除为null的属性
    base_props = []
    params = {
        'person_id': person_id,
        'dept_name': dept_name,
        'isEnd': event.get('isEnd', True),
        'hasEndDate': event.get('hasEndDate', False)
    }

    # 只添加非null的属性
    if event.get('position'):
        base_props.append("position: $position")
        params['position'] = event.get('position')

    if event.get('startYear') is not None:
        base_props.append("startYear: $startYear")
        params['startYear'] = event.get('startYear')

    if event.get('startMonth') is not None:
        base_props.append("startMonth: $startMonth")
        params['startMonth'] = event.get('startMonth')

    if event.get('endYear') is not None:
        base_props.append("endYear: $endYear")
        params['endYear'] = event.get('endYear')

    if event.get('endMonth') is not None:
        base_props.append("endMonth: $endMonth")
        params['endMonth'] = event.get('endMonth')

    # 添加必要的基本属性
    base_props.extend(["isEnd: $isEnd", "hasEndDate: $hasEndDate"])
    props_str = ", ".join(base_props)

    # 构建Cypher查询
    cypher = f"""
    MATCH (p:Person {{person_id: $person_id}})
    MATCH (d:Department {{department_name: $dept_name}})
    MERGE (p)-[r:WORKED_AT {{{props_str}}}]->(d)
    """

    session.run(cypher, params)


# 创建学习关系
def create_study_relationship(session, person_id, school_name, event):
    """创建学习关系"""
    # 构建基本属性，排除为null的属性
    base_props = []
    params = {
        'person_id': person_id,
        'school_name': school_name,
        'isEnd': event.get('isEnd', True),
        'hasEndDate': event.get('hasEndDate', False)
    }

    # 只添加非null的属性
    if event.get('department'):
        base_props.append("department: $department")
        params['department'] = event.get('department')

    if event.get('major'):
        base_props.append("major: $major")
        params['major'] = event.get('major')

    if event.get('degree'):
        base_props.append("degree: $degree")
        params['degree'] = event.get('degree')

    # 构建关系属性部分
    base_props.extend(["isEnd: $isEnd", "hasEndDate: $hasEndDate"])
    props_str = ", ".join(base_props)

    # 构建Cypher查询
    cypher = f"""
    MATCH (p:Person {{person_id: $person_id}})
    MATCH (d:Department {{department_name: $school_name}})
    MERGE (p)-[r:STUDIED_AT {{{props_str}}}]->(d)
    """

    # 添加日期属性（如果存在）
    if event.get('startYear'):
        cypher = cypher.replace('MERGE (p)-[r:STUDIED_AT {',
                                'MERGE (p)-[r:STUDIED_AT {\n      startYear: $startYear,')
        params['startYear'] = event['startYear']

    if event.get('startMonth'):
        cypher = cypher.replace('MERGE (p)-[r:STUDIED_AT {',
                                'MERGE (p)-[r:STUDIED_AT {\n      startMonth: $startMonth,')
        params['startMonth'] = event['startMonth']

    if event.get('endYear'):
        cypher = cypher.replace('MERGE (p)-[r:STUDIED_AT {',
                                'MERGE (p)-[r:STUDIED_AT {\n      endYear: $endYear,')
        params['endYear'] = event['endYear']

    if event.get('endMonth'):
        cypher = cypher.replace('MERGE (p)-[r:STUDIED_AT {',
                                'MERGE (p)-[r:STUDIED_AT {\n      endMonth: $endMonth,')
        params['endMonth'] = event['endMonth']

    session.run(cypher, params)


# 处理'from'字段中的部门关系
def process_from_relationships(session, person_id, from_ids, departments):
    """处理'from'字段中的部门关系"""
    if not from_ids:
        return

    dept_id_list = from_ids.split(',')
    for dept_id in dept_id_list:
        if dept_id in departments:
            dept_data = departments[dept_id]
            dept_name = dept_data.get('department_name', '未知部门')

            # 创建部门节点
            create_department_node(
                session,
                dept_name,
                dept_id,
                province=dept_data.get('province', ''),
                level=dept_data.get('department_level', '未知')
            )

            # 创建没有属性的工作关系（来自from字段）
            cypher = """
            MATCH (p:Person {person_id: $person_id})
            MATCH (d:Department {department_id: $dept_id})
            MERGE (p)-[:WORKED_AT]->(d)
            """

            params = {
                'person_id': person_id,
                'dept_id': dept_id
            }

            session.run(cypher, params)


# 处理部门层级关系
def process_department_hierarchy(session, departments):
    """处理部门之间的层级关系"""
    print("处理部门层级关系...")
    count = 0

    for dept_id, dept_data in departments.items():
        father_id = dept_data.get('father_department_id')
        if father_id and father_id in departments:
            cypher = """
            MATCH (child:Department {department_id: $child_id})
            MATCH (parent:Department {department_id: $parent_id})
            MERGE (child)-[:BELONGS_TO]->(parent)
            """

            params = {
                'child_id': dept_id,
                'parent_id': father_id
            }

            session.run(cypher, params)
            count += 1

    print(f"已创建 {count} 个部门层级关系")


# 主函数
if __name__ == "__main__":
    start_time = time.time()
    print("开始导入数据到Neo4j...")
    import_data_to_neo4j()
    end_time = time.time()
    print(f"数据导入完成，耗时 {end_time - start_time:.2f} 秒")