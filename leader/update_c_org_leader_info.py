import re
import hashlib
from tqdm import tqdm
from bs4 import BeautifulSoup
import pymysql
from pymysql.cursors import DictCursor


class LeaderExtractor:
    def __init__(self):
        """初始化领导信息提取器"""
        print("初始化提取器...")

        # 非人名关键词过滤列表
        self.non_person_keywords = [
            "本人编辑", "四人帮", "秘书长", "上海市", "双重领导",
            "纪律检查", "行政监察", "北京市", "山西省", "浙江省",
            "制度建设", "合署办公", "组织架构", "民主党派", "人民团体",
            "少数民族", "台湾同胞", "港澳同胞", "侨胞", "纪检监察",
            "浦东新区", "党组书记", "直属机关", "办事机构", "机构改革",
            "市直机关", "反恐专员", "北京海关", "民办高校", "提案",
            "稿件", "起草", "讲话稿", "会议纪要", "承办", "纪检组长",
            "职数", "事业单位", "厅长", "蒙古族", "行政编制", "副厅级",
            "正处级", "民族宗教", "主任", "督查室", "副处级", "省长助理",
            "国防动员", "党组成员", "主席", "长沙市", "世纪", "国防",
            "元帅", "中南地区", "北京", "高中", "苏联", "中共中央",
            "江西", "广东", "无线电", "总工程师", "衡阳", "书记",
            "公安", "春节", "坑口", "重铀酸铵", "二机部", "党委书记",
            "吉林省", "巡视员", "正厅级", "国务院", "司令员", "中央委员",
            "藏族", "满族", "苗族", "维吾尔族", "回族", "监事会", "监察官",
            "滨海新区", "地源热泵"
        ]

    def clean_name(self, name):
        """清理人名，移除括号内容和空格"""
        if not name:
            return ""
        # 移除括号及内容
        name = re.sub(r'（.*?）|\(.*?\)', '', name)
        # 移除空格
        name = re.sub(r'\s+', '', name)
        return name.strip()

    def clean_url(self, url):
        """清理URL，移除问号后的内容"""
        if not url:
            return ""
        if '?' in url:
            url = url.split('?')[0]
        return url

    def is_valid_leader_name(self, name):
        """检查是否为有效的人名

        条件：
        1. 长度大于0且小于等于4个字符
        2. 不包含非人名关键词
        """
        if not name:
            return False

        # 检查长度
        if len(name) == 0 or len(name) > 4:
            if "•" in name or "·" in name:
                return True
            return False

        # 检查是否包含非人名关键词
        for keyword in self.non_person_keywords:
            if keyword in name:
                return False

        return True

    def extract_leaders(self, html, org_info_id, org_info_uuid):
        """从HTML中提取领导信息"""
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        leaders = []

        # 打印所有标题以辅助调试
        all_headers = soup.find_all(['h1', 'h2', 'h3', 'h4'], recursive=True)
        print("\n页面标题:")
        for header in all_headers:
            header_text = header.get_text().strip()
            print(f" - {header_text}")

        # 1. 精确查找"机构领导"部分标题
        leadership_section = None
        leadership_headers = ["机构领导", "现任领导", "主要领导", "领导成员", "领导班子", "领导分工"]

        # 首先尝试查找h2标签中的领导标题
        for header in soup.find_all('h2', recursive=True):
            header_text = header.get_text().strip()
            if any(term in header_text for term in leadership_headers):
                leadership_section = header
                print(f"找到领导部分(h2): {header_text}")
                break

        # 如果未找到，尝试使用h3标签
        if not leadership_section:
            for header in soup.find_all('h3', recursive=True):
                header_text = header.get_text().strip()
                if any(term in header_text for term in leadership_headers):
                    leadership_section = header
                    print(f"找到领导部分(h3): {header_text}")
                    break

        # 尝试找带有特定name属性的标题
        if not leadership_section:
            for header in soup.find_all(['h2', 'h3'], attrs={'name': True}, recursive=True):
                header_text = header.get_text().strip()
                if any(term in header_text for term in leadership_headers):
                    leadership_section = header
                    print(f"找到领导部分(带name属性): {header_text}")
                    break

        if not leadership_section:
            print("未找到领导部分标题")
            return []

        # 2. 确定标题的标签类型，以便确定同级标题
        header_tag = leadership_section.name  # 'h2', 'h3'等
        print(f"领导部分标题标签类型: {header_tag}")

        # 3. 定义一个函数，收集从当前标题到下一个同级标题之间的所有内容
        def collect_until_next_same_level_header(start_header):
            """收集从起始标题到下一个同级标题之间的所有内容"""
            content_sections = []
            current_element = start_header.next_element

            # 找到同级或更高级的下一个标题
            while current_element:
                # 如果是标题元素，检查是否为同级或更高级
                if hasattr(current_element, 'name') and current_element.name:
                    # 如果遇到同级或更高级标题，停止收集
                    if current_element.name in ['h1', 'h2', 'h3', 'h4'] and current_element != start_header:
                        # 检查是否为同级或更高级标题
                        if (current_element.name == header_tag or
                                (header_tag == 'h3' and current_element.name == 'h2') or
                                (header_tag == 'h4' and current_element.name in ['h2', 'h3'])):
                            break

                if hasattr(current_element, 'name') and current_element.name:
                    content_sections.append(current_element)

                try:
                    current_element = current_element.next_element
                except:
                    break

            return content_sections

        # 4. 收集领导部分的内容
        leadership_content = collect_until_next_same_level_header(leadership_section)

        # 5. 从领导部分内容中提取所有链接
        found_links = []
        for element in leadership_content:
            if hasattr(element, 'find_all'):
                # 尝试查找链接
                links = element.find_all('a', href=True)
                if links:
                    found_links.extend(links)

        print(f"在领导部分找到 {len(found_links)} 个链接")

        # 6. 从所有找到的链接中提取人名和URL
        for link in found_links:
            href = link.get('href', '')
            if '/item/' in href:
                # 构建完整URL
                if href.startswith('/'):
                    person_url = f"https://baike.baidu.com{href}"
                else:
                    person_url = href

                # 获取原始人名
                leader_name = link.get_text().strip()

                # 先记录原始人名文本，用于调试
                print(f"发现链接文本: '{leader_name}', URL: {person_url}")

                # 清理人名和URL
                clean_name = self.clean_name(leader_name)
                clean_url = self.clean_url(person_url)

                # 检查是否为有效人名
                if clean_name and clean_url and self.is_valid_leader_name(clean_name):
                    # 使用URL的MD5哈希作为UUID
                    # TODO: 此处UUID需要替换为人物的姓名+籍贯+出生年月进行生成
                    person_uuid = hashlib.md5(clean_url.encode()).hexdigest()
                    leaders.append({
                        'org_info_id': org_info_id,
                        'org_info_uuid': org_info_uuid,
                        'uuid': person_uuid,
                        'leader_name': clean_name,
                        'source_url': clean_url
                    })
                    print(f"添加领导: '{clean_name}' (原始: '{leader_name}'), URL: {clean_url}")
                else:
                    print(f"过滤掉无效人名: '{clean_name}' (原始: '{leader_name}'), URL: {clean_url}")

        return leaders


def get_database_connection(config):
    """建立与MySQL数据库的连接"""
    try:
        connection = pymysql.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            charset='utf8mb4',
            cursorclass=DictCursor
        )
        print(f"成功连接到数据库 {config['database']}")
        return connection
    except Exception as e:
        print(f"连接数据库时出错: {e}")
        raise


def insert_or_update_leader(conn, leader_data, org_name):
    """向数据库插入或更新领导信息"""
    try:
        with conn.cursor() as cursor:
            # 查询是否存在该领导记录
            sql_check = "SELECT * FROM c_org_leader_info WHERE uuid = %s"
            cursor.execute(sql_check, (leader_data['uuid'],))
            existing_leader = cursor.fetchone()

            if existing_leader:
                # 处理组织ID和UUID
                org_info_id_list = str(existing_leader['org_info_id']).split(',')
                org_info_uuid_list = str(existing_leader['org_info_uuid']).split(',')

                # 新增：处理组织名称
                org_name_list = str(existing_leader['org_name']).split(',') if existing_leader['org_name'] else []

                # 更新逻辑...
                if str(leader_data['org_info_id']) not in org_info_id_list:
                    org_info_id_list.append(str(leader_data['org_info_id']))

                if leader_data['org_info_uuid'] not in org_info_uuid_list:
                    org_info_uuid_list.append(leader_data['org_info_uuid'])

                # 新增：同样处理组织名称
                if org_name not in org_name_list:
                    org_name_list.append(org_name)

                # 修改 UPDATE 语句，加入 org_name 字段
                sql_update = """
                UPDATE c_org_leader_info 
                SET org_info_id = %s, org_info_uuid = %s, org_name = %s
                WHERE uuid = %s
                """
                cursor.execute(
                    sql_update,
                    (','.join(org_info_id_list), ','.join(org_info_uuid_list), ','.join(org_name_list),
                     leader_data['uuid'])
                )
            else:
                # 修改 INSERT 语句，加入 org_name 字段
                sql_insert = """
                INSERT INTO c_org_leader_info (uuid, org_info_id, org_info_uuid, leader_name, source_url, org_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(
                    sql_insert,
                    (
                        leader_data['uuid'],
                        leader_data['org_info_id'],
                        leader_data['org_info_uuid'],
                        leader_data['leader_name'],
                        leader_data['source_url'],
                        org_name
                    )
                )
                conn.commit()
                print(f"插入新领导: {leader_data['leader_name']}")

            return True
    except Exception as e:
        conn.rollback()
        print(f"插入或更新领导信息时出错: {e}")
        return False


def get_processed_org_ids(conn):
    """获取已处理过的组织ID"""
    processed_ids = set()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT org_info_id FROM c_org_leader_info"
            cursor.execute(sql)
            results = cursor.fetchall()

            for result in results:
                # 处理逗号分隔的多个来源ID
                org_ids = str(result['org_info_id']).split(',')
                processed_ids.update(org_ids)

            print(f"已有 {len(processed_ids)} 个组织ID被处理过")
            return processed_ids
    except Exception as e:
        print(f"获取已处理组织ID时出错: {e}")
        return processed_ids


def process_database_records(conn, limit=None, offset=0):
    """从数据库中处理记录，提取领导信息"""
    # 初始化提取器
    extractor = LeaderExtractor()

    try:
        # 获取已处理的组织ID
        processed_ids = get_processed_org_ids(conn)

        # 创建游标
        with conn.cursor() as cursor:
            # 获取总记录数
            cursor.execute("SELECT COUNT(*) as count FROM c_org_info WHERE remark IS NOT NULL AND remark != ''")
            total_count = cursor.fetchone()['count']
            print(f"数据库中共有 {total_count} 条有效记录")

            # 构建SQL查询
            sql = "SELECT id, uuid, org_name, remark FROM c_org_info WHERE remark IS NOT NULL AND remark != '' ORDER BY id LIMIT %s OFFSET %s"

            # 设置默认限制
            if limit is None:
                limit = total_count

            # 执行查询
            cursor.execute(sql, (limit, offset))
            records = cursor.fetchall()
            print(f"获取了 {len(records)} 条记录")

            # 处理记录
            for record in tqdm(records, total=len(records)):
                org_info_id = record['id']
                org_info_uuid = record['uuid']
                org_name = record['org_name']
                html_content = record['remark']

                # 检查是否已处理过该组织
                if str(org_info_id) in processed_ids:
                    print(f"跳过已处理过的组织: {org_name} (ID: {org_info_id})")
                    continue

                # 检查HTML内容
                if not html_content or len(html_content) < 100:
                    print(f"跳过HTML内容过短的记录: {org_name} (ID: {org_info_id})")
                    continue

                print(f"\n处理组织: {org_name} (ID: {org_info_id}, UUID: {org_info_uuid})")

                # 提取领导信息
                leaders = extractor.extract_leaders(html_content, org_info_id, org_info_uuid)

                # 存储到数据库
                if leaders:
                    for leader in leaders:
                        insert_or_update_leader(conn, leader, org_name)
                    print(f"成功处理组织 {org_name} 的 {len(leaders)} 位领导")
                else:
                    print(f"未找到组织 {org_name} 的领导信息")

    except Exception as e:
        print(f"处理数据库记录时出错: {e}")
        raise


def update_c_org_leader_info(db_config):
    # 连接数据库
    try:
        conn = get_database_connection(db_config)

        # 处理数据库记录
        process_database_records(conn)

        # 关闭连接
        conn.close()
        print("数据库连接已关闭")

    except Exception as e:
        print(f"程序运行出错: {e}")