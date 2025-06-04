# Leader Graph

一个全面的Python工具，用于爬取、解析和分析百度百科数据，专注于组织机构和领导人履历信息的采集与结构化处理。项目旨在构建领导人关系图谱数据库，用于分析和可视化。

## 项目特色

- **组织机构信息管理** - 从CSV文件导入组织架构数据，支持多级组织关系
- **智能网页爬取** - 基于Selenium的多线程爬虫，支持代理池和移动设备模拟
- **内容验证机制** - 自动验证爬取内容有效性，过滤安全验证和错误页面
- **HTML内容缓存** - 避免重复网络请求，提高数据处理效率
- **结构化解析** - 从百度百科页面提取表格和内容信息
- **AI驱动的数据提取** - 使用GPT-4o/Qwen模型进行履历事件结构化
- **图数据库集成** - 支持Neo4j图数据库，构建复杂关系网络
- **多模型支持** - 同时支持Azure OpenAI和阿里云Qwen API

## 项目架构

```
├── config/                     # 配置管理模块
│   ├── __init__.py             
│   └── settings.py             # 应用配置管理
├── html_extractor/             # HTML内容提取模块
│   ├── __init__.py
│   ├── extract_content_from_remark.py      # 百科内容结构化提取
│   ├── extract_table_from_remark.py        # 百科表格信息提取
│   ├── leader_content_schema.json          # 领导人内容字段映射
│   ├── leader_table_schema.json            # 领导人表格字段映射
│   ├── org_content_schema.json             # 组织内容字段映射
│   ├── org_table_schema.json               # 组织表格字段映射
│   └── save_html_from_remark.py            # HTML内容导出工具
├── leader/                     # 领导人数据处理模块
│   ├── __init__.py
│   ├── bio_processor.py        # GPT-4o履历数据处理
│   ├── bio_processor_qwen.py   # Qwen履历数据处理
│   ├── create_c_org_leader_info.py         # 领导人表创建
│   ├── extract_org_leader_info.py          # 领导人信息提取
│   ├── schema.py               # 履历数据结构定义
│   ├── update_c_org_leader_info.py         # 领导人数据更新
│   ├── update_c_org_leader_info_remark.py  # 领导人HTML爬取
│   └── update_leader_img_url.py            # 领导人头像提取
├── org/                        # 组织机构处理模块
│   ├── __init__.py
│   ├── create_c_org_info.py    # 组织信息表创建
│   ├── extract_org_info.py     # 组织信息提取
│   └── update_c_org_info_remark.py         # 组织HTML爬取
├── parser/                     # HTML解析模块
│   ├── __init__.py
│   └── baike_parser.py         # 百度百科内容解析器
├── processor/                  # 数据处理核心模块
│   ├── __init__.py
│   └── data_processor.py       # 主数据处理器（生产者-消费者模式）
├── proxy/                      # 代理管理模块
│   ├── __init__.py
│   ├── pool.py                 # 代理池管理
│   └── providers.py            # 代理服务提供者
├── scraper/                    # 网页爬取模块
│   ├── __init__.py
│   ├── baike_scraper.py        # 百度百科专用爬虫
│   └── selenium_scraper.py     # Selenium浏览器自动化
├── src/                        # AI处理和数据导入模块
│   ├── bio_demo.py             # 履历提取演示
│   ├── mysql2neo4j.py          # MySQL到Neo4j数据导入
│   ├── news_demo.py            # 新闻实体提取演示
│   ├── news_processor.py       # 新闻结构化处理
│   └── news_schema.py          # 新闻数据结构定义
├── utils/                      # 工具函数模块
│   ├── __init__.py
│   ├── content_validator.py    # HTML内容验证器
│   ├── db_utils.py             # 数据库工具
│   ├── file_utils.py           # 文件操作工具
│   └── logger.py               # 日志管理工具
├── config_eg.yaml              # 配置文件示例
└── main.py                     # 主程序入口
```

## 安装配置

### 1. 环境准备

```bash
git clone https://github.com/yourusername/leader_graph.git
cd leader_graph
```

### 2. 创建虚拟环境

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate     # Windows
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 数据库配置

确保已安装并配置以下数据库：
- **MySQL** - 用于存储组织和领导人数据
- **Neo4j** (可选) - 用于图关系分析

### 5. 配置文件设置

首次运行时会自动生成配置文件模板 `config.yaml`：

```yaml
# 输入数据目录
input_data_dir: './data/input_data'

# 爬虫配置
num_producers: 10
num_consumers: 1
max_retries: 3
min_content_size: 1024

# 代理配置
use_proxy: true
proxy_config:
  providers:
    - type: "xiaoxiang"
      app_key: "your_api_key"
      app_secret: "your_api_secret"
  refresh_interval: 15
  min_proxies: 20

# AI模型配置
azure_openai_endpoint: "your_azure_endpoint"
azure_openai_api_key: "your_azure_api_key"
qwen_api_key: "your_qwen_api_key"
cost_limit: 1.0

# 数据库配置
db_config:
  host: "localhost"
  user: "root"
  password: "your_password"
  database: "cnfic_leader"
  charset: "utf8mb4"

# Neo4j配置（可选）
neo4j_config:
  uri: "bolt://localhost:7687"
  user: "neo4j"
  password: "your_password"
```

## 使用指南

### 完整流程运行

```bash
python main.py
```

这将执行完整的数据处理流程：
1. 从输入目录创建组织和领导人数据表
2. 爬取组织机构网页信息
3. 解析组织机构详细信息
4. 提取领导人信息并更新数据库
5. 爬取领导人个人网页
6. 解析领导人详细信息和头像
7. AI结构化处理履历数据

### 分步骤运行

#### 1. 组织机构数据导入

```python
from org import create_c_org_info
from config.settings import Config

config = Config.from_file('./config.yaml')
create_c_org_info(
    input_directory=config.input_data_dir, 
    db_config=config.db_config
)
```

#### 2. 网页内容爬取

```python
from org import fetch_and_store_html

fetch_and_store_html(db_config=config.db_config, update=False)
```

#### 3. 结构化信息提取

```python
from leader import extract_org_leader_info, bio_processor

# 提取领导人基本信息
extract_org_leader_info()

# AI处理履历数据
bio_processor(config_path='./config.yaml', cost_limit=1.0, update=False)
```

#### 4. 图数据库导入（可选）

```bash
python src/mysql2neo4j.py
```

### 演示和测试功能

#### 履历提取演示

```bash
python src/bio_demo.py
```

#### 新闻实体提取演示

```bash
python src/news_demo.py
```

## 输入数据格式

### 组织机构数据

输入CSV文件应包含以下列：
- `一级部门`: 一级组织名称
- `二级部门`: 二级组织名称  
- `省份`: 所属地区
- `部门类型`: 组织类型
- `URL`: 百度百科链接

示例：
```csv
一级部门,二级部门,省份,部门类型,URL
上海市政府,市发改委,上海,政府部门,https://baike.baidu.com/item/上海市发改委
上海市政府,市财政局,上海,政府部门,https://baike.baidu.com/item/上海市财政局
```

## 输出数据

### 1. 数据库表结构

#### c_org_info (组织信息表)
- 基本信息：uuid, org_name, org_type, org_region等
- 详细信息：org_duty, internal_dept, org_history等
- HTML内容：remark字段存储完整网页内容

#### c_org_leader_info (领导人信息表)  
- 基本信息：uuid, leader_name, leader_position等
- 个人信息：gender, birth_place, alma_mater等
- 履历信息：career_history, career_history_structured等

### 2. 结构化履历数据

AI处理后的JSON格式履历数据：
```json
{
  "events": [
    {
      "eventType": "study",
      "startYear": 2000,
      "startMonth": 9,
      "isEnd": true,
      "hasEndDate": true,
      "endYear": 2004,
      "endMonth": 7,
      "school": "北京大学",
      "department": "信息科学技术学院",
      "major": "计算机科学",
      "degree": "学士"
    },
    {
      "eventType": "work", 
      "startYear": 2004,
      "startMonth": 8,
      "isEnd": false,
      "hasEndDate": false,
      "place": "某政府机构",
      "position": "科长"
    }
  ]
}
```

### 3. Neo4j图数据库

- **Person节点** - 领导人信息
- **Organization节点** - 组织机构信息  
- **WORKS_FOR关系** - 工作关系
- **STUDIED_AT关系** - 学习关系
- **SCHOOLMATES关系** - 同学关系
- **SAME_HOMETOWN关系** - 同乡关系

## 核心功能特性

### 多线程爬取
- 生产者-消费者模式，支持多线程并发爬取
- 智能代理池管理，自动轮换和失效处理
- 请求频率控制，避免被反爬虫机制检测

### 内容验证
- 自动检测百度安全验证页面
- 验证页面内容完整性和有效性
- 过滤无效或异常的网页内容

### AI数据处理
- 支持Azure OpenAI GPT-4o和阿里云Qwen模型
- 自动提取和结构化人物履历信息
- Token使用和成本追踪
- 并发处理与速率限制控制

### 灵活配置
- YAML配置文件，支持热更新
- 模块化设计，支持单独运行各组件
- 详细的日志记录和错误处理

## 常见问题

### Q: 如何处理反爬虫机制？
A: 项目内置代理池管理、请求频率控制、User-Agent轮换等反爬虫策略。建议配置多个代理服务提供商。

### Q: AI处理成本如何控制？
A: 在配置文件中设置`cost_limit`参数，系统会自动追踪Token使用情况并在达到限制时停止处理。

### Q: 数据库表结构如何自定义？
A: 修改`html_extractor/`目录下的JSON schema文件，定义字段映射关系。

### Q: 如何扩展新的数据源？
A: 参考现有的Provider模式，在`proxy/providers.py`中添加新的代理服务商，或在parser模块中添加新的网站解析器。

## 开发贡献

欢迎提交Issue和Pull Request来改进项目。主要改进方向：
- 新的数据源集成
- 解析算法优化  
- 性能和稳定性提升
- 文档和示例完善

## 许可证

本项目采用MIT许可证，详见LICENSE文件。

## 技术栈

- **Python 3.8+** - 主要开发语言
- **Selenium** - 网页自动化和内容获取
- **BeautifulSoup** - HTML解析和内容提取
- **MySQL** - 关系型数据库存储
- **Neo4j** - 图数据库关系分析
- **OpenAI/Qwen** - AI模型API
- **Pandas** - 数据处理和分析
- **PyMySQL** - MySQL数据库连接
- **PyYAML** - 配置文件处理
