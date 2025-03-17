# Leader Graph

A comprehensive Python tool for scraping, parsing, and analyzing Baidu Baike (Chinese Wikipedia) data, with a focus on leadership profiles and biographical information. The project aims to build a graph database of leaders and their relationships for analysis and visualization.

## Features

- **Multi-threaded scraping** with producer-consumer pattern for efficient data collection
- **Proxy management** with support for multiple proxy providers
- **Selenium-based web scraping** with mobile device emulation
- **Content validation** to ensure high-quality data collection
- **HTML caching** to minimize redundant network requests
- **Structured parsing** of biographical information
- **AI-powered data extraction** using GPT-4o/Qwen for biographical event extraction
- **Neo4j integration** for graph-based data analysis

## Project Structure

```
├── config/                 # Configuration module
│   ├── __init__.py         
│   └── settings.py         # Application settings
├── parser/                 # HTML parsing module
│   ├── __init__.py         
│   └── baike_parser.py     # Parser for Baidu Baike content
├── processor/              # Data processing module
│   ├── __init__.py         
│   ├── data_processor.py   # Main processor class
│   └── task_manager.py     # Task scheduling and management
├── proxy/                  # Proxy handling module
│   ├── __init__.py         
│   ├── pool.py             # Proxy pool management
│   └── providers.py        # Different proxy service providers
├── scraper/                # Web scraping module
│   ├── __init__.py         
│   ├── baike_scraper.py    # Baidu Baike specific scraper
│   └── selenium_scraper.py # Selenium-based web browser automation
├── src/                    # Source code for AI data extraction
│   ├── bio_processor.py    # Biographical data extraction with GPT-4o
│   ├── bio_processor_qwen.py # Alternative processor using Qwen model
│   ├── data2neo4j.py       # Import data to Neo4j graph database
│   ├── news_processor.py   # Extract structured info from news articles
│   ├── news_schema.py      # Schema for news extraction
│   └── schema.py           # Schema for biographical data
├── utils/                  # Utility functions
│   ├── __init__.py         
│   ├── content_validator.py # HTML content validation
│   ├── file_utils.py       # File operations
│   ├── html_cache.py       # HTML content caching
│   └── logger.py           # Logging utilities
└── main.py                 # Main entry point
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/leader_graph.git
cd leader_graph
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

## Configuration

1. Create a `config.yaml` file (a template will be generated on first run if not present)
2. Configure the following settings:
   - Input CSV file path containing the list of persons to scrape
   - Output directory for scraped and processed data
   - Proxy providers and credentials
   - Processing parameters (threads, retry limits, etc.)
   - AI API keys for data extraction (if using AI processing)

Example configuration:
```yaml
input_csv_path: "./shanghai_leadership_list.csv"
output_dir: "./data"
num_producers: 10
num_consumers: 1
use_mobile: true
max_retries: 3
use_proxy: true
proxy_config:
  providers:
    - type: "xiaoxiang"
      app_key: "your_api_key"
      app_secret: "your_api_secret"
  refresh_interval: 15
  min_proxies: 20
save_interval: 10
request_delay_min: 1.0
request_delay_max: 3.0
min_content_size: 1024
azure_openai_endpoint: "your_azure_endpoint"
azure_openai_api_key: "your_azure_api_key"
azure_openai_api_version: "2024-10-21"
qwen_api_key: "your_qwen_api_key"
```

## Usage

### Basic Usage

Run the main scraper with the default configuration:
```bash
python main.py
```

### Specific Processing Stages

Run only specific processing stages:
```bash
# Just fetch HTML content
python main.py --stage fetch

# Just parse already fetched HTML
python main.py --stage parse

# Run full process (fetch and parse)
python main.py --stage full
```

### AI-Based Biographical Data Extraction

Process biographical data using GPT-4o:
```bash
python src/bio_processor.py
```

Or using Qwen model:
```bash
python src/bio_processor_qwen.py
```

### Neo4j Graph Database Import

Import processed data to Neo4j:
```bash
python src/data2neo4j.py
```

## Input Data Format

The input CSV should contain at minimum the following columns:
- `person_id`: Unique identifier for the person
- `person_name`: Person's name
- `person_url`: Baidu Baike URL for the person

## Output Data

### Raw Data
- HTML files of Baidu Baike pages stored in the specified output directory
- Each file is named using the format: `{safe_name}_{person_id}.html`

### Processed Data
- Parsed biographical information stored in CSV format
- Extracted structured events in JSON format (when using AI processing)
- Graph database (when using Neo4j import)

## Acknowledgements

- [Selenium](https://www.selenium.dev/)
- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/)
- [OpenAI](https://openai.com/) / [Azure OpenAI](https://azure.microsoft.com/en-us/products/ai-services/openai-service)
- [Qwen](https://qianwen.aliyun.com/)
- [Neo4j](https://neo4j.com/)
