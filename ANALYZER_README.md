# 🤖 AI SQL Table Analyzer

自动分析 Snowflake 数据库表结构的工具，使用 AI_COMPLETE 函数进行智能分析。

## 📋 功能特性

- ✅ **插件式配置**：通过 YAML 配置文件轻松选择数据库和表
- 🔍 **智能采样**：随机抽取指定行数进行分析
- 🤖 **AI 分析**：使用 Snowflake Cortex AI_COMPLETE 自动识别列类型和用途
- 📊 **详细报告**：生成 JSON 格式的完整分析结果
- 🎨 **美观展示**：提供格式化的结果查看器

## 🚀 快速开始

### 1. 配置目标表

编辑 `database_config.yaml` 文件：

```yaml
# 用户信息（模拟认证）
user:
  username: "DEMO_USER"
  role: "ACCOUNTADMIN"
  accessible_databases:
    - "AI_SQL_COMP"
    - "FINANCE_DB"

# 分析目标
analysis_target:
  database: "AI_SQL_COMP"
  schema: "PUBLIC"

  # 要分析的表（注释掉不需要的表）
  tables:
    - name: "STYLESOURCEBOOK_PRODUCTS"
      enabled: true
      sample_size: 10

    - name: "STYLESOURCEBOOK_RETAILERS"
      enabled: true
      sample_size: 10

    - name: "TEST_PRODUCTS"
      enabled: false  # 禁用此表
      sample_size: 10

# AI 配置
ai_config:
  model: "mistral-large"  # 或 "claude-3-7-sonnet"
```

### 2. 运行分析

```bash
python analyze_tables.py
```

**示例输出：**

```
██████████████████████████████████████████████████████████████████████
  Table Structure Analyzer
  Database: AI_SQL_COMP
  User: DEMO_USER
  AI Model: mistral-large
██████████████████████████████████████████████████████████████████████

📋 Tables to analyze: 3
  • STYLESOURCEBOOK_PRODUCTS (10 rows)
  • STYLESOURCEBOOK_PRODUCT_IMAGES (10 rows)
  • STYLESOURCEBOOK_RETAILERS (10 rows)

======================================================================
📊 Analyzing: STYLESOURCEBOOK_PRODUCTS
======================================================================
  🔍 Sampling 10 rows...
  ✓ Retrieved 10 rows with 33 columns
  🤖 Calling AI_COMPLETE with model: mistral-large...
  ✓ AI analysis completed
```

### 3. 查看结果

```bash
python view_results.py
```

**输出示例：**

```
────────────────────────────────────────────────────────────────────────
📊 TABLE: STYLESOURCEBOOK_PRODUCTS
────────────────────────────────────────────────────────────────────────
✅ Status: SUCCESS
Sample Size: 10 rows
Total Columns: 33

📝 Table Purpose:
   This table stores detailed information about products, including
   their attributes, pricing, availability, and associated images.

📋 Column Analysis (33 columns):
    1. ID                        [Integer]
       → Unique identifier for each product
    2. NAME                      [String]
       → Name of the product
    3. PRICE                     [Float]
       → Product price
   ...

🔍 Data Quality Observations:
   Some null values in 'WP_ID', 'COLOUR_ID'. The 'LAST_CHANGED_AT'
   column has some dates that seem incorrect (negative years).
```

## 📁 输出文件

- **analysis_results.json** - 完整的分析结果（JSON 格式）
  - 包含配置信息
  - 每个表的分析结果
  - AI 识别的列信息
  - 数据质量观察

## 🏗️ 代码架构

```
analyze_tables.py          # 主分析脚本
├── DatabaseConfig         # 配置管理
├── TableAnalyzer          # 分析引擎
    ├── sample_table()     # 采样数据
    ├── analyze_table()    # AI 分析
    └── save_results()     # 保存结果

view_results.py            # 结果查看器

database_config.yaml       # 配置文件（可修改）
```

## 🔧 技术实现

### 模拟前端请求

代码通过直接调用服务层模拟前端 API 请求：

```python
# 模拟 POST /api/v1/ai-sql/complete
ai_response = await self.ai_service.ai_complete(
    model=self.config.ai_model,
    prompt=prompt
)
```

这相当于前端发送：

```javascript
POST /api/v1/ai-sql/complete
{
  "model": "mistral-large",
  "prompt": "Analyze this table: ..."
}
```

### 智能采样

```python
# 随机采样 N 行
SELECT *
FROM {database}.{schema}.{table}
SAMPLE (10 ROWS)
```

### AI 提示词

```
Table: PRODUCTS
Total Columns: 33
Columns: ID, NAME, PRICE, ...

Sample Data (first 5 rows):
Row 1: {...}
Row 2: {...}

Please analyze this table and provide:
1. Column names and their inferred data types
2. Brief description of what each column represents
3. Overall purpose of this table
4. Any data quality observations

Return your analysis in JSON format.
```

## 📊 分析结果字段

```json
{
  "table_name": "PRODUCTS",
  "status": "success",
  "sample_size": 10,
  "actual_columns": ["ID", "NAME", ...],
  "ai_analysis": {
    "table_name": "PRODUCTS",
    "total_columns": 33,
    "columns": [
      {
        "name": "ID",
        "type": "Integer",
        "description": "Unique identifier"
      }
    ],
    "table_purpose": "Stores product information...",
    "data_quality_notes": "Some null values in..."
  }
}
```

## 🎯 使用场景

1. **数据库探索** - 快速了解新数据库的表结构
2. **文档生成** - 自动生成数据字典
3. **数据质量检查** - 发现潜在的数据问题
4. **Schema 迁移** - 理解源数据结构

## ⚙️ 自定义配置

### 修改采样大小

```yaml
tables:
  - name: "LARGE_TABLE"
    enabled: true
    sample_size: 50  # 增加到 50 行
```

### 切换 AI 模型

```yaml
ai_config:
  model: "claude-3-7-sonnet"  # 更强大但慢一点
  # model: "mistral-large"    # 更快
```

### 只分析特定表

```yaml
tables:
  - name: "TABLE_1"
    enabled: true   # ✅ 分析

  - name: "TABLE_2"
    enabled: false  # ❌ 跳过
```

## 🔍 故障排除

### 时间戳转换错误

如果遇到 `Timestamp is not recognized` 错误，代码会自动将时间戳转换为字符串：

```python
# 自动处理
TO_CHAR(timestamp_column) as timestamp_column
```

### 连接问题

确保 `.env` 文件配置正确：

```env
SNOWFLAKE_ACCOUNT=xxx
SNOWFLAKE_USER=xxx
SNOWFLAKE_PASSWORD=xxx
SNOWFLAKE_WAREHOUSE=xxx
```

### SQL 语法错误

某些表可能有保留字作为列名，代码会尝试处理，但如果失败会在结果中标记为 error。

## 📝 下一步计划

- [ ] 添加真实的 API 端点调用（通过 HTTP）
- [ ] 支持批量数据库分析
- [ ] 生成 Markdown/HTML 格式的报告
- [ ] 添加列之间的关系推断
- [ ] 集成数据质量评分

## 🤝 贡献

欢迎提交 Issue 和 PR！

---

**生成时间**: 2025-12-19
**AI 模型**: Snowflake Cortex (mistral-large / claude-3-7-sonnet)
