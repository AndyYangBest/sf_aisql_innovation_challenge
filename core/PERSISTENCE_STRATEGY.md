# EDA Workflow 持久化策略

## 📋 总结

### 当前状态
- ✅ Workflow 结果保存在本地文件: `~/.strands/workflows/{workflow_id}.json`
- ❌ 没有持久化到数据库
- ❌ 服务器重启/容器销毁会丢失数据
- ❌ 无法查询历史记录

### 推荐方案: 双写策略 ⭐

**保留 Strands 本地文件 + 同步到 PostgreSQL**

---

## 🗄️ 数据库设计

### 表 1: `eda_workflow_executions` (必须)

存储 workflow 执行记录和结果

```sql
CREATE TABLE eda_workflow_executions (
    id SERIAL PRIMARY KEY,
    workflow_id VARCHAR(255) UNIQUE NOT NULL,
    workflow_type VARCHAR(50) NOT NULL,  -- EDA_OVERVIEW, EDA_TIME_SERIES, etc.

    -- 关联
    table_asset_id INTEGER REFERENCES table_assets(id),
    user_id INTEGER,

    -- 状态
    status VARCHAR(50) DEFAULT 'pending',  -- pending, running, completed, failed
    progress INTEGER DEFAULT 0,  -- 0-100

    -- 任务统计
    tasks_total INTEGER DEFAULT 0,
    tasks_completed INTEGER DEFAULT 0,
    tasks_failed INTEGER DEFAULT 0,

    -- 时间
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds FLOAT,

    -- 结果 (JSON)
    artifacts JSONB,  -- 所有任务结果
    summary JSONB,    -- 摘要信息

    -- 类型检测结果 (重要! 可以直接查询)
    data_structure_type VARCHAR(50),  -- panel, time_series, etc.
    column_type_inferences JSONB,     -- 所有列的类型推断

    -- 元数据
    user_intent TEXT,
    error_message TEXT,

    -- 审计
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_deleted BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_workflow_executions_table_asset ON eda_workflow_executions(table_asset_id);
CREATE INDEX idx_workflow_executions_status ON eda_workflow_executions(status);
CREATE INDEX idx_workflow_executions_type ON eda_workflow_executions(workflow_type);
CREATE INDEX idx_workflow_executions_structure_type ON eda_workflow_executions(data_structure_type);
```

### 表 2: `eda_workflow_logs` (可选)

存储重要的 workflow 事件日志

```sql
CREATE TABLE eda_workflow_logs (
    id SERIAL PRIMARY KEY,
    workflow_execution_id INTEGER REFERENCES eda_workflow_executions(id) ON DELETE CASCADE,

    -- 分类
    log_level VARCHAR(20) NOT NULL,  -- INFO, WARNING, ERROR
    log_type VARCHAR(50) NOT NULL,   -- workflow_started, task_completed, tool_called, etc.

    -- 上下文
    task_id VARCHAR(100),
    tool_name VARCHAR(100),

    -- 内容
    message TEXT NOT NULL,
    details JSONB,

    -- 性能
    duration_seconds FLOAT,

    -- 时间
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_workflow_logs_execution ON eda_workflow_logs(workflow_execution_id);
CREATE INDEX idx_workflow_logs_level ON eda_workflow_logs(log_level);
CREATE INDEX idx_workflow_logs_type ON eda_workflow_logs(log_type);
CREATE INDEX idx_workflow_logs_timestamp ON eda_workflow_logs(timestamp);
```

---

## 💾 实现方案

### 已创建的文件

1. **`src/app/models/eda_workflow.py`** ✅
   - `EDAWorkflowExecution` model
   - `EDAWorkflowLog` model

2. **`src/app/services/eda_workflow_persistence.py`** ✅
   - `EDAWorkflowPersistenceService` class
   - 提供 CRUD 操作

### 需要集成的地方

#### 1. 在 `eda_workflows.py` 中集成

```python
class EDAOrchestrator:
    def __init__(self, ..., db: AsyncSession | None = None):
        self.db = db  # 接收数据库会话

    async def run_eda(self, table_asset, user_intent, workflow_type, user_id=None):
        # 1. 创建数据库记录
        if self.db:
            persistence = EDAWorkflowPersistenceService(self.db)
            execution = await persistence.create_execution(
                workflow_id=workflow_id,
                workflow_type=workflow_type,
                table_asset_id=table_asset.id,
                user_intent=user_intent,
                user_id=user_id,
                tasks_total=len(tasks),
            )

        try:
            # 2. 执行 Strands workflow (写入本地文件)
            ...

            # 3. 读取本地文件结果
            workflow_data = json.load(open(workflow_file))
            artifacts = self._extract_artifacts_from_workflow_data(workflow_data)
            summary = self._generate_summary_from_workflow_data(workflow_data)

            # 4. 更新数据库记录
            if self.db:
                await persistence.complete_execution(
                    workflow_id=workflow_id,
                    artifacts=artifacts,
                    summary=summary,
                )

        except Exception as e:
            # 5. 失败时更新状态
            if self.db:
                await persistence.fail_execution(
                    workflow_id=workflow_id,
                    error_message=str(e),
                )
            raise
```

#### 2. 在 API 端点中使用

```python
# src/app/api/v1/eda.py
from app.core.db.database import async_get_db

@router.post("/analyze/{table_asset_id}")
async def analyze_table(
    table_asset_id: int,
    user_intent: str = None,
    db: AsyncSession = Depends(async_get_db),
):
    # 获取 table asset
    table_asset = await get_table_asset(db, table_asset_id)

    # 创建 orchestrator (传入 db)
    orchestrator = create_eda_orchestrator(
        sf_service,
        ai_sql_service,
        db=db  # ⭐ 传入数据库会话
    )

    # 运行分析
    results = await orchestrator.run_eda(
        table_asset=table_asset,
        user_intent=user_intent,
        user_id=current_user.id,  # 从认证中获取
    )

    return results

@router.get("/history/{table_asset_id}")
async def get_analysis_history(
    table_asset_id: int,
    db: AsyncSession = Depends(async_get_db),
):
    """获取某个表的分析历史"""
    persistence = EDAWorkflowPersistenceService(db)
    executions = await persistence.get_executions_for_table(table_asset_id)

    return {
        "table_asset_id": table_asset_id,
        "executions": [
            {
                "id": e.id,
                "workflow_id": e.workflow_id,
                "workflow_type": e.workflow_type,
                "status": e.status,
                "data_structure_type": e.data_structure_type,
                "started_at": e.started_at,
                "completed_at": e.completed_at,
                "duration_seconds": e.duration_seconds,
            }
            for e in executions
        ]
    }
```

---

## 📊 使用场景

### 1. 查询分析历史

```python
# 获取某个表的所有分析记录
executions = await persistence.get_executions_for_table(table_asset_id=1)

for execution in executions:
    print(f"Workflow: {execution.workflow_type}")
    print(f"Status: {execution.status}")
    print(f"Data Structure: {execution.data_structure_type}")
    print(f"Completed: {execution.completed_at}")
```

### 2. 统计分析

```sql
-- 分析成功率
SELECT
    workflow_type,
    COUNT(*) as total,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as success,
    AVG(duration_seconds) as avg_duration
FROM eda_workflow_executions
GROUP BY workflow_type;

-- 最常分析的表
SELECT
    table_asset_id,
    COUNT(*) as analysis_count,
    MAX(completed_at) as last_analyzed
FROM eda_workflow_executions
WHERE status = 'completed'
GROUP BY table_asset_id
ORDER BY analysis_count DESC
LIMIT 10;

-- 数据结构类型分布
SELECT
    data_structure_type,
    COUNT(*) as count
FROM eda_workflow_executions
WHERE data_structure_type IS NOT NULL
GROUP BY data_structure_type;
```

### 3. 重用历史结果

```python
# 检查是否有最近的分析结果
recent_execution = await db.execute(
    select(EDAWorkflowExecution)
    .where(
        EDAWorkflowExecution.table_asset_id == table_asset_id,
        EDAWorkflowExecution.status == "completed",
        EDAWorkflowExecution.completed_at > datetime.now() - timedelta(hours=24)
    )
    .order_by(EDAWorkflowExecution.completed_at.desc())
    .limit(1)
)

if recent_execution:
    # 直接返回缓存的结果
    return recent_execution.artifacts
else:
    # 运行新的分析
    results = await orchestrator.run_eda(...)
```

---

## 🚀 迁移步骤

### 1. 创建数据库表

```bash
# 生成迁移文件
alembic revision --autogenerate -m "add eda workflow execution tables"

# 运行迁移
alembic upgrade head
```

### 2. 更新代码

- ✅ Models 已创建 (`eda_workflow.py`)
- ✅ Service 已创建 (`eda_workflow_persistence.py`)
- ⏳ 需要修改 `eda_workflows.py` 集成持久化
- ⏳ 需要在 API 端点中传入 `db` 参数

### 3. 测试

```python
# 测试持久化
async def test_persistence():
    async for db in async_get_db():
        orchestrator = create_eda_orchestrator(
            sf_service,
            ai_sql_service,
            db=db  # 传入数据库会话
        )

        results = await orchestrator.run_eda(
            table_asset=table_asset,
            user_intent="Test persistence",
        )

        # 验证数据库中有记录
        execution = await db.execute(
            select(EDAWorkflowExecution).where(
                EDAWorkflowExecution.workflow_id == results["workflow_id"]
            )
        )
        assert execution is not None
        assert execution.status == "completed"
        assert execution.artifacts is not None
```

---

## ⚠️ 注意事项

### 1. 数据量控制

**Artifacts 可能很大** (包含完整的 JSON 结果)

策略:
- ✅ 使用 JSONB 类型 (PostgreSQL 压缩存储)
- ✅ 定期清理旧记录 (保留最近 90 天)
- ⚠️ 考虑只保存摘要，完整结果存 S3

### 2. 日志量控制

**Hook 日志非常多**

策略:
- ✅ 只保存重要日志 (workflow_started, task_completed, task_failed)
- ✅ 不保存每个 tool call 的详细日志
- ✅ 定期清理 (保留最近 30 天)

### 3. 性能考虑

- ✅ 异步写入数据库 (不阻塞 workflow)
- ✅ 使用索引加速查询
- ⚠️ 考虑使用后台任务写入日志

---

## 🎯 推荐实施顺序

1. **Phase 1: 基础持久化** (必须)
   - ✅ 创建 `eda_workflow_executions` 表
   - ✅ 集成到 `EDAOrchestrator`
   - ✅ 在 API 中传入 `db` 参数

2. **Phase 2: 历史查询** (推荐)
   - 添加 API 端点查询历史
   - 添加统计分析功能
   - 实现结果缓存

3. **Phase 3: 日志持久化** (可选)
   - 创建 `eda_workflow_logs` 表
   - 修改 hooks 写入数据库
   - 实现日志查询和分析

---

## 📚 相关文件

- ✅ `src/app/models/eda_workflow.py` - 数据库模型
- ✅ `src/app/services/eda_workflow_persistence.py` - 持久化服务
- ⏳ `src/app/orchestration/eda_workflows.py` - 需要集成
- ⏳ `src/app/api/v1/eda.py` - 需要添加 API 端点
- ⏳ `alembic/versions/xxx_add_eda_workflow_tables.py` - 需要创建迁移

---

## ✅ 总结

### 应该保存到 PostgreSQL:
1. ✅ **Workflow 执行记录** (必须) - 持久化、可查询、可统计
2. ✅ **类型检测结果** (必须) - 方便查询和分析
3. ⚠️ **Hook 日志** (可选) - 只保存重要日志，控制数量

### 不需要保存:
- ❌ 本地 Strands 文件可以保留 (作为备份)
- ❌ 详细的 tool call 日志 (太多了)

### 实施建议:
- 使用**双写策略**: Strands 本地文件 + PostgreSQL
- 先实施 Phase 1 (基础持久化)
- 根据需求逐步添加 Phase 2 和 Phase 3
