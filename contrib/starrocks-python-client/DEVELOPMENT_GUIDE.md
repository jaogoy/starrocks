# StarRocks Python Client - 开发指南

本文档为 `starrocks-python-client` 项目提供了一个结构化的开发任务列表，旨在指导后续的开发迭代工作。

---

## 开发任务树 (V1.0)

此任务列表基于 PRD、设计文档和初步的代码评估制定。

### 第一阶段：模型与方言增强 (Foundation: Model & Dialect Enhancement)

*   **1. 定义 `View` 和 `MaterializedView` 的 Python 模型**
    *   **状态:** ✅ 已完成
    *   **任务:** 在 `starrocks/sql/schema.py` 中创建 `View` 和 `MaterializedView` 类。
    *   **细节:** 参考设计文档 5.5 节的实现，包含 `name`, `definition`, `schema`, `columns`, `properties` (仅MV) 等核心属性。这些类应继承自 `sqlalchemy.schema.SchemaItem`。
    *   **验收标准:** 可以通过 `view = View(...)` 和 `mv = MaterializedView(...)` 创建实例。

*   **2. 增强 `Table` 定义以支持 StarRocks 特性**
    *   **状态:** ✅ 已完成
    *   **任务:** 确定并标准化在 `Table` 对象中声明 StarRocks 特性（Key Type, Partition, Distribution 等）的方式。
    *   **细节:** 在 `starrocks/params.py` 中定义所有 `starrocks_*` 前缀的关键字常量。确保这些属性可以通过 `Table` 对象的 `__table_args__` 字典（用于 ORM 模型）和直接作为 `Table` 构造函数的 `**kwargs` 来声明。
    *   **验收标准:** 用户可以清晰地通过 **ORM** 和 **Core** 两种方式定义一个包含所有 StarRocks 核心特性的表，并提供相应的示例。

*   **3. 增强 DDL 编译器 (`Compiler`)**
    *   **状态:** ✅ 已完成
    *   **任务:** 扩展 `starrocks/compiler.py` 中的 `StarRocksDDLCompiler`，使其能根据模型生成正确的 DDL。
    *   **细节:**
        *   **`visit_create_table`**: 解析 `Table` 对象中的 `starrocks_*` 参数，生成 `DUPLICATE KEY`, `PARTITION BY`, `DISTRIBUTED BY`, `ORDER BY` 和 `PROPERTIES` 子句。
        *   **`visit_create_view` & `visit_create_materialized_view`**: 根据 `View`/`MV` 对象生成 `CREATE` 语句。
        *   **`visit_drop_view` & `visit_drop_materialized_view`**: 生成对应的 `DROP` 语句。
    *   **验收标准:** SQLAlchemy 的 DDL 执行 (`metadata.create_all()`) 能够正确创建包含 StarRocks 特性的表、视图和物化视图。

### 第二阶段：反射机制完善 (Reflection Enhancement)

*   **1. 完善 `Table` 的反射**
    *   **状态:** 🔶 部分完成
    *   **任务:** 增强 `starrocks/reflection.py` 中的 `StarRocksInspector` 的 `get_table_options` 方法。
    *   **细节:** **优先通过查询 `information_schema`** 来提取 StarRocks 特有属性。如果 `information_schema` 无法提供所有信息，再以解析 `SHOW CREATE TABLE` 作为备选方案。将提取出的信息以 `starrocks_*` 为前缀的 key 存入返回的字典中。**已实现 `SHOW CREATE TABLE` 的备用解析逻辑，但需要集成测试验证。**
    *   **验收标准:** `inspector.get_table_options('my_table')` 能返回一个包含所有 StarRocks 特性的字典。

*   **2. 实现 `View` 和 `MV` 的反射**
    *   **状态:** ✅ 已完成
    *   **任务:** 在 `starrocks/reflection.py` 中添加对视图和物化视图的反射能力。
    *   **细节:** 实现 `get_view_names`, `get_view_definition`, `get_materialized_view_names`, `get_materialized_view_definition` 等方法，统一通过查询 `information_schema` 实现。
    *   **验收标准:** `inspector` 能够成功获取数据库中所有 `View` 和 `MV` 的名称、定义和属性。

### 第三阶段：Alembic 集成 (Alembic Integration)

*   **1. 创建 Alembic 自定义 `ops`**
    *   **状态:** ✅ 已完成
    *   **任务:** 在 `starrocks/alembic/ops.py` 中定义与 `View`, `MV` 及 `Table` 特殊变更相关的 Alembic 操作。
    *   **细节:** 创建 `CreateViewOp`, `DropViewOp`, `AlterTablePropertiesOp` 等，并实现其 `reverse()` 方法以支持 `downgrade`。
    *   **验收标准:** 这些 `Op` 对象可以在 Alembic 迁移脚本中被调用。

*   **2. 实现 `autogenerate` 差异对比**
    *   **状态:** 🔶 部分完成
    *   **任务:** 在 `starrocks/alembic/compare.py` 中实现自定义的比较逻辑。
    *   **细节:**
        *   **`@comparators.dispatch_for("schema")`**: 实现 `compare_views` 和 `compare_materialized_views` 函数。
        *   **`@comparators.dispatch_for("table")`**: 实现 `compare_starrocks_table_options` 函数，对比 `starrocks_*` 属性。
        *   **`@comparators.dispatch_for("column")`**: 实现 `compare_starrocks_column_options` 函数，对比列级别的特有属性。
    *   **验收标准:** `autogenerate` 能够为 `Table`, `View`, `MV` 和 `Column` 的增、删、改生成正确的迁移脚本。

*   **3. 实现自定义操作的 SQL 渲染**
    *   **状态:** ✅ 已完成
    *   **任务:** 在 `starrocks/alembic/render.py` 中，为每个自定义 `Op` 实现 SQL 渲染逻辑。
    *   **细节:** 使用 `@renderers.dispatch_for(...)` 装饰器，将 `Op` 对象转换为最终的 DDL 字符串。
    *   **验收标准:** `alembic upgrade <revision> --sql` 能够打印出正确的 StarRocks DDL 语句。

### 第四阶段：测试与文档 (Testing & Documentation)

*   **1. 编写集成测试**
    *   **状态:** 🔶 部分完成
    *   **任务:** 在 `test/` 目录下，为 Alembic `autogenerate` 编写端到端的集成测试。
    *   **细节:** 覆盖 `Table`, `View`, `MV` 的创建、修改、删除场景，验证脚本生成、`upgrade` 和 `downgrade` 的正确性。
    *   **验收标准:** 自动化测试能验证整个流程的正确性。

*   **2. 编写用户文档**
    *   **状态:** ❌ 待办
    *   **任务:** 在 `README.md` 或 `docs/` 目录下，提供详细的用户指南。
    *   **细节:** 包含 ORM/Core 示例、Alembic 配置、完整工作流示例和限制说明。
    *   **验收标准:** 用户根据文档可以独立完成整个 Schema 迁移过程。

*(图例: ✅ = 已完成, 🔶 = 部分完成, ❌ = 待办)*