# StarRocks Python Client - 开发指南

本文档为 `starrocks-python-client` 项目提供了一个结构化的开发任务列表，旨在指导后续的开发迭代工作。

---

## 开发任务树 (V1.0) - 总体进度: ~85%

此任务列表基于 PRD、设计文档和初步的代码评估制定。

对于每个对象，都有 模型定义、compiler、inspect、compare、render 等几个过程，每个都得检查。

### 第一阶段：模型与方言增强 (Foundation: Model & Dialect Enhancement)

- **1. 定义 `View` 和 `MaterializedView` 的 Python 模型**

  - **进度:** 100%
  - **状态:** ✅ 已完成
  - **任务:** 在 `starrocks/sql/schema.py` 中创建 `View` 和 `MaterializedView` 类。
  - **细节:** 参考设计文档 5.5 节的实现，包含 `name`, `definition`, `schema`, `columns`, `properties` (仅 MV) 等核心属性。这些类应继承自 `sqlalchemy.schema.SchemaItem`。
  - **验收标准:** 可以通过 `view = View(...)` 和 `mv = MaterializedView(...)` 创建实例。
  - **子任务清单:**
    - ✅ `View` 类的定义 (`starrocks/sql/schema.py`)
    - ✅ `MaterializedView` 类的定义 (`starrocks/sql/schema.py`)

- **2. 增强 `Table` 定义以支持 StarRocks 特性**

  - **进度:** 95%
  - **状态:** 💹 已推进 (接近完成)
  - **任务:** 确定并标准化在 `Table` 对象中声明 StarRocks 特性（Key Type, Partition, Distribution 等）的方式。
  - **细节:** 在 `starrocks/params.py` 中定义所有 `starrocks_*` 前缀的关键字常量。确保这些属性可以通过 `Table` 对象的 `__table_args__` 字典（用于 ORM 模型）和直接作为 `Table` 构造函数的 `**kwargs` 来声明。
  - **验收标准:** 用户可以清晰地通过 **ORM** 和 **Core** 两种方式定义一个包含所有 StarRocks 核心特性的表，并提供相应的示例。
  - **子任务清单:**
    - ✅ 支持通过 `Table` 构造函数的 kwargs 定义 `starrocks_*` 属性。
    - ✅ 支持通过 ORM 模型的 `__table_args__` 定义 `starrocks_*` 属性。
    - ❌ **待办:** 在 `sqlalchemy.Index` 上支持 `starrocks_using='BITMAP'` 的定义方式。

- **3. 增强 DDL 编译器 (`Compiler`)**
  - **进度:** 80%
  - **状态:** 💹 已推进 (核心完成)
  - **任务:** 扩展 `starrocks/compiler.py` 中的 `StarRocksDDLCompiler`，使其能根据模型生成正确的 DDL。
  - **细节:** 编译器采用访问者模式，为每个需要自定义 SQL 的 DDL 元素实现 `visit_*` 方法。
  - **验收标准:** SQLAlchemy 的 DDL 执行 (`metadata.create_all()`) 以及 `alembic upgrade` 能够正确创建和修改包含 StarRocks 特性的表、视图和物化视图。
  - **子任务清单:**
    - ✅ `visit_create_table`: 支持编译 `ENGINE`, `KEY`, `PARTITION BY`, `DISTRIBUTED BY`, `ORDER BY`, `PROPERTIES` 子句。
    - ✅ `visit_alter_table`: 支持编译 `DISTRIBUTED BY`, `ORDER BY`, `SET PROPERTIES` 子句。
    - ✅ `visit_alter_column`: 除了 AGG_TYPE，应该原有的都支持。
    - ✅ `visit_create_view` 和 `visit_drop_view`，`alter_view`。
    - ✅ `visit_create_materialized_view`: 当前仅支持编译 `definition` 和 `properties`。
    - ❌ **待办: `visit_create_materialized_view`**: 需增强以支持编译 `PARTITION BY`, `DISTRIBUTED BY`, `ORDER BY`, `REFRESH` 子句。
    - ✅ `visit_drop_materialized_view`。
    - ❌ **待办: `visit_alter_materialized_view`**: 需新增以支持编译 `RENAME`, `SET PROPERTIES`, `REFRESH`, `SET ACTIVE/INACTIVE`。
    - ❌ **待办: `visit_create_index`**: 需实现对 `USING BITMAP` 子句的编译。
    - ❌ **待办: `visit_drop_index`**: 确保索引可以被正确删除。

### 第二阶段：反射机制完善 (Reflection Enhancement)

- **1. 完善 `Table` 的反射**

  - **进度:** 90%
  - **状态:** 💹 已推进 (核心完成)
  - **任务:** 增强 `starrocks/reflection.py` 中的 `StarRocksInspector` 以完整反射 `Table` 级别的所有元数据。
  - **细节:** 这包括表的特有属性（如分桶、PROPERTIES）以及附属于表的索引信息。应优先使用 `information_schema`。
  - **验收标准:** `inspector.get_table_options()` 和 `inspector.get_indexes()` 能返回包含所有 StarRocks 表级特性的完整、准确信息。
  - **子任务清单:**
    - ✅ `get_table_options`: 已能反射 `ENGINE`, `KEY`, `DISTRIBUTION`, `PARTITION`, `PROPERTIES`。
    - ✅ `get_pk_constraint`, `get_unique_constraints`: 已有实现。（但需要约束下，只使用 `starrocks_PRIMARY_KEY` 等）
    - ❌ **待办: `get_indexes`**: 需增加对 `BITMAP` 索引类型的识别。

- **2. 完善 `Column` 的反射**

  - **进度:** 90%
  - **状态:** 💹 已推进 (核心完成)
  - **任务:** 增强 `StarRocksInspector` 的 `get_columns` 方法以完整反射 `Column` 级别的所有元数据。
  - **细节:** 除了标准属性，还需要特别处理 StarRocks 特有的 `AUTO_INCREMENT` 标志。
  - **验收标准:** `inspector.get_columns()` 能返回包含所有 StarRocks 列级特性的完整、准确信息，特别是 `AUTO_INCREMENT`。
  - **子任务清单:**
    - ✅ `get_columns`: 已能反射 `type`, `nullable`, `default`, `comment`。
    - ❌ **待办: `get_columns`**: 需增加对 `AUTO_INCREMENT` 属性的反射支持。这是一个明确的缺失功能点。

- **3. 实现 `View` 和 `MV` 的反射**
  - **进度:** 70%
  - **状态:** ✅ 已完成
  - **任务:** 在 `starrocks/reflection.py` 中添加对视图和物化视图的反射能力。
  - **细节:** 实现 `get_view_names`, `get_view_definition`, `get_materialized_view_names`, `get_materialized_view_definition` 等方法，统一通过查询 `information_schema` 实现。
  - **验收标准:** `inspector` 能够成功获取数据库中所有 `View` 和 `MV` 的名称、定义和属性。
  - **子任务清单:**
    - ✅ `get_view_names` 和 `get_view_definition`。
    - ✅ `get_materialized_view_names` 和 `get_materialized_view_definition` (当前仅支持 `properties`)。
    - ❌ **待办: 增强 `MV` 反射**: 需增加对 `PARTITION BY`, `DISTRIBUTED BY`, `ORDER BY`, `REFRESH` 策略, `STATUS` (ACTIVE/INACTIVE) 等属性的反射支持。

### 第三阶段：Alembic 集成 (Alembic Integration)

- **1. 创建 Alembic 自定义 `ops`**

  - **进度:** 90%
  - **状态:** ✅ 已完成
  - **任务:** 在 `starrocks/alembic/ops.py` 中定义与 `View`, `MV` 及 `Table` 特殊变更相关的 Alembic 操作。
  - **细节:** 创建 `CreateViewOp`, `DropViewOp`, `AlterTablePropertiesOp` 等，并实现其 `reverse()` 方法以支持 `downgrade`。
  - **验收标准:** 这些 `Op` 对象可以在 Alembic 迁移脚本中被调用，并且它们的 `reverse()` 方法是正确的。
  - **子任务清单:**
    - ✅ `View` 的 `Create`/`Drop`/`Alter` 操作已定义。
    - ✅ `MV` 的 `Create`/`Drop` 操作已定义。
    - ❌ **待办: 增强 `AlterMaterializedViewOp`**: 需实现对 `rename`, `set_properties`, `set_refresh_scheme`, `set_status` 等操作的支持。
    - ✅ `Table` 的 `Alter` 操作（如 `AlterTablePropertiesOp`, `AlterTableDistributionOp` 等）已定义。
    - ✅ 所有操作均已实现 `reverse()` 方法。

- **2. 实现 `autogenerate` 差异对比**

  - **进度:** 70%
  - **状态:** 💹 已推进 (核心完成)
  - **任务:** 在 `starrocks/alembic/compare.py` 中实现自定义的比较逻辑。
  - **细节:** 使用 Alembic 提供的 `@comparators.dispatch_for` 装饰器来注册自定义的比较函数，以插件化的方式扩展 `autogenerate` 的能力。
  - **验收标准:** `alembic revision --autogenerate` 能够为 `Table`, `View`, `MV`, `Column`, `Index` 的增、删、改生成正确的迁移脚本。
  - **子任务清单:**
    - ✅ `schema` 级对比: 支持 `View` 的 `CREATE`, `DROP`, `ALTER`。
    - ✅ `schema` 级对比: 支持 `MV` 的 `CREATE`, `DROP`。
    - ❌ **待办: 增强 `MV` 对比**: 需增加对 `PARTITION BY`, `DISTRIBUTED BY`, `ORDER BY`, `REFRESH` 策略, `STATUS` 等属性的变更检测，**包括对 `ALTER MATERIALIZED VIEW` 操作的完整支持**。
    - ✅ `table` 级对比: 支持 `ENGINE`, `KEY`, `DISTRIBUTION`, `PARTITION`, `PROPERTIES`, `COMMENT` 的变更检测。
    - ✅ `column` 级对比: 支持 `type`, `nullable`, `default`, `comment`, `agg_type` 的变更检测。
    - ✅ 对不支持的 `agg_type` 变更会主动抛出异常。
    - ❌ **待办:** 对不支持的 `auto_increment` 暂时没实现。
    - ❌ **待办:** `index` 级对比: 需确保 Alembic 的默认索引对比逻辑能够正确处理我们新增的 `starrocks_using='BITMAP'` 参数。

- **3. 实现自定义操作的 SQL 渲染**

  - **进度:** 100%
  - **状态:** ✅ 已完成
  - **任务:** 在 `starrocks/alembic/render.py` 中，为每个自定义 `Op` 实现 SQL 渲染逻辑。
  - **细节:** 使用 `@renderers.dispatch_for(...)` 装饰器，将 `Op` 对象转换为最终的 DDL 字符串。
  - **验收标准:** `alembic upgrade <revision> --sql` 能够打印出正确的 StarRocks DDL 语句。
  - **子任务清单:**
    - ✅ 所有自定义 `Op` 均已实现 Python 代码渲染。

- **4. 新增：支持 Bitmap 索引**
  - **进度:** 0%
  - **状态:** ❌ 待办
  - **任务:** 实现对 StarRocks Bitmap 索引的完整支持。
  - **细节:** 这涉及到对 **模型定义**、**编译器**、**反射** 和 **对比** 流程的全面增强，是 V1.0 剩余最重要的功能点。
  - **验收标准:** `autogenerate` 可以正确生成 `op.create_index` 和 `op.drop_index`，并带上 `starrocks_using='BITMAP'` 参数。

### 第四阶段：测试与文档 (Testing & Documentation)

- **1. 编写集成测试**

  - **进度:** 70%
  - **状态:** 💹 已推进
  - **任务:** 在 `test/` 目录下，为 Alembic `autogenerate` 编写端到端的集成测试。
  - **细节:** 测试应覆盖完整的 `revision --autogenerate`, `upgrade`, `downgrade` 流程。验证脚本生成、在线数据库执行、离线 SQL 生成的正确性。
  - **验收标准:** 自动化测试能验证 V1.0 所有核心功能的完整生命周期（创建、修改、删除、升级、降级）。
  - **子任务清单:**
    - ✅ **已有覆盖 (Existing Coverage):**
      - **DDL 编译 (`test/sql/`)**: 已覆盖 `Table`, `View`, `MV` 的 `CREATE`/`ALTER` 语句生成。
      - **数据库反射 (`test/integration/test_reflection_*.py`)**: 已覆盖对 `Table`, `Column` 属性以及聚合类型的反射。
      - **Alembic `autogenerate` (`test_autogenerate_*.py`)**: 已覆盖对 `Table`, `Column`, `View` 属性变更的检测，`MV` 仅覆盖基础场景。
      - **Alembic 代码渲染 (`test/test_render.py`)**: 已通过单元测试覆盖 `View`, `MV`, `Table` 的 `Op` 对象到 Python 代码的渲染。
    - ❌ **待办: 功能完整性测试 (Functional Completeness Tests)**:
      - **`Materialized View`**: 需要完整的生命周期测试，覆盖 `CREATE`/`ALTER` (`RENAME`, `REFRESH`, `PROPERTIES`, `STATUS`)/`DROP`，并确保 `upgrade` 和 `downgrade` 均能正确执行。
      - **`Bitmap Index`**: 需要完整的生命周期测试 (`CREATE`/`DROP`)，确保 `upgrade` 和 `downgrade` 均能正确执行，并覆盖反射和 `autogenerate` 对比。
      - **`AUTO_INCREMENT`**: 需要完善反射支持 (当前测试为 `xfail`) 和 `autogenerate` 变更支持 (当前仅有告警)。
    - ❌ **待办: 核心场景测试 (Core Scenario Tests)**:
      - **`downgrade` 可逆性**: 确保每个 `upgrade` 操作都有一个对应的、功能正确的 `downgrade` 操作，执行后能将数据库恢复到迁移前状态。
      - **`autogenerate` 默认值处理**: 测试当模型中未指定某些属性（如 `ENGINE`），而数据库中存在系统默认值时，`autogenerate` 不会生成不必要的变更。
      - **离线 SQL 生成 (`--sql` 模式)**: 验证 `alembic upgrade --sql` 能为所有操作生成语法正确的 DDL 脚本，并与预期快照进行比对。
      - **幂等性测试**: 多次对同一个数据库版本执行 `autogenerate`，应始终生成空的迁移脚本。
      - **复杂类型支持**: 增加对 `ARRAY`, `JSON`, `MAP` 等复杂数据类型 `ALTER` 场景的测试。
      - **组合操作**: 测试在单个迁移脚本中包含多种混合操作（如同时创建表、修改视图）的场景。

- **2. 编写用户文档**
  - **进度:** 10%
  - **状态:** ❌ 待办
  - **任务:** 在 `README.md` 或 `docs/` 目录下，提供详细的用户指南。
  - **细节:** 包含 ORM/Core 示例、Alembic 配置、完整工作流示例和限制说明。
  - **验收标准:** 一个新用户根据文档可以独立完成整个 Schema 迁移过程。
  - **子任务清单:**
    - ❌ **待办:** 撰写安装、配置和快速上手指南。
    - ❌ **待办:** 提供包含所有核心功能的端到端示例 (`Table` with all properties, `View`, `MV`, `Bitmap Index`)。
    - ❌ **待办:** 详细记录所有 `starrocks_*` 参数的用法和含义。
    - ❌ **待办:** 明确列出已知限制（如不支持 DDL 事务）和手动操作建议。

_(图例: ✅ = 已完成, 💹 = 已推进/核心完成, ❌ = 待办)_
