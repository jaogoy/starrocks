import logging
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context
from sqlalchemy.types import TypeEngine

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = context.config.attributes.get("target_metadata", None)

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


logger = logging.getLogger(__name__)

def my_render_item(type_, obj, autogen_context):
    """
    自定义渲染函数。
    """
    # 检查我们正在渲染的对象是否是我们自定义模块中的类型
    # logger.debug(f"rendering item: {obj}, type: {type_}, module: {obj.__class__.__module__}")
    if isinstance(obj, TypeEngine) and obj.__class__.__module__.startswith('starrocks.datatype'):
        # 添加我们需要的导入
        autogen_context.imports.add("import starrocks.datatype as sr")
        # 返回我们想要的字符串表示形式
        # obj.__class__.__name__ 会得到 'INTEGER', 'VARCHAR' 等
        # repr(obj) 会包含参数，例如 'VARCHAR(255)'
        return f"sr.{repr(obj)}"

    # 对于其他所有类型的对象，返回 False，让 Alembic 使用默认的渲染逻辑
    return False


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        user_module_prefix={'starrocks.datatype.': 'sr.'},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # This is for the test suite, where we want to control the version table's
    # creation explicitly.
    version_table_kwargs = {
        "starrocks_properties": {"replication_num": "1"},
    }

    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_kwargs=version_table_kwargs,
            # user_module_prefix={'starrocks.datatype.': 'sr.'},
            render_item=my_render_item,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
