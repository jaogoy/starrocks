#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Examples demonstrating how to avoid naming conflicts between views and tables
with StarRocks SQLAlchemy dialect.
"""

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, 
    select, text, event
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from starrocks import ViewMixin, MaterializedViewMixin


# Example 1: 使用命名约定避免冲突
def example_naming_convention():
    """使用命名约定来区分视图和表"""
    
    class Base(DeclarativeBase):
        pass
    
    # 定义表 - 使用普通名称
    class User(Base):
        __tablename__ = "users"
        
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
        email: Mapped[str] = mapped_column(String(100))
    
    # 定义视图 - 使用后缀 _view 来区分
    class UserView(Base, ViewMixin):
        __view_name__ = "users_view"  # 注意：不是 "users"
        __view_comment__ = "用户视图"
        
        # 定义视图中的列
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
        
        @classmethod
        def create_view_definition(cls, metadata: MetaData):
            """Create the view definition"""
            users = metadata.tables["users"]
            
            selectable = select(
                users.c.id,
                users.c.name
            ).where(users.c.id > 0)
            
            return cls.create_view_definition(metadata, selectable)
    
    # 定义物化视图 - 使用后缀 _mv 来区分
    class UserStatsMV(Base, MaterializedViewMixin):
        __view_name__ = "users_mv"  # 注意：不是 "users"
        __view_comment__ = "用户统计物化视图"
        __refresh_strategy__ = "MANUAL"
        
        # 定义物化视图中的列
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
        count: Mapped[int] = mapped_column(Integer)
        
        @classmethod
        def create_view_definition(cls, metadata: MetaData):
            """Create the materialized view definition"""
            users = metadata.tables["users"]
            
            selectable = select(
                users.c.id,
                users.c.name,
                users.c.id.count().label("count")
            ).group_by(users.c.id, users.c.name)
            
            return cls.create_materialized_view_definition(metadata, selectable)
    
    print("命名约定示例:")
    print(f"- 表名: {User.__tablename__}")
    print(f"- 视图名: {UserView.__view_name__}")
    print(f"- 物化视图名: {UserStatsMV.__view_name__}")
    print("这样可以避免命名冲突！")


# Example 2: 使用不同的schema来隔离
def example_schema_isolation():
    """使用不同的schema来隔离视图和表"""
    
    class Base(DeclarativeBase):
        pass
    
    # 定义表 - 在默认schema中
    class User(Base):
        __tablename__ = "users"
        
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
    
    # 定义视图 - 在 views schema 中
    class UserView(Base, ViewMixin):
        __view_name__ = "users"  # 可以使用相同的名称
        __view_schema__ = "views"  # 在 views schema 中
        __view_comment__ = "用户视图"
        
        # 定义视图中的列
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
        
        @classmethod
        def create_view_definition(cls, metadata: MetaData):
            """Create the view definition"""
            users = metadata.tables["users"]
            
            selectable = select(
                users.c.id,
                users.c.name
            ).where(users.c.id > 0)
            
            return cls.create_view_definition(metadata, selectable)
    
    print("Schema隔离示例:")
    print(f"- 表: {User.__tablename__} (默认schema)")
    print(f"- 视图: {UserView.__view_schema__}.{UserView.__view_name__}")
    print("使用不同的schema可以完全避免命名冲突！")


# Example 3: 使用类属性来明确标识
def example_class_attributes():
    """使用类属性来明确标识视图和表"""
    
    class Base(DeclarativeBase):
        pass
    
    # 定义表
    class User(Base):
        __tablename__ = "users"
        
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
    
    # 定义视图
    class UserView(Base, ViewMixin):
        __view_name__ = "user_view"
        __view_comment__ = "用户视图"
        
        # 定义视图中的列
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
    
    print("类属性示例:")
    print(f"- User.__tablename__: {User.__tablename__}")
    print(f"- User.__is_view__: {getattr(User, '__is_view__', False)}")
    print(f"- UserView.__tablename__: {UserView.__tablename__}")
    print(f"- UserView.__is_view__: {UserView.__is_view__}")
    print(f"- UserView.__is_materialized_view__: {UserView.__is_materialized_view__}")
    print("类属性可以明确区分视图和表！")


# Example 4: 解释View和Table的区别
def example_view_vs_table():
    """解释View和Table的区别"""
    
    class Base(DeclarativeBase):
        pass
    
    # 定义表
    class User(Base):
        __tablename__ = "users"
        
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
    
    # 定义视图
    class UserView(Base, ViewMixin):
        __view_name__ = "user_view"
        __view_comment__ = "用户视图"
        
        # 定义视图中的列
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
    
    print("View vs Table 区别:")
    print("1. 表类:")
    print(f"   - __tablename__: {User.__tablename__}")
    print(f"   - 会被SQLAlchemy注册为表: {User.__tablename__ in Base.metadata.tables}")
    print(f"   - 没有 __is_view__ 属性")
    print()
    print("2. 视图类:")
    print(f"   - __tablename__: {UserView.__tablename__}")
    print(f"   - 不会被SQLAlchemy注册为表: {UserView.__view_name__ in Base.metadata.tables}")
    print(f"   - 有 __is_view__ 属性: {UserView.__is_view__}")
    print()
    print("3. 创建对象:")
    print("   - 表类: 创建 Table 对象")
    print("   - 视图类: 创建 View 对象")


# Example 5: 最佳实践建议
def example_best_practices():
    """展示最佳实践"""
    
    print("最佳实践建议:")
    print("1. 使用命名约定:")
    print("   - 表: users, orders, products")
    print("   - 视图: users_view, orders_view, products_view")
    print("   - 物化视图: users_mv, orders_mv, products_mv")
    print()
    print("2. 使用不同的schema:")
    print("   - 表: public.users")
    print("   - 视图: views.users")
    print("   - 物化视图: materialized_views.users")
    print()
    print("3. 使用描述性的名称:")
    print("   - 表: user_profiles")
    print("   - 视图: active_users_view")
    print("   - 物化视图: user_statistics_mv")
    print()
    print("4. 理解View和Table的区别:")
    print("   - View类不会创建Table对象")
    print("   - View类创建View对象")
    print("   - View对象可以生成查询用的Table对象")


if __name__ == "__main__":
    print("视图命名冲突避免示例")
    print("=" * 50)
    print()
    
    example_naming_convention()
    print()
    
    example_schema_isolation()
    print()
    
    example_class_attributes()
    print()
    
    example_view_vs_table()
    print()
    
    example_best_practices() 