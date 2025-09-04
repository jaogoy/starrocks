# Cursor AI Guide for starrocks-python-client

This document provides context and guidance for the AI assistant (Cursor) when collaborating on the `starrocks-python-client` project.

## 1. Project Overview

- **Project Name**: `starrocks-python-client`
- **Core Goal**: To provide a full-featured Python client, especially by implementing a powerful SQLAlchemy Dialect and Alembic integration, enabling Python developers to seamlessly use StarRocks' advanced features.
- **Technology Stack**:
  - **Language**: Python
  - **Core Libraries**: SQLAlchemy, Alembic
  - **Testing**: pytest
- **Key Modules**:
  - `starrocks/dialect.py`: Core implementation of the SQLAlchemy Dialect, responsible for SQL dialect translation, connection management, and execution.
  - `starrocks/reflection.py`: Responsible for database object "reflection," i.e., reading metadata of tables, views, indexes, etc., from a StarRocks database.
  - `starrocks/alembic/`: Specific implementation for Alembic integration, used for handling the autogeneration and execution of database migration scripts.
  - `test/`: Directory for unit and integration tests.

## 2. Current Development Goal

Our current primary goal is to **enhance support for StarRocks-specific features in SQLAlchemy and Alembic**. This includes, but is not limited to:

- **Improve Data Type Support**:
  - Add support for StarRocks-specific data types such as `BITMAP`, `HLL`, and `JSON`.
  - Ensure these types are correctly identified and handled in SQLAlchemy model definitions and Alembic migration scripts.
- **Enhance DDL Support**:
  - Implement full support for StarRocks table attributes (e.g., `engine`, `distribution`, `order by`, `properties`).
  - Enable the Alembic `autogenerate` process to accurately detect and generate change scripts for these attributes.
- **Views and Materialized Views**:
  - Add support for creating, dropping, and reflecting StarRocks Views and Materialized Views.
  - Ensure Alembic can manage view migrations.
- **Improve Compatibility**:
  - Fix bugs and compatibility issues encountered when SQLAlchemy or Alembic interact with StarRocks.

## 3. Coding & Development Workflow

Please follow these standards and procedures when assisting with development:

- **Design First**: Before any coding, please provide a design outline that includes:
  1.  **Implementation Approach**: Describe how you will implement the feature.
  2.  **Code Change Points**: Identify which files and functions need modification.
  3.  **Test Cases**: List the test cases that will be added or modified to verify the functionality.
  [[memory:8065643]]

- **Coding Standards**:
  - **Code Style**: Follow PEP 8, Flake8, and Ruff standards.
  - **Type Annotations**: All functions, methods, and complex variable declarations should have explicit type annotations. Historical code should also be gradually annotated. [[memory:8065598]]
  - **Docstrings**: Write clear, Google-style docstrings for all modules, classes, and functions. In the docstrings of specific test classes (e.g., `TestAlterTableIntegration`), please follow this field order: `engine`, `key`, `comment`, `partition`, `distribution`, `order by`, `properties`. [[memory:8012332]]
  - **Code Modifications**: When refactoring or making changes, strictly limit them to the current task's scope and avoid altering unrelated code. [[memory:8012264]]
  - **Markdownlint**: Follow the project's Markdownlint standards for all Markdown files.

- **Testing Requirements**:
  - **Framework**: Use `pytest`.
  - **Practice**:
    - Every new feature or bug fix must have corresponding test cases, following a "unit tests first, then integration tests" sequence.
  - **Review**: I expect to review the test cases you write or modify before you implement the code to pass them. [[memory:8065598]]
  - **Running Tests**: Use the `pytest test/` command to run all tests.

## 4. Environment & Dependencies

- **Dependency Management**: Project dependencies are managed via `setup.py`.
- **Local Development**: A running StarRocks instance is typically required for integration testing. Connection information is usually configured via environment variables.
