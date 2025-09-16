# My prefered Code Style

## Python

总体需要遵守 PEP 8、PEP 257、PEP 484 规范，并参考 Flake8, Ruff 规范。

在此基础上，增加参考 Google Python Style Guide，其中有几部分对前面的规范进行了修改：

- 最大行长度（从 79 改为 120）
- docstring 风格（Google 风格更清晰）
- 强制类型注解

我比较重点关心的几个部分：

- **Type Annotations**: All functions, methods, and complex variable declarations should have explicit type annotations. Historical code should also be gradually annotated.
- **Docstrings**: Write clear, Google-style docstrings for all modules, classes, and functions, including arguments and returns.

如下是更多一些自己的代码规范要求。

### 相关字符串常量请提取为类常量

- 目的：提升可读性与可维护性，避免硬编码、拼写错误与含义混淆。
- 做法：为同一语义类别建立类（如 `TableOptions`、`FileFormats`），用 UPPER_SNAKE_CASE 类属性集中管理；使用处统一通过 `ClassName.CONSTANT` 引用，避免直接写字面量。
- 类型：优先为常量加类型注解（如 `Final[str]`），在函数签名可配合 `Literal[...]` 或 `Enum` 增强约束。

示例：

```python
from typing import Final, Literal


class TableOptions:
    ENGINE: Final[str] = "engine"
    KEY: Final[str] = "key"
    COMMENT: Final[str] = "comment"
    PARTITION: Final[str] = "partition"
    DISTRIBUTION: Final[str] = "distribution"
    ORDER_BY: Final[str] = "order by"
    PROPERTIES: Final[str] = "properties"


OptionName = Literal[
    TableOptions.ENGINE,
    TableOptions.KEY,
    TableOptions.COMMENT,
    TableOptions.PARTITION,
    TableOptions.DISTRIBUTION,
    TableOptions.ORDER_BY,
    TableOptions.PROPERTIES,
]


def set_option(name: OptionName, value: str) -> None:
    ...


set_option(TableOptions.ENGINE, "olap")
```

补充：

- 需要更强的枚举语义时可使用 `Enum`（如 `class FileFormat(str, Enum): ...`）。
- 需要集合校验可在类外定义只读集合（如 `frozenset({...})`），字符串来源仍以类常量为准。

## Java

## C++

## shell

## Markdown

Use **Markdownlint**: Follow the project's Markdownlint standards for all Markdown files.