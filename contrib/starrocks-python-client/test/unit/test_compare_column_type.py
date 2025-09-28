import pytest
from sqlalchemy import Column

from starrocks.datatype import BIGINT, INTEGER, VARCHAR, DECIMAL, TINYINT, BOOLEAN, STRING, ARRAY, MAP, STRUCT
from starrocks.alembic.starrocks import StarrocksImpl
from starrocks.dialect import StarRocksDialect


def run_compare(type1, type2):
    """
    Helper function to compare two types using StarrocksImpl.compare_type.
    The method under test is `StarrocksImpl.compare_type`.

    Args:
        type1: The SQLAlchemy type object for the inspector column.
        type2: The SQLAlchemy type object for the metadata column.

    Returns:
        True if types are considered different, False otherwise.
    """
    inspector_column = Column("test_col", type1)
    metadata_column = Column("test_col", type2)

    dialect = StarRocksDialect()
    impl = StarrocksImpl(
        dialect=dialect,
        connection=None,
        as_sql=False,
        transactional_ddl=False,
        output_buffer=None,
        context_opts={},
    )

    # In the implementation, metadata_column is the source of truth,
    # and inspector_column is from the database.
    return impl.compare_type(inspector_column, metadata_column)


# 1. Simple type comparison
simple_type_params = [
    # 1.1 Completely identical
    pytest.param(INTEGER(), INTEGER(), False, id="INTEGER vs INTEGER (same)"),
    pytest.param(VARCHAR(10), VARCHAR(10), False, id="VARCHAR(10) vs VARCHAR(10) (same)"),
    pytest.param(DECIMAL(10, 2), DECIMAL(10, 2), False, id="DECIMAL(10, 2) vs DECIMAL(10, 2) (same)"),
    # 1.2 Equivalent types (special rules)
    pytest.param(TINYINT(1), BOOLEAN(), False, id="TINYINT(1) vs BOOLEAN (equivalent)"),
    pytest.param(BOOLEAN(), TINYINT(1), False, id="BOOLEAN vs TINYINT(1) (equivalent)"),
    pytest.param(VARCHAR(65533), STRING(), False, id="VARCHAR(65533) vs STRING (equivalent)"),
    pytest.param(STRING(), VARCHAR(65533), False, id="STRING vs VARCHAR(65533) (equivalent)"),
    # 1.3 Different types
    pytest.param(INTEGER(), STRING(), True, id="INTEGER vs STRING (different)"),
    pytest.param(VARCHAR(10), VARCHAR(20), True, id="VARCHAR(10) vs VARCHAR(20) (different)"),
    pytest.param(DECIMAL(10, 2), DECIMAL(12, 4), True, id="DECIMAL(10,2) vs DECIMAL(12,4) (different)"),
    pytest.param(INTEGER(), BIGINT(), True, id="INTEGER vs BIGINT (different)"),
]

# 2. Complex type vs Simple type
complex_vs_simple_params = [
    pytest.param(INTEGER(), ARRAY(INTEGER), True, id="simple INTEGER vs complex ARRAY"),
    pytest.param(ARRAY(INTEGER), INTEGER(), True, id="complex ARRAY vs simple INTEGER"),
    pytest.param(MAP(STRING, INTEGER), STRING(), True, id="complex MAP vs simple STRING"),
    pytest.param(STRUCT(a=INTEGER), INTEGER(), True, id="complex STRUCT vs simple INTEGER"),
]

# 3. Different complex types
different_complex_types_params = [
    pytest.param(ARRAY(INTEGER), MAP(INTEGER, INTEGER), True, id="ARRAY vs MAP"),
    pytest.param(MAP(STRING, INTEGER), STRUCT(a=STRING, b=INTEGER), True, id="MAP vs STRUCT"),
]

# 4. ARRAY type comparison
array_type_params = [
    pytest.param(ARRAY(INTEGER), ARRAY(INTEGER), False, id="ARRAY<INT> vs ARRAY<INT> (same)"),
    pytest.param(ARRAY(INTEGER), ARRAY(STRING), True, id="ARRAY<INT> vs ARRAY<STR> (different item type)"),
    pytest.param(ARRAY(ARRAY(INTEGER)), ARRAY(ARRAY(INTEGER)), False, id="Nested ARRAY (same)"),
    pytest.param(ARRAY(ARRAY(INTEGER)), ARRAY(ARRAY(STRING)), True, id="Nested ARRAY (different item type)"),
    pytest.param(ARRAY(MAP(STRING, INTEGER)), ARRAY(MAP(STRING, STRING)), True, id="Nested ARRAY with MAP (deep difference)"),
]

# 5. MAP type comparison
map_type_params = [
    pytest.param(MAP(STRING, INTEGER), MAP(STRING, INTEGER), False, id="MAP<STR,INT> vs MAP<STR,INT> (same)"),
    pytest.param(MAP(INTEGER, STRING), MAP(INTEGER, STRING), False, id="MAP<INT,STR> vs MAP<INT,STR> (same)"),
    pytest.param(MAP(STRING, INTEGER), MAP(INTEGER, INTEGER), True, id="MAP (different key type: STR vs INT)"),
    pytest.param(MAP(INTEGER, STRING), MAP(STRING, STRING), True, id="MAP (different key type: INT vs STR)"),
    pytest.param(MAP(STRING, INTEGER), MAP(STRING, STRING), True, id="MAP (different value type)"),
    pytest.param(MAP(STRING, ARRAY(INTEGER)), MAP(STRING, ARRAY(INTEGER)), False, id="Nested MAP with ARRAY (same)"),
    pytest.param(MAP(STRING, ARRAY(INTEGER)), MAP(STRING, ARRAY(STRING)), True, id="Nested MAP with ARRAY (deep difference)"),
    pytest.param(MAP(STRING, STRUCT(a=INTEGER)), MAP(STRING, STRUCT(a=STRING)), True, id="Nested MAP with STRUCT (deep difference)"),
]

# 6. STRUCT type comparison
struct_type_params = [
    pytest.param(STRUCT(a=INTEGER, b=STRING), STRUCT(a=INTEGER, b=STRING), False, id="STRUCT (same)"),
    pytest.param(STRUCT(a=INTEGER, b=STRING), STRUCT(b=STRING, a=INTEGER), True, id="STRUCT (different field order)"),
    pytest.param(STRUCT(a=INTEGER), STRUCT(a=INTEGER, b=STRING), True, id="STRUCT (different field count)"),
    pytest.param(STRUCT(a=INTEGER), STRUCT(c=INTEGER), True, id="STRUCT (different field name)"),
    pytest.param(STRUCT(a=INTEGER), STRUCT(a=STRING), True, id="STRUCT (different field type)"),
    pytest.param(STRUCT(a=INTEGER, b=MAP(STRING, INTEGER)), STRUCT(a=INTEGER, b=MAP(STRING, INTEGER)), False, id="Nested STRUCT with MAP (same)"),
    pytest.param(STRUCT(a=INTEGER, b=MAP(STRING, INTEGER)), STRUCT(a=INTEGER, b=MAP(STRING, STRING)), True, id="Nested STRUCT with MAP (deep difference)"),
]

# 7. Complex nested types (ARRAY, MAP, STRUCT combined)
complex_nested_params = [
    pytest.param(
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(STRING)))), 
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(STRING)))), 
        False, 
        id="ARRAY<MAP<STR,STRUCT<INT,ARRAY<STR>>>> (same)"
    ),
    pytest.param(
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(STRING)))), 
        ARRAY(MAP(STRING, STRUCT(a=INTEGER, b=ARRAY(INTEGER)))), 
        True, 
        id="ARRAY<MAP<STR,STRUCT<INT,ARRAY<STR>>>> vs ARRAY<MAP<STR,STRUCT<INT,ARRAY<INT>>>> (deep difference)"
    ),
    pytest.param(
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(STRING, INTEGER)))), 
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(STRING, INTEGER)))), 
        False, 
        id="MAP<INT,ARRAY<STRUCT<STR,MAP<STR,INT>>>> (same)"
    ),
    pytest.param(
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(STRING, INTEGER)))), 
        MAP(INTEGER, ARRAY(STRUCT(a=STRING, b=MAP(INTEGER, INTEGER)))), 
        True, 
        id="MAP<INT,ARRAY<STRUCT<STR,MAP<STR,INT>>>> vs MAP<INT,ARRAY<STRUCT<STR,MAP<INT,INT>>>> (deep difference)"
    ),
    pytest.param(
        STRUCT(
            id=INTEGER, 
            tags=ARRAY(STRING), 
            metadata=MAP(STRING, STRUCT(value=STRING, count=INTEGER))
        ), 
        STRUCT(
            id=INTEGER, 
            tags=ARRAY(STRING), 
            metadata=MAP(STRING, STRUCT(value=STRING, count=INTEGER))
        ), 
        False, 
        id="STRUCT<INT,ARRAY<STR>,MAP<STR,STRUCT<STR,INT>>> (same)"
    ),
    pytest.param(
        STRUCT(
            id=INTEGER, 
            tags=ARRAY(STRING), 
            metadata=MAP(STRING, STRUCT(value=STRING, count=INTEGER))
        ), 
        STRUCT(
            id=INTEGER, 
            tags=ARRAY(INTEGER), 
            metadata=MAP(STRING, STRUCT(value=STRING, count=INTEGER))
        ), 
        True, 
        id="STRUCT<INT,ARRAY<STR>,MAP<STR,STRUCT<STR,INT>>> vs STRUCT<INT,ARRAY<INT>,MAP<STR,STRUCT<STR,INT>>> (deep difference)"
    ),
]


class TestCompareColumnType:
    """Test suite for StarrocksImpl.compare_type."""

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", simple_type_params)
    def test_simple_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of simple (non-nested) data types."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", complex_vs_simple_params)
    def test_complex_vs_simple_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison between complex and simple types."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", different_complex_types_params)
    def test_different_complex_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison between different kinds of complex types (e.g., ARRAY vs MAP)."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", array_type_params)
    def test_array_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of ARRAY types, including nested scenarios."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", map_type_params)
    def test_map_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of MAP types, including nested scenarios."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", struct_type_params)
    def test_struct_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of STRUCT types, including nested scenarios."""
        assert run_compare(inspector_type, metadata_type) == is_different

    @pytest.mark.parametrize("inspector_type, metadata_type, is_different", complex_nested_params)
    def test_complex_nested_types(self, inspector_type, metadata_type, is_different):
        """Tests comparison of deeply nested complex types combining ARRAY, MAP, and STRUCT."""
        assert run_compare(inspector_type, metadata_type) == is_different
