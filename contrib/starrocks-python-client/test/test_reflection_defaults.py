import pytest

from starrocks.reflection import ReflectionViewDefaults, ReflectionViewInfo


def test_reflection_view_defaults_apply_basic():
    info: ReflectionViewInfo = ReflectionViewDefaults.apply(
        name="v1",
        definition="SELECT 1",
        comment=None,
        security=None,
    )
    assert info.name == "v1"
    assert info.definition == "SELECT 1"
    assert info.comment == ""
    assert info.security == ""


def test_reflection_view_defaults_apply_upper_security_and_keep_comment():
    info: ReflectionViewInfo = ReflectionViewDefaults.apply(
        name="v2",
        definition="SELECT 2",
        comment="some comment",
        security="invoker",
    )
    expected_comment = "some comment"
    expected_security = "INVOKER"
    assert info.comment == expected_comment
    assert info.security == expected_security


