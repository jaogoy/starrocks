import pytest

from starrocks.reflection_info import ReflectionViewInfo
from starrocks.defaults import ReflectionViewDefaults
from starrocks.types import ViewSecurityType


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
    assert info.comment == "some comment"
    assert info.security == ViewSecurityType.INVOKER


def test_reflection_view_defaults_empty_strings():
    info: ReflectionViewInfo = ReflectionViewDefaults.apply(
        name="v3",
        definition="SELECT 3",
        comment="",
        security="",
    )
    assert info.comment == ""
    assert info.security == ""


