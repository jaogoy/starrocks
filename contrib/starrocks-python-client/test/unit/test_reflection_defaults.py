import pytest

from starrocks.engine.interfaces import ReflectedViewState
from starrocks.defaults import ReflectionViewDefaults
from starrocks.types import ViewSecurityType


def test_reflection_view_defaults_apply_basic():
    info: ReflectedViewState = ReflectionViewDefaults.apply(
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
    info: ReflectedViewState = ReflectionViewDefaults.apply(
        name="v2",
        definition="SELECT 2",
        comment="some comment",
        security="invoker",
    )
    assert info.comment == "some comment"
    assert info.security == ViewSecurityType.INVOKER


def test_reflection_view_defaults_empty_strings():
    info: ReflectedViewState = ReflectionViewDefaults.apply(
        name="v3",
        definition="SELECT 3",
        comment="",
        security="",
    )
    assert info.comment == ""
    assert info.security == ""


