# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from unittest.mock import Mock

from alembic.autogenerate.api import AutogenContext

from starrocks.alembic.ops import (
    AlterViewOp,
    CreateViewOp,
    DropViewOp,
)
from starrocks.alembic.render import (
    _alter_view,
    _create_view,
    _drop_view,
)


logger = logging.getLogger(__name__)


def normalize_whitespace(s):
    """Normalize whitespace for comparison (collapse multiple spaces/newlines to single space)."""
    str = ' '.join(s.split())\
        .replace(' , ', ', ').replace(' ,', ',') \
        .replace(' (', '(').replace(' )', ')') \
        .replace(' {', '{').replace(' }', '}') \
        .replace(' [', '[').replace(' ]', ']')
    return str


class TestViewRendering:
    def setup_method(self, method):
        self.ctx = Mock(spec=AutogenContext)

    def test_render_create_view_basic(self):
        """Simple: Basic CREATE VIEW rendering."""
        op = CreateViewOp("v1", "SELECT 1", schema=None, comment=None, security=None)
        rendered = _create_view(self.ctx, op)
        expected = "op.create_view('v1', 'SELECT 1')"
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_create_view_with_schema(self):
        """Coverage: CREATE VIEW with schema attribute."""
        op = CreateViewOp("v1", "SELECT 1", schema="myschema")
        rendered = _create_view(self.ctx, op)
        expected = "op.create_view('v1', 'SELECT 1', schema='myschema')"
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_create_view_with_comment(self):
        """Coverage: CREATE VIEW with comment attribute."""
        op = CreateViewOp("v1", "SELECT 1", comment="Test view")
        rendered = _create_view(self.ctx, op)
        expected = "op.create_view('v1', 'SELECT 1', comment='Test view')"
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_create_view_with_security(self):
        """Coverage: CREATE VIEW with security attribute (DEFINER/INVOKER)."""
        # Test INVOKER
        op = CreateViewOp("v1", "SELECT 1", security="INVOKER")
        rendered = _create_view(self.ctx, op)
        expected = "op.create_view('v1', 'SELECT 1', security='INVOKER')"
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

        # Test DEFINER
        op2 = CreateViewOp("v2", "SELECT 2", security="DEFINER")
        rendered2 = _create_view(self.ctx, op2)
        expected2 = "op.create_view('v2', 'SELECT 2', security='DEFINER')"
        assert normalize_whitespace(rendered2) == normalize_whitespace(expected2)

    def test_render_create_view_with_columns(self):
        """Coverage: CREATE VIEW with columns attribute."""
        op = CreateViewOp(
            "v1",
            "SELECT id, name FROM users",
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'},
                {'name': 'email', 'comment': 'Email address'}
            ]
        )
        rendered = _create_view(self.ctx, op)
        expected = """
        op.create_view('v1', 'SELECT id, name FROM users', columns=[
            {'name': 'id'},
            {'name': 'name', 'comment': 'User name'},
            {'name': 'email', 'comment': 'Email address'}
        ])
        """
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_create_view_complex(self):
        """Complex: CREATE VIEW with all attributes combined."""
        op = CreateViewOp(
            "v1",
            "SELECT id, name FROM users",
            schema="myschema",
            comment="User view",
            security="INVOKER",
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ]
        )
        rendered = _create_view(self.ctx, op)
        expected = """
        op.create_view('v1', 'SELECT id, name FROM users',
            schema='myschema',
            comment='User view',
            security='INVOKER',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ])
        """
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_drop_view_basic(self):
        """Simple: Basic DROP VIEW rendering."""
        op = DropViewOp("v1", schema=None, if_exists=False)
        rendered = _drop_view(self.ctx, op)
        assert rendered == "op.drop_view('v1')"

    def test_render_drop_view_with_schema(self):
        """Coverage: DROP VIEW with schema attribute."""
        op = DropViewOp("v1", schema="myschema", if_exists=False)
        rendered = _drop_view(self.ctx, op)
        expected = "op.drop_view('v1', schema='myschema')"
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_drop_view_with_if_exists(self):
        """Coverage: DROP VIEW with if_exists attribute."""
        op = DropViewOp("v1", schema=None, if_exists=True)
        rendered = _drop_view(self.ctx, op)
        expected = "op.drop_view('v1', if_exists=True)"
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_drop_view_complex(self):
        """Complex: DROP VIEW with all attributes combined."""
        op = DropViewOp("v1", schema="myschema", if_exists=True)
        rendered = _drop_view(self.ctx, op)
        expected = "op.drop_view('v1', schema='myschema', if_exists=True)"
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_alter_view_basic(self):
        """Simple: Basic ALTER VIEW rendering (definition only)."""
        op = AlterViewOp("v1", "SELECT 2", schema=None, comment=None, security=None)
        rendered = _alter_view(self.ctx, op)
        expected = "op.alter_view('v1', 'SELECT 2')"
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_alter_view_with_comment(self):
        """Coverage: ALTER VIEW with comment attribute."""
        op = AlterViewOp("v1", definition=None, comment="Modified comment")
        rendered = _alter_view(self.ctx, op)
        expected = "op.alter_view('v1', comment='Modified comment')"
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_alter_view_with_security(self):
        """Coverage: ALTER VIEW with security attribute."""
        op = AlterViewOp("v1", definition=None, security="DEFINER")
        rendered = _alter_view(self.ctx, op)
        expected = "op.alter_view('v1', security='DEFINER')"
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_alter_view_partial_attrs(self):
        """Coverage: ALTER VIEW with multiple but not all attributes (key scenario)."""
        op = AlterViewOp(
            "v1",
            definition=None,  # Definition is NOT changed
            schema="myschema",
            comment="Modified comment",
            security=None  # Security is NOT changed
        )
        rendered = _alter_view(self.ctx, op)
        expected = "op.alter_view('v1', schema='myschema', comment='Modified comment')"
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_alter_view_complex(self):
        """Complex: ALTER VIEW with all attributes combined."""
        op = AlterViewOp(
            "v1",
            "SELECT id, name FROM users",
            schema="myschema",
            comment="Modified view",
            security="DEFINER"
        )
        rendered = _alter_view(self.ctx, op)
        expected = """
        op.alter_view('v1', 'SELECT id, name FROM users',
            schema='myschema',
            comment='Modified view',
            security='DEFINER')
        """
        assert normalize_whitespace(rendered) == normalize_whitespace(expected)

    def test_render_view_with_special_chars(self):
        """Complex: Rendering view with special characters in definition and schema."""
        op = CreateViewOp(
            "v_complex",
            "SELECT `user-id`, 'some_string', \"another_string\\n\" FROM `my_table`",
            schema="s'1"
        )
        rendered = _create_view(self.ctx, op)
        # Verify key elements are present (exact escaping handled by Python's repr())
        assert "op.create_view('v_complex'" in rendered
        assert "SELECT" in rendered
        assert "user-id" in rendered
        assert "schema=" in rendered

    def test_create_view_reverse(self):
        """Reverse: CreateViewOp reverses to DropViewOp."""
        create_op = CreateViewOp("v1", "SELECT 1", schema="s1")
        reverse_op = create_op.reverse()
        assert isinstance(reverse_op, DropViewOp)
        assert reverse_op.view_name == "v1"
        assert reverse_op.schema == "s1"

    def test_alter_view_reverse(self):
        """Reverse: AlterViewOp reverses to another AlterViewOp with swapped attributes."""
        alter_op = AlterViewOp(
            "v1",
            "SELECT 2",
            schema="s1",
            comment="New Comment",
            security="DEFINER",
            reverse_view_definition="SELECT 1",
            reverse_view_comment="Old Comment",
            reverse_view_security="INVOKER",
        )
        reverse_op = alter_op.reverse()
        assert isinstance(reverse_op, AlterViewOp)
        assert reverse_op.view_name == "v1"
        assert reverse_op.schema == "s1"
        # The new attributes of the reverse op should be the old attributes of the original op
        assert reverse_op.definition == "SELECT 1"
        assert reverse_op.comment == "Old Comment"
        assert reverse_op.security == "INVOKER"
        # And the reverse attributes of the reverse op should be the new attributes of the original op
        assert reverse_op.reverse_view_definition == "SELECT 2"
        assert reverse_op.reverse_view_comment == "New Comment"
        assert reverse_op.reverse_view_security == "DEFINER"

    def test_drop_view_reverse(self):
        """Reverse: DropViewOp reverses to CreateViewOp (with and without columns)."""
        # Test without columns
        drop_op = DropViewOp(
            "v1",
            schema=None,
            _reverse_view_definition="SELECT 1",
            _reverse_view_comment="Test view",
            _reverse_view_security="INVOKER"
        )
        reverse_op = drop_op.reverse()
        assert isinstance(reverse_op, CreateViewOp)
        assert reverse_op.view_name == "v1"
        assert reverse_op.definition == "SELECT 1"
        assert reverse_op.comment == "Test view"
        assert reverse_op.security == "INVOKER"

        # Test with columns
        drop_op_with_cols = DropViewOp(
            "v2",
            schema=None,
            _reverse_view_definition="SELECT id, name FROM users",
            _reverse_view_comment="User view",
            _reverse_view_security="INVOKER",
            _reverse_view_columns=[
                {'name': 'id', 'comment': None},
                {'name': 'name', 'comment': 'User name'}
            ]
        )
        reverse_op_with_cols = drop_op_with_cols.reverse()
        assert isinstance(reverse_op_with_cols, CreateViewOp)
        assert reverse_op_with_cols.view_name == "v2"
        assert reverse_op_with_cols.definition == "SELECT id, name FROM users"
        assert reverse_op_with_cols.columns is not None
        assert len(reverse_op_with_cols.columns) == 2
        assert reverse_op_with_cols.columns[0]['name'] == 'id'
        assert reverse_op_with_cols.columns[1]['name'] == 'name'
        assert reverse_op_with_cols.columns[1]['comment'] == 'User name'
