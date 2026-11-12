"""add_is_lock_to_raw_sales

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-11-12 19:35:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if insp.has_table('raw_sales'):
        existing_cols = {c['name'] for c in insp.get_columns('raw_sales')}
        if 'is_lock' not in existing_cols:
            op.add_column('raw_sales', sa.Column('is_lock', sa.Boolean(), server_default=sa.text('false'), nullable=False))


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if insp.has_table('raw_sales'):
        existing_cols = {c['name'] for c in insp.get_columns('raw_sales')}
        if 'is_lock' in existing_cols:
            op.drop_column('raw_sales', 'is_lock')
