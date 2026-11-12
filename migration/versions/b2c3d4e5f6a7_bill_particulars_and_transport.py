"""bill_particulars_and_transport

Revision ID: b2c3d4e5f6a7
Revises: 17f3455045ca
Create Date: 2026-11-11 20:45:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, Sequence[str], None] = '17f3455045ca'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Safely extend raw_sales with missing bill-header fields
    sales_columns = [
        sa.Column('bill_amount', sa.Numeric(), nullable=True),
        sa.Column('dis', sa.Numeric(), nullable=True),
        sa.Column('packing_case', sa.Numeric(), nullable=True),
        sa.Column('packing_amount', sa.Numeric(), nullable=True),
        sa.Column('transport_name', sa.Text(), nullable=True),
        sa.Column('transport_amount', sa.Numeric(), nullable=True),
        sa.Column('total_packs', sa.Numeric(), nullable=True),
        sa.Column('bdl', sa.Numeric(), nullable=True),
        sa.Column('dispatch_case', sa.Integer(), nullable=True),
        sa.Column('dispatch_bdl', sa.Integer(), nullable=True),
        sa.Column('fright', sa.Numeric(), nullable=True),
        sa.Column('gross_amt5', sa.Numeric(), nullable=True),
        sa.Column('gross_amt12', sa.Numeric(), nullable=True),
        sa.Column('total_tax5', sa.Numeric(), nullable=True),
        sa.Column('total_tax12', sa.Numeric(), nullable=True),
        sa.Column('is_gst', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('gr_no', sa.Text(), nullable=True),
        sa.Column('is_cash', sa.Text(), nullable=True),
        sa.Column('customer_name', sa.Text(), nullable=True),
        sa.Column('city_id', sa.Text(), nullable=True),
        sa.Column('note', sa.Text(), nullable=True),
        sa.Column('user_name', sa.Text(), nullable=True),
        sa.Column('add_date', sa.Text(), nullable=True),
        sa.Column('bill_group', sa.Text(), nullable=True),
        sa.Column('ref_no', sa.Numeric(), nullable=True),
        sa.Column('company_id', sa.Text(), nullable=True),
        sa.Column('session_id', sa.Text(), nullable=True),
        sa.Column('is_deleted', sa.Boolean(), server_default=sa.text('false'), nullable=False),
    ]
    
    # Check existing columns in raw_sales before adding
    bind = op.get_bind()
    insp = sa.inspect(bind)
    existing_cols = {c['name'] for c in insp.get_columns('raw_sales')}
    
    for col in sales_columns:
        if col.name not in existing_cols:
            op.add_column('raw_sales', col)

    # 2. Add raw_bill_details
    if not insp.has_table('raw_bill_details'):
        op.create_table(
            'raw_bill_details',
            sa.Column('raw_id', postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column('checksum', sa.Text(), nullable=True),
            sa.Column('source_system', sa.Text(), nullable=False),
            sa.Column('is_processed', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'), nullable=False),
            sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('processing_batch_id', sa.Text(), nullable=True),
            sa.Column('processing_attempts', sa.Integer(), server_default=sa.text('0'), nullable=False),
            sa.Column('last_processing_error', sa.Text(), nullable=True),
            sa.Column('bill_id', sa.Text(), nullable=False),
            sa.Column('line_idx', sa.Integer(), nullable=False),
            sa.Column('item_id', sa.Text(), nullable=True),
            sa.Column('item_name', sa.Text(), nullable=True),
            sa.Column('size_id', sa.Text(), nullable=True),
            sa.Column('size', sa.Integer(), nullable=True),
            sa.Column('qty', sa.Numeric(), nullable=True),
            sa.Column('rate', sa.Numeric(), nullable=True),
            sa.Column('scheme', sa.Text(), nullable=True),
            sa.Column('amount', sa.Numeric(), nullable=True),
            sa.Column('sr', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('mtsr', sa.Numeric(), nullable=True),
            sa.Column('company_id', sa.Text(), nullable=True),
            sa.Column('group_name', sa.Text(), nullable=True),
            sa.Column('stock_id', sa.Integer(), nullable=True),
            sa.Column('unit', sa.Text(), nullable=True),
            sa.Column('netrate', sa.Numeric(), nullable=True),
            sa.Column('taxcode', sa.Numeric(), nullable=True),
            sa.Column('discount', sa.Numeric(), nullable=True),
            sa.Column('attend_by', sa.Text(), nullable=True),
            sa.Column('inc_per', sa.Numeric(), nullable=True),
            sa.Column('bill_type', sa.Integer(), nullable=True),
            sa.Column('hbr', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('is_deleted', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.PrimaryKeyConstraint('raw_id'),
            sa.UniqueConstraint('bill_id', 'line_idx', name='uq_raw_bill_details_bill_line'),
        )
        op.create_index('idx_raw_bill_details_bill_id', 'raw_bill_details', ['bill_id'], unique=False)
        op.create_index('idx_raw_bill_details_item_id', 'raw_bill_details', ['item_id'], unique=False)
        op.create_index('idx_raw_bill_details_is_processed', 'raw_bill_details', ['is_processed'], unique=False)

    # 3. Add raw_lr_master
    if not insp.has_table('raw_lr_master'):
        op.create_table(
            'raw_lr_master',
            sa.Column('raw_id', postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column('checksum', sa.Text(), nullable=True),
            sa.Column('source_system', sa.Text(), nullable=False),
            sa.Column('is_processed', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'), nullable=False),
            sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('processing_batch_id', sa.Text(), nullable=True),
            sa.Column('processing_attempts', sa.Integer(), server_default=sa.text('0'), nullable=False),
            sa.Column('last_processing_error', sa.Text(), nullable=True),
            sa.Column('lr_id', sa.Text(), nullable=False),
            sa.Column('entry_idx', sa.Integer(), server_default=sa.text('1'), nullable=False),
            sa.Column('bill_id', sa.Text(), nullable=True),
            sa.Column('company_id', sa.Text(), nullable=True),
            sa.Column('sno', sa.Integer(), nullable=True),
            sa.Column('customer_id', sa.Text(), nullable=True),
            sa.Column('entry_date', sa.Text(), nullable=True),
            sa.Column('customer_name', sa.Text(), nullable=True),
            sa.Column('city_id', sa.Text(), nullable=True),
            sa.Column('total_packs', sa.Numeric(), nullable=True),
            sa.Column('gr_no', sa.Text(), nullable=True),
            sa.Column('lr_date', sa.Text(), nullable=True),
            sa.Column('transport_name', sa.Text(), nullable=True),
            sa.Column('ps', sa.Text(), nullable=True),
            sa.Column('note', sa.Text(), nullable=True),
            sa.Column('printed', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('is_lock', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('packing_case', sa.Numeric(), nullable=True),
            sa.Column('packing_amount', sa.Numeric(), nullable=True),
            sa.Column('transport_amount', sa.Numeric(), nullable=True),
            sa.Column('is_update', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('user_name', sa.Text(), nullable=True),
            sa.Column('add_date', sa.Text(), nullable=True),
            sa.Column('add_time', sa.Text(), nullable=True),
            sa.Column('is_edit_lock', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('is_ok', sa.Integer(), server_default=sa.text('0'), nullable=False),
            sa.Column('lock_date', sa.Text(), nullable=True),
            sa.Column('bdl', sa.Numeric(), nullable=True),
            sa.Column('bill_group', sa.Text(), nullable=True),
            sa.Column('ref_no', sa.Numeric(), nullable=True),
            sa.Column('is_dispatch', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('clear_remark', sa.Text(), nullable=True),
            sa.Column('lock_user', sa.Text(), nullable=True),
            sa.Column('lock_user_date', sa.Text(), nullable=True),
            sa.Column('checked', sa.Text(), nullable=True),
            sa.Column('is_lr', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('pickup_by', sa.Text(), nullable=True),
            sa.Column('is_bus', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('is_sms_sent', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('auto_freight', sa.Numeric(), nullable=True),
            sa.Column('is_deleted', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.PrimaryKeyConstraint('raw_id'),
            sa.UniqueConstraint('lr_id', 'entry_idx', name='uq_raw_lr_master_lr_entry'),
        )
        op.create_index('idx_raw_lr_master_lr_id', 'raw_lr_master', ['lr_id'], unique=False)
        op.create_index('idx_raw_lr_master_bill_id', 'raw_lr_master', ['bill_id'], unique=False)
        op.create_index('idx_raw_lr_master_customer_id', 'raw_lr_master', ['customer_id'], unique=False)
        op.create_index('idx_raw_lr_master_gr_no', 'raw_lr_master', ['gr_no'], unique=False)
        op.create_index('idx_raw_lr_master_is_processed', 'raw_lr_master', ['is_processed'], unique=False)

    # 4. Add raw_lr_details
    if not insp.has_table('raw_lr_details'):
        op.create_table(
            'raw_lr_details',
            sa.Column('raw_id', postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column('checksum', sa.Text(), nullable=True),
            sa.Column('source_system', sa.Text(), nullable=False),
            sa.Column('is_processed', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'), nullable=False),
            sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('processing_batch_id', sa.Text(), nullable=True),
            sa.Column('processing_attempts', sa.Integer(), server_default=sa.text('0'), nullable=False),
            sa.Column('last_processing_error', sa.Text(), nullable=True),
            sa.Column('lr_id', sa.Text(), nullable=False),
            sa.Column('bill_id', sa.Text(), nullable=False),
            sa.Column('line_idx', sa.Integer(), server_default=sa.text('1'), nullable=False),
            sa.Column('sno', sa.Text(), nullable=True),
            sa.Column('case_count', sa.Integer(), nullable=True),
            sa.Column('bdl', sa.Numeric(), nullable=True),
            sa.Column('dcase', sa.Numeric(), nullable=True),
            sa.Column('dbdl', sa.Integer(), nullable=True),
            sa.Column('rsno', sa.Text(), nullable=True),
            sa.Column('freight', sa.Numeric(), nullable=True),
            sa.Column('is_deleted', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.PrimaryKeyConstraint('raw_id'),
            sa.UniqueConstraint('lr_id', 'bill_id', 'line_idx', name='uq_raw_lr_details_lr_bill_line'),
        )
        op.create_index('idx_raw_lr_details_lr_id', 'raw_lr_details', ['lr_id'], unique=False)
        op.create_index('idx_raw_lr_details_bill_id', 'raw_lr_details', ['bill_id'], unique=False)
        op.create_index('idx_raw_lr_details_is_processed', 'raw_lr_details', ['is_processed'], unique=False)

    # 5. Add raw_delete_records
    if not insp.has_table('raw_delete_records'):
        op.create_table(
            'raw_delete_records',
            sa.Column('raw_id', postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column('checksum', sa.Text(), nullable=True),
            sa.Column('source_system', sa.Text(), nullable=False),
            sa.Column('bill_id', sa.Text(), nullable=False),
            sa.Column('bill_no', sa.Text(), nullable=True),
            sa.Column('bill_date', sa.Text(), nullable=True),
            sa.Column('customer_name', sa.Text(), nullable=True),
            sa.Column('city_name', sa.Text(), nullable=True),
            sa.Column('net_amount', sa.Numeric(), nullable=True),
            sa.Column('user_name', sa.Text(), nullable=True),
            sa.Column('delete_date', sa.Text(), nullable=True),
            sa.Column('delete_time', sa.Text(), nullable=True),
            sa.Column('remark', sa.Text(), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('NOW()'), nullable=False),
            sa.PrimaryKeyConstraint('raw_id'),
            sa.UniqueConstraint('bill_id'),
        )
        op.create_index('idx_raw_delete_records_bill_id', 'raw_delete_records', ['bill_id'], unique=False)


def downgrade() -> None:
    op.drop_table('raw_delete_records')
    op.drop_table('raw_lr_details')
    op.drop_table('raw_lr_master')
    op.drop_table('raw_bill_details')
    
    sales_col_names = [
        'bill_amount', 'dis', 'packing_case', 'packing_amount', 'transport_name',
        'transport_amount', 'total_packs', 'bdl', 'dispatch_case', 'dispatch_bdl',
        'fright', 'gross_amt5', 'gross_amt12', 'total_tax5', 'total_tax12',
        'is_gst', 'gr_no', 'is_cash', 'customer_name', 'city_id', 'note',
        'user_name', 'add_date', 'bill_group', 'ref_no', 'company_id', 'session_id', 'is_deleted'
    ]
    for col_name in sales_col_names:
        op.drop_column('raw_sales', col_name)
