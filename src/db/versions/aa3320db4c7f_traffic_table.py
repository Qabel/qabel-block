"""
Create a traffic table for storing monthly traffic.

Revision ID: aa3320db4c7f
Revises: 262d44e38959
Create Date: 2016-05-13 10:37:08.185975

"""

# revision identifiers, used by Alembic.
revision = 'aa3320db4c7f'
down_revision = '262d44e38959'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
        'traffic',
        sa.Column('user_id', sa.INTEGER),
        sa.Column('traffic_month', sa.DATE),
        sa.Column('traffic', sa.BIGINT, default=0),
    )
    op.create_primary_key(
        'pk_traffic', 'traffic',
        ['user_id', 'traffic_month']
    )
    op.create_check_constraint(
        # ensure that only months are stored, not days
        'traffic_month_constraint', 'traffic',
        "date_trunc('month', traffic_month) = traffic_month",
    )


def downgrade():
    op.drop_table('traffic')
