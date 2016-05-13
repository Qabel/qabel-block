"""
Remove local (block-server) quota storage, since the quota is now handled by the accounting server.

Revision ID: 262d44e38959
Revises: 27e9aa8fa794
Create Date: 2016-05-12 16:37:30.413778

"""

# revision identifiers, used by Alembic.
revision = '262d44e38959'
down_revision = '27e9aa8fa794'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.drop_column('users', 'max_quota')


def downgrade():
    op.add_column('users',
                  sa.Column('max_quota', sa.BIGINT, default=2147483648))
    op.execute('UPDATE users SET max_quota = 2147483648')
