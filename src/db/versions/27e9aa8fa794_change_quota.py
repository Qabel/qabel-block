"""change quota

Revision ID: 27e9aa8fa794
Revises: 6d9fa02d7936
Create Date: 2016-03-14 11:43:13.612133

"""

# revision identifiers, used by Alembic.
revision = '27e9aa8fa794'
down_revision = '6d9fa02d7936'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.alter_column('users', 'max_quota', type_=sa.BIGINT)
    op.alter_column('users', 'max_quota', server_default='2147483648')

    op.execute('UPDATE users SET max_quota = 2147483648')


def downgrade():
    op.execute('UPDATE users SET max_quota = 16777216')
    op.alter_column('users', 'max_quota', type_=sa.INTEGER,
                    server_default='16777216')
