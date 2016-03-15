"""init db

Revision ID: 6d9fa02d7936
Revises: 
Create Date: 2016-03-14 11:20:41.940586

"""

# revision identifiers, used by Alembic.
revision = '6d9fa02d7936'
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.execute("""
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  max_quota integer DEFAULT 16777216,
  download_traffic bigint DEFAULT 0,
  size bigint DEFAULT 0
);
CREATE TABLE IF NOT EXISTS prefixes (
  name VARCHAR(36) PRIMARY KEY,
  user_id INTEGER NOT NULL
);
SELECT to_regclass('prefixes.prefix_idx');

DO $$
BEGIN

IF NOT EXISTS (
    SELECT to_regclass('prefixes.prefix_idx')
    ) THEN

    CREATE INDEX prefix_idx ON prefixes (user_id);
END IF;

END$$;
    """)


def downgrade():
    op.drop_table('users')
    op.drop_index('prefix_idx')
    op.drop_table('prefixes')
