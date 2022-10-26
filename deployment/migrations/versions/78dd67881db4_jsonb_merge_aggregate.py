"""jsonb_merge aggregate

Revision ID: 78dd67881db4
Revises: 6c6941a9191f
Create Date: 2022-10-25 16:10:41.360222

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '78dd67881db4'
down_revision = '6c6941a9191f'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE AGGREGATE jsonb_merge(jsonb) (
            SFUNC = 'jsonb_concat',
            STYPE = jsonb,
            INITCOND = '{}'
        )"""
    )


def downgrade() -> None:
    op.execute("DROP AGGREGATE jsonb_merge(jsonb)")
