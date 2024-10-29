"""make dates timezoned

Revision ID: ab5994a38d82
Revises: de443b0391c2
Create Date: 2024-10-29 11:21:00.177992

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "ab5994a38d82"
down_revision: Union[str, None] = "de443b0391c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            ALTER TABLE events
            ALTER COLUMN observed_at 
                TYPE TIMESTAMP WITH TIME ZONE
                USING observed_at AT TIME ZONE 'UTC';
            ALTER TABLE events
            ALTER COLUMN occurred_at 
                TYPE TIMESTAMP WITH TIME ZONE
                USING occurred_at AT TIME ZONE 'UTC';
            """
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            ALTER TABLE events
            ALTER COLUMN observed_at 
                TYPE TIMESTAMP WITHOUT TIME ZONE
                USING observed_at;
            ALTER TABLE events
            ALTER COLUMN occurred_at 
                TYPE TIMESTAMP WITH TIME ZONE
                USING occurred_at;
            """
        )
    )
