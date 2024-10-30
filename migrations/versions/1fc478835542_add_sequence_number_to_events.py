"""add sequence number to events

Revision ID: 1fc478835542
Revises: ab5994a38d82
Create Date: 2024-10-30 11:36:04.412225

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "1fc478835542"
down_revision: Union[str, None] = "ab5994a38d82"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            ALTER TABLE events
            ADD COLUMN sequence_number SERIAL;
            """
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            ALTER TABLE events
            DROP COLUMN sequence_number;
            """
        )
    )
