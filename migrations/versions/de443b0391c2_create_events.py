"""create_events

Revision ID: de443b0391c2
Revises:
Create Date: 2024-10-23 17:22:51.772793

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "de443b0391c2"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
       CREATE TABLE events (
	        id TEXT NOT NULL, 
	        name TEXT NOT NULL, 
	        stream TEXT NOT NULL, 
	        category TEXT NOT NULL, 
	        position INT NOT NULL, 
	        payload JSONB, 
	        observed_at TIMESTAMP NOT NULL, 
	        occurred_at TIMESTAMP NOT NULL, 
	        PRIMARY KEY (id), 
	        UNIQUE (id)
        );
    """
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
        DROP TABLE events;
    """
        )
    )
