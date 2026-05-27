"""update german baltic terminals

Revision ID: dd4bf1d0e21f
Revises: b3d5b76d7f0d
Create Date: 2026-05-26 21:30:06.355982

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'dd4bf1d0e21f'
down_revision: Union[str, Sequence[str], None] = 'b3d5b76d7f0d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        INSERT INTO terminals (terminal_name, country, flow_direction, in_signal_scope, is_fsru, notes)
        VALUES (
            'Mukran (Deutsche Ostsee)',
            'DE',
            'import',
            TRUE,
            TRUE,
            'Neptune + Energos Power FSRUs at Mukran port, Rugen island. Replaced Lubmin II as active German Baltic import terminal. Operational 2024.'
        )
    """)
    op.execute("""
        UPDATE terminals SET
            in_signal_scope = FALSE,
            notes = 'FSRU Neptune departed May 2024 to Mukran. Site repurposed for hydrogen terminal. Retired as LNG import terminal.'
        WHERE terminal_name = 'Lubmin II FSRU'
    """)


def downgrade() -> None:
    op.execute("DELETE FROM terminals WHERE terminal_name = 'Mukran (Deutsche Ostsee)'")
    op.execute("""
        UPDATE terminals SET
            in_signal_scope = TRUE,
            notes = 'Deutsche Courage'
        WHERE terminal_name = 'Lubmin II FSRU'
    """)

