"""add zone to terminals and expand port_events valid_zone constraint

Revision ID: 3f8a2c1d9e47
Revises: dd4bf1d0e21f
Create Date: 2026-05-27

"""
from typing import Sequence, Union

from alembic import op


revision: str = '3f8a2c1d9e47'
down_revision: Union[str, Sequence[str], None] = 'dd4bf1d0e21f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

VALID_ZONES = ('usgulf', 'usatlantic', 'nweurope', 'baltic', 'iberian', 'wmed', 'emed')

# (terminal_name, zone)
ZONE_ASSIGNMENTS = [
    # US Gulf export
    ('Calcasieu Pass',           'usgulf'),
    ('Cameron',                  'usgulf'),
    ('Corpus Christi',           'usgulf'),
    ('Freeport',                 'usgulf'),
    ('Golden Pass',              'usgulf'),
    ('Plaquemines',              'usgulf'),
    ('Sabine Pass',              'usgulf'),
    # US Atlantic export
    ('Cove Point',               'usatlantic'),
    ('Elba Island',              'usatlantic'),
    # NW Europe import (North Sea coast + Channel)
    ('Zeebrugge',                'nweurope'),
    ('Brunsbuttel FSRU',         'nweurope'),
    ('Lubmin II FSRU',           'nweurope'),
    ('Wilhelmshaven 1 FSRU',     'nweurope'),
    ('Wilhelmshaven 2 FSRU',     'nweurope'),
    ('Dunkerque',                'nweurope'),
    ('Isle of Grain',            'nweurope'),
    ('South Hook',               'nweurope'),
    ('Eemshaven FSRU',           'nweurope'),
    ('Gate (Rotterdam)',         'nweurope'),
    # Baltic import
    ('Mukran (Deutsche Ostsee)', 'baltic'),
    ('Swinoujscie',              'baltic'),
    ('Klaipeda FSRU',            'baltic'),
    # Iberian Atlantic import
    ('Bilbao',                   'iberian'),
    ('Huelva',                   'iberian'),
    ('Sines',                    'iberian'),
    # W Mediterranean import
    ('Barcelona',                'wmed'),
    ('Cartagena',                'wmed'),
    ('Sagunto',                  'wmed'),
    ('Adriatic LNG',             'wmed'),
    ('Piombino FSRU',            'wmed'),
    ('Ravenna FSRU',             'wmed'),
    ('Krk (LNG Croatia)',        'wmed'),
    # E Mediterranean import
    ('Alexandroupolis FSRU',     'emed'),
    ('Revithoussa',              'emed'),
    # DZ, EG, NG, TT export terminals are out of scope — zone left NULL
]


def upgrade() -> None:
    zones_literal = ", ".join(f"'{z}'" for z in VALID_ZONES)

    op.execute(f"""
        ALTER TABLE terminals
        ADD COLUMN zone TEXT
        CHECK (zone IN ({zones_literal}))
    """)

    for terminal_name, zone in ZONE_ASSIGNMENTS:
        op.execute(f"""
            UPDATE terminals SET zone = '{zone}'
            WHERE terminal_name = '{terminal_name}'
        """)

    # Rebuild port_events valid_zone constraint
    op.execute("ALTER TABLE port_events DROP CONSTRAINT valid_zone")
    op.execute(f"""
        ALTER TABLE port_events
        ADD CONSTRAINT valid_zone CHECK (zone IN ({zones_literal}))
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE port_events DROP CONSTRAINT valid_zone")
    op.execute("ALTER TABLE port_events ADD CONSTRAINT valid_zone CHECK (zone IN ('usgulf','nweurope'))")
    op.execute("ALTER TABLE terminals DROP COLUMN zone")
