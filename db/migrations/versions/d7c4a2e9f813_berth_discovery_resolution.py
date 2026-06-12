"""berth-discovery resolution (Phase-2 auto-add)

Phase 2 consumes discovery_candidates: an unknown tanker whose latest position is
inside an LNG *berth* polygon is VF-enriched (`scripts/discover_berth_tankers.py`),
and registered into vessel_registry iff VF confirms TYPE = 'LNG Tanker'. A tanker
physically in a correctly-tagged LNG berth is near-deterministically an LNG carrier;
the VF type-check is the final gate.

This adds the resolution bookkeeping so each candidate is VF-polled at most once
(a negative cache — a VF record costs credits even when the answer is "not LNG"):

  - resolved_at  set when the candidate has been VF-checked (NULL = unprocessed)
  - outcome      registered | not_lng | no_master | error
  - vf_type      the VF MASTERDATA.TYPE seen at resolution (audit)

It also extends vf_rescue_log.result with 'not_lng' so a non-LNG VF hit (3 credits,
billed-per-record) reconciles against the shared rescue/discovery glide budget.

Revision ID: d7c4a2e9f813
Revises: a3f9c1e7b250
Create Date: 2026-06-12

"""

from typing import Sequence, Union

from alembic import op


revision: str = "d7c4a2e9f813"
down_revision: Union[str, Sequence[str], None] = "a3f9c1e7b250"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE discovery_candidates ADD COLUMN resolved_at TIMESTAMPTZ")
    op.execute("ALTER TABLE discovery_candidates ADD COLUMN outcome TEXT")
    op.execute("ALTER TABLE discovery_candidates ADD COLUMN vf_type TEXT")
    op.execute(
        "CREATE INDEX ix_discovery_candidates_unresolved "
        "ON discovery_candidates (resolved_at) WHERE resolved_at IS NULL"
    )
    op.execute(
        "ALTER TABLE vf_rescue_log DROP CONSTRAINT vf_rescue_log_result_check"
    )
    op.execute(
        """
        ALTER TABLE vf_rescue_log ADD CONSTRAINT vf_rescue_log_result_check
        CHECK (result IN (
            'rescued', 'no_position', 'rejected_stale', 'rejected_teleport',
            'error', 'dry_run', 'skipped_budget', 'not_lng'
        ))
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_discovery_candidates_unresolved")
    op.execute("ALTER TABLE discovery_candidates DROP COLUMN IF EXISTS vf_type")
    op.execute("ALTER TABLE discovery_candidates DROP COLUMN IF EXISTS outcome")
    op.execute("ALTER TABLE discovery_candidates DROP COLUMN IF EXISTS resolved_at")
    op.execute(
        "ALTER TABLE vf_rescue_log DROP CONSTRAINT vf_rescue_log_result_check"
    )
    op.execute(
        """
        ALTER TABLE vf_rescue_log ADD CONSTRAINT vf_rescue_log_result_check
        CHECK (result IN (
            'rescued', 'no_position', 'rejected_stale', 'rejected_teleport',
            'error', 'dry_run', 'skipped_budget'
        ))
        """
    )
