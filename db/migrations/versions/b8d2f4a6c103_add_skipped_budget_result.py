"""add skipped_budget to vf_rescue_log result check

`skipped_budget` audits candidates the daily credit budget could not serve
(0 credits, no cooldown — the vessel stays eligible for later cycles). Logged
once per vessel per UTC day. This measures unmet rescue demand under the
glide-path cap: a vessel with a skipped_budget row and no later billed row the
same day went truly unserved. Powers the cap/allocation decision flagged in
docs/park-checkups.md (2026-06-04: cap saturated by ~09:00 UTC daily).

Revision ID: b8d2f4a6c103
Revises: f1a8c3d5e7b9
Create Date: 2026-06-05

"""

from typing import Sequence, Union

from alembic import op


revision: str = "b8d2f4a6c103"
down_revision: Union[str, Sequence[str], None] = "f1a8c3d5e7b9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RESULTS_OLD = (
    "'rescued','no_position','rejected_stale','rejected_teleport','error','dry_run'"
)
RESULTS_NEW = RESULTS_OLD + ",'skipped_budget'"


def upgrade() -> None:
    op.execute("ALTER TABLE vf_rescue_log DROP CONSTRAINT vf_rescue_log_result_check")
    op.execute(
        "ALTER TABLE vf_rescue_log ADD CONSTRAINT vf_rescue_log_result_check "
        f"CHECK (result IN ({RESULTS_NEW}))"
    )


def downgrade() -> None:
    op.execute("DELETE FROM vf_rescue_log WHERE result = 'skipped_budget'")
    op.execute("ALTER TABLE vf_rescue_log DROP CONSTRAINT vf_rescue_log_result_check")
    op.execute(
        "ALTER TABLE vf_rescue_log ADD CONSTRAINT vf_rescue_log_result_check "
        f"CHECK (result IN ({RESULTS_OLD}))"
    )
