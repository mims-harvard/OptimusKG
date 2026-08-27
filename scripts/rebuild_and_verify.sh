#!/usr/bin/env bash
# Regenerate the silver edge tables affected by the determinism fixes, then
# rebuild gold with BioCypher validation and re-run every verification check.
#
# Run this in its own terminal window (Super+Enter), not as a background job.
set -euo pipefail

cd "$(dirname "$0")/.."

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

NODES="silver.exposure_gene,silver.exposure_biological_process,silver.exposure_cellular_component,silver.exposure_molecular_function,silver.exposure_disease,silver.exposure_exposure"

log "Rebuilding silver edge tables affected by the determinism fix"
uv run kedro run --pipeline silver --nodes "$NODES"

log "Rebuilding gold (with BioCypher schema validation)"
uv run kedro run --pipeline gold --params gold.validate_biocypher=true

log "Checksums of regenerated silver tables"
uv run python - <<'PY'
from pathlib import Path

from optimuskg.utils import calculate_checksum

names = [
    "exposure_gene",
    "exposure_biological_process",
    "exposure_cellular_component",
    "exposure_molecular_function",
    "exposure_disease",
    "exposure_exposure",
]
for n in names:
    p = Path(f"data/silver/edges/{n}.parquet")
    print(f"{n:<32}{calculate_checksum(path=p, chunk_size=8192, digest_size=16)}")
PY

log "Verifying relation guarantees over the whole result"
uv run python scripts/verify_relation_guarantees.py

log "Verifying reproducibility of every checked builder"
uv run python scripts/check_determinism.py

log "Auditing for any remaining relation loss"
uv run python scripts/audit_relation_loss.py

log "Unit tests"
uv run pytest tests -q -p no:warnings

log "DONE - review the output above, then press enter to close"
read -r _
