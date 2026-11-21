#!/bin/bash
# Prepared inputs and gold tables are kept outside the submission repository.
set -euo pipefail
: "${BOARD_DATA:?set BOARD_DATA to the prepared evaluator directory}"
export BOARD_DATA
BOARD_DATA=$(realpath "$BOARD_DATA")
if [[ -z ${BOARD_IMAGE:-} ]]; then
    : "${BOARD_BINARY:?set BOARD_BINARY to the candidate executable or BOARD_IMAGE to a Docker tag}"
    export BOARD_BINARY
    BOARD_BINARY=$(realpath "$BOARD_BINARY")
fi
export POLARS_MAX_THREADS=8 OPENBLAS_NUM_THREADS=1
exec systemd-run --user --scope --quiet -p MemoryMax=24G -p MemorySwapMax=0 \
    -p CPUQuota=800% -p TasksMax=4096 -- taskset -c "${BOARD_CPUS:-8-11,72-75}" \
    "${BOARD_PYTHON:-python3}" "$(dirname "$0")/localboard.py" "$@"
