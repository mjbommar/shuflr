#!/usr/bin/env bash
# Build the shuflr-client wheel and run the Python tests.
# Prefers `uv` when available (fast), falls back to `python3 -m venv` + pip.
set -euo pipefail

# Tell uv (and maturin, which shells out to uv) to ignore any user-
# level config: this script should produce identical output regardless
# of the dev's ~/.config/uv/uv.toml.
export UV_NO_CONFIG=1

HERE="$(cd "$(dirname "$0")" && pwd)"
CRATE="$(dirname "$HERE")"
WORKSPACE="$(cd "$CRATE/../.." && pwd)"

# Build the Rust binary with the serve feature first, so the Python
# test harness can spawn it.
( cd "$WORKSPACE" && cargo build --features serve )

cd "$CRATE"

if command -v uv >/dev/null 2>&1; then
    export UV_PROJECT_ENVIRONMENT="$CRATE/.venv"
    if [ ! -d .venv ]; then
        uv --no-config venv .venv
    fi
    uv --no-config pip install -q --python .venv/bin/python "maturin>=1.5,<2.0" pytest
    PY_BIN="$CRATE/.venv/bin/python"
else
    if [ ! -d .venv ]; then
        python3 -m venv .venv
    fi
    # shellcheck disable=SC1091
    source .venv/bin/activate
    pip install -q --upgrade pip
    pip install -q "maturin>=1.5,<2.0" pytest
    PY_BIN="$CRATE/.venv/bin/python"
fi

# Develop-install the extension module into the venv. maturin uses
# $VIRTUAL_ENV (set below) to pick the interpreter.
export VIRTUAL_ENV="$CRATE/.venv"
export PATH="$CRATE/.venv/bin:$PATH"
"$CRATE/.venv/bin/maturin" develop --release

SHUFLR_BIN="$WORKSPACE/target/debug/shuflr" \
"$CRATE/.venv/bin/pytest" -q "$@"
