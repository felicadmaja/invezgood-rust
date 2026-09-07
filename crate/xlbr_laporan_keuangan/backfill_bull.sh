#!/usr/bin/env bash
# Backfill interest_paid + tax_paid untuk emiten BULL saja.
# Usage (dari repo root atau folder crate ini):
#   ./crate/xlbr_laporan_keuangan/backfill_bull.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

exec cargo run -p xlbr_laporan_keuangan --example backfill_interest_tax_paid -- BULL
