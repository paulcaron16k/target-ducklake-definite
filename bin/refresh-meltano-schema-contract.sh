#!/usr/bin/env bash
# Refresh the vendored copy of Meltano's JSON Schema for meltano.yml (DUCKLKD-42).
#
# tests/test_admin_ui.py validates meltano.yml and plugin-declaration.yml
# against this copy, and reads Meltano's loader capability names from it, so
# the tests run with no network and know what the Meltano release consumers
# use accepts. Re-pin REF below to move to a newer Meltano, run this, and let
# the tests name what changed. The same pattern as
# bin/refresh-table-config-contract.sh.
set -euo pipefail

# The Meltano release this copy came from. Bump deliberately.
REF="v4.3.0"
REPO="meltano/meltano"
SCHEMA="src/meltano/schemas/meltano.schema.json"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
dest="${root}/tests/contracts/meltano.schema.json"

for tool in gh jq; do
    if ! command -v "${tool}" >/dev/null 2>&1; then
        echo "error: ${tool} is required to fetch and check the schema" >&2
        exit 1
    fi
done

staging="$(mktemp -d)"
trap 'rm -rf "${staging}"' EXIT

echo "Fetching ${SCHEMA} from ${REPO}@${REF}..."
gh api "repos/${REPO}/contents/${SCHEMA}?ref=${REF}" --jq '.content' | base64 -d > "${staging}/schema.json"
# Refuse anything that is not Meltano's schema (a 404 body, an error page).
schema_id="$(jq -r 'if type == "object" then .["$id"] // empty else empty end' "${staging}/schema.json" 2>/dev/null || true)"
if [[ "${schema_id}" != "https://meltano.com/meltano.schema.json" ]]; then
    echo "error: ${SCHEMA} is not Meltano's meltano.yml schema; nothing overwritten" >&2
    exit 1
fi
mv "${staging}/schema.json" "${dest}"
echo "Wrote ${dest} (from ${REPO}@${REF})."
echo
echo "Now run: uv run pytest tests/test_admin_ui.py"
