#!/usr/bin/env bash
# Refresh the vendored copy of Zamboni's writeCompleted event contract (DUCKLKD-86).
#
# Zamboni owns the writeCompleted event (docs/write-completed.md); this repo
# owns a pinned copy of its published JSON Schema.
# tests/test_write_completed_contract.py validates the events
# target_ducklake/write_completed.py builds against that copy, and checks that
# its contract version is the one the code states (CONTRACT_VERSION). So the
# tests run with no network, and a contract change arrives as a reviewed diff
# of a checked-in file.
#
# Re-pin REF below, run this, and the tests either still pass or name what
# changed. The same pattern as bin/refresh-table-config-contract.sh.
set -euo pipefail

# The Zamboni tag this copy came from. Bump deliberately, never automatically.
REF="v0.7.0"
REPO="paulcaron16k/Zamboni"
# Zamboni promises this path; moving it is a breaking change there.
SPEC="src/zamboni/zamboni-write-completed_spec.json"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
dest="${root}/tests/contracts/zamboni-write-completed_spec.json"

for tool in gh jq; do
    if ! command -v "${tool}" >/dev/null 2>&1; then
        echo "error: ${tool} is required to fetch and check the contract" >&2
        exit 1
    fi
done

staging="$(mktemp -d)"
trap 'rm -rf "${staging}"' EXIT

echo "Fetching ${SPEC} from ${REPO}@${REF}..."
gh api "repos/${REPO}/contents/${SPEC}?ref=${REF}" --jq '.content' | base64 -d > "${staging}/spec.json"
# Refuse anything that is not the event's schema: a 404 body or an error page
# would otherwise land in the repo.
version="$(jq -r 'if type == "object" then .["x-contract-version"] // empty else empty end' "${staging}/spec.json" 2>/dev/null || true)"
if [[ -z "${version}" ]]; then
    echo "error: ${SPEC} states no x-contract-version; nothing overwritten" >&2
    exit 1
fi
mv "${staging}/spec.json" "${dest}"
echo "Wrote ${dest} (writeCompleted contract ${version}, from ${REF})."
echo
echo "Now run: uv run pytest tests/test_write_completed_contract.py"
echo "If it fails, the contract moved: read the diff before changing write_completed.py."
