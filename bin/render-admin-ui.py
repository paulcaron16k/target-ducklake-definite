#!/usr/bin/env python
"""Render the files the xflow admin UI and Meltano users copy (DUCKLKD-42).

* spec.json, from the target's config_jsonschema (target_ducklake/admin_spec.py
  holds what only the form needs).
* plugin-declaration.yml, from meltano.yml's target-ducklake loader block.

Both are generated: edit their sources and run this. tests/test_admin_ui.py
fails while either file is stale, and while meltano.yml and config_jsonschema
disagree.

Usage:
    bin/render-admin-ui.py            # write both files
    bin/render-admin-ui.py --check    # exit 1 if either is stale
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
# Render this checkout's spec, whichever target_ducklake the venv installed.
sys.path.insert(0, str(ROOT))

from target_ducklake.admin_spec import render_spec  # noqa: E402

MELTANO_YML = ROOT / "meltano.yml"
SPEC = ROOT / "spec.json"
DECLARATION = ROOT / "plugin-declaration.yml"

LOADER = "target-ducklake"

# The loader keys a declaration carries, after name, namespace and pip_url.
_CARRIED = ("capabilities", "settings", "settings_group_validation")

# meltano.yml installs the checkout (-e .); a consumer installs a release. The
# nats extra lets the nats.* settings work. <tag> is left for the consumer to
# fill in: a branch would move under them.
PIP_URL = (
    "target-ducklake[nats]@"
    "git+https://github.com/paulcaron16k/target-ducklake-definite.git@<tag>"
)

_HEADER = """\
# Meltano plugin declaration for target-ducklake. GENERATED: do not edit.
#
# Written by bin/render-admin-ui.py from meltano.yml's target-ducklake loader;
# tests/test_admin_ui.py fails when it is stale. To change a setting, edit
# target_ducklake/target.py's config_jsonschema and meltano.yml (the tests
# keep them in agreement), then re-render.
#
# target-ducklake is not on Meltano Hub, so a project using it declares it as
# a custom loader: copy this block under plugins.loaders in meltano.yml.
#
# * pip_url: replace <tag> with a release tag (git tag -l 'v*'). A tag pins
#   the build; a branch changes under you. Use the first release that ships
#   this file or a later one: older tags (v0.0.2) lack the nats extra and
#   several of the settings below.
# * Settings: keep the whole list, even if you set a few. Meltano reads a
#   setting from the environment (TARGET_DUCKLAKE_<NAME>), types it and
#   redacts it only if it is declared. Nested settings use dotted names
#   (nats.servers, read from TARGET_DUCKLAKE_NATS_SERVERS): Meltano builds the
#   nats object from them. Free-form objects (stream_defaults,
#   stream_overrides, batch_config, ...) are one setting of kind object.
# * Settings marked sensitive hold credentials: set them from environment
#   variables or a secret store, not in meltano.yml.
"""


def render_declaration() -> str:
    """plugin-declaration.yml's contents.

    Returns:
        The declaration, header included.

    Raises:
        SystemExit: If meltano.yml declares no target-ducklake loader.
    """
    project = yaml.safe_load(MELTANO_YML.read_text(encoding="utf-8"))
    loader = next(
        (
            plugin
            for plugin in project.get("plugins", {}).get("loaders", [])
            if plugin.get("name") == LOADER
        ),
        None,
    )
    if loader is None:
        msg = f"{MELTANO_YML.name} declares no {LOADER} loader"
        raise SystemExit(msg)
    declaration = {
        "name": loader["name"],
        "namespace": loader["namespace"],
        "pip_url": PIP_URL,
        **{key: loader[key] for key in _CARRIED if key in loader},
    }
    body = yaml.safe_dump(declaration, sort_keys=False, width=88, allow_unicode=True)
    return f"{_HEADER}\n{body}"


def rendered() -> dict[Path, str]:
    """Each generated file's path, with the contents it should have.

    Returns:
        spec.json and plugin-declaration.yml, rendered.
    """
    return {SPEC: render_spec(), DECLARATION: render_declaration()}


def main(argv: list[str]) -> int:
    """Write the files, or with --check report the stale ones.

    Args:
        argv: The command-line arguments, without the program name.

    Returns:
        0, or 1 when --check finds a stale file.
    """
    check = "--check" in argv
    stale = []
    for path, contents in rendered().items():
        current = path.read_text(encoding="utf-8") if path.exists() else None
        if current == contents:
            continue
        if check:
            stale.append(path.name)
        else:
            path.write_text(contents, encoding="utf-8")
            sys.stdout.write(f"wrote {path.name}\n")
    if stale:
        sys.stderr.write(
            f"stale: {', '.join(stale)}. Re-render with bin/render-admin-ui.py\n"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
