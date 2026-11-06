"""spec.json, plugin-declaration.yml and meltano.yml against config_jsonschema.

config_jsonschema (target.py) is the loader's settings surface. Three files
describe it to others and must not drift from it: spec.json (the xflow admin
UI's form, generated), meltano.yml's loader (hand-written) and
plugin-declaration.yml (generated from meltano.yml). DUCKLKD-42.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
import typing as t
from pathlib import Path

import jsonschema
import pytest
import yaml

from target_ducklake.admin_spec import (
    EXPOSED,
    REQUIRED,
    SYNC_MODES,
    UI_EXCLUDED,
    config_schema,
    form_schema,
)
from target_ducklake.layout import LOAD_METHODS
from target_ducklake.target import Targetducklake

if t.TYPE_CHECKING:
    from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]
RENDERER = ROOT / "bin" / "render-admin-ui.py"
# Meltano's own schema for meltano.yml, vendored by
# bin/refresh-meltano-schema-contract.sh at a pinned Meltano release.
MELTANO_SCHEMA = ROOT / "tests" / "contracts" / "meltano.schema.json"

# Objects Meltano declares leaf by leaf (nats.servers); every other object is
# one setting of kind object.
DECOMPOSED = frozenset({"nats"})

# Leaf names that hold a credential: they must be secret on every surface.
CREDENTIAL = re.compile(r"(password|secret|token|_key|_url)$")

Json = dict[str, t.Any]
Settings = dict[str, Json]

# A JSON Schema type, as a Meltano setting kind. Not decimal for a number:
# Meltano 4.3 reads a decimal set from the environment as a Decimal, which its
# own JSON encoder can't write to the loader's config file. A number setting
# is a string in Meltano, which the target parses.
KINDS = {
    "string": "string",
    "integer": "integer",
    "number": "string",
    "boolean": "boolean",
    "object": "object",
    "array": "array",
}


def _renderer() -> ModuleType:
    """bin/render-admin-ui.py, imported (its name has dashes)."""
    spec = importlib.util.spec_from_file_location("render_admin_ui", RENDERER)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _leaves(properties: dict[str, t.Any]) -> dict[str, dict[str, t.Any]]:
    """Settings by the name Meltano gives them, DECOMPOSED objects as leaves."""
    leaves = {}
    for name, prop in properties.items():
        if name in DECOMPOSED:
            leaves.update(
                {f"{name}.{leaf}": sub for leaf, sub in prop["properties"].items()}
            )
        else:
            leaves[name] = prop
    return leaves


def _loader(document: Json) -> Json:
    (loader,) = document["plugins"]["loaders"]
    return loader


@pytest.fixture(scope="module")
def schema_settings() -> dict[str, dict[str, t.Any]]:
    """config_jsonschema's settings, by Meltano name."""
    return _leaves(config_schema()["properties"])


@pytest.fixture(scope="module")
def meltano_loader() -> dict[str, t.Any]:
    """meltano.yml's target-ducklake loader."""
    return _loader(yaml.safe_load((ROOT / "meltano.yml").read_text()))


@pytest.fixture(scope="module")
def meltano_settings(meltano_loader: dict[str, t.Any]) -> dict[str, dict[str, t.Any]]:
    """meltano.yml's settings, by name."""
    return {setting["name"]: setting for setting in meltano_loader["settings"]}


@pytest.fixture(scope="module")
def spec() -> dict[str, t.Any]:
    """spec.json as committed."""
    return json.loads((ROOT / "spec.json").read_text())


@pytest.fixture(scope="module")
def declaration() -> dict[str, t.Any]:
    """plugin-declaration.yml as committed."""
    return yaml.safe_load((ROOT / "plugin-declaration.yml").read_text())


# The generated files.


def test_generated_files_are_current():
    """spec.json and plugin-declaration.yml are exactly what the renderer writes.

    The whole file, header included: it is what a consumer copies.
    """
    for path, contents in _renderer().rendered().items():
        assert path.read_text() == contents, (
            f"{path.name} is stale: run bin/render-admin-ui.py"
        )


def test_render_check_exits_zero_when_current():
    """The committed files are current, as CI's --check would say."""
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(RENDERER), "--check"],
        capture_output=True,
        text=True,
        check=False,
        cwd=ROOT,
    )
    assert result.returncode == 0, result.stderr


def test_render_check_names_a_stale_file_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """--check reports a stale file and leaves it; a plain run rewrites it."""
    renderer = _renderer()
    stale = tmp_path / "spec.json"
    stale.write_text("{}\n")
    monkeypatch.setattr(renderer, "rendered", lambda: {stale: "new\n"})

    assert renderer.main(["--check"]) == 1
    assert stale.read_text() == "{}\n"
    assert renderer.main([]) == 0
    assert stale.read_text() == "new\n"


def test_config_schema_has_the_sdk_settings_before_any_target_is_built():
    """The SDK adds batch_config & co. when a target is built; ours has them at import.

    In a fresh interpreter, so no other test has built a target first: before,
    a test reading config_jsonschema passed or failed with the test order.
    """
    code = (
        "import json; from target_ducklake.target import Targetducklake as T; "
        "print(json.dumps(sorted(T.config_jsonschema['properties'])))"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
        cwd=ROOT,
    )
    names = set(json.loads(result.stdout))

    assert {"batch_config", "faker_config", "stream_maps", "hard_delete"} <= names


# spec.json: the admin UI's form.


def test_spec_envelope(spec: Json):
    """The ConnectorSpecification fields the admin UI reads; a draft-07 form."""
    assert spec["documentationUrl"].startswith(
        "https://github.com/paulcaron16k/target-ducklake-definite"
    )
    assert spec["supportsDbt"] is False
    assert set(spec["supportedDestinationSyncModes"]) == set(SYNC_MODES.values())
    connection = spec["connectionSpecification"]
    jsonschema.Draft7Validator.check_schema(connection)
    assert connection["additionalProperties"] is False
    assert connection["required"] == list(REQUIRED)


def test_every_load_method_has_a_sync_mode():
    """A merge keeps one row per key: Airbyte's append_dedup."""
    assert set(SYNC_MODES) == set(LOAD_METHODS)
    assert SYNC_MODES == {
        "append": "append",
        "merge": "append_dedup",
        "overwrite": "overwrite",
    }


def test_the_form_offers_only_real_settings_in_order(spec: Json):
    """The form's fields are EXPOSED settings, numbered in that order."""
    properties = spec["connectionSpecification"]["properties"]
    real = config_schema()["properties"]

    assert list(properties) == list(EXPOSED)
    assert set(properties) <= set(real)
    assert [field["order"] for field in properties.values()] == list(
        range(len(EXPOSED))
    )
    assert set(REQUIRED) <= set(properties)


def test_every_setting_is_offered_or_left_out_with_a_reason():
    """A new setting fails here until someone decides whether the form offers it."""
    settings = set(config_schema()["properties"])

    assert sorted(settings - set(EXPOSED) - set(UI_EXCLUDED)) == []
    assert sorted(set(UI_EXCLUDED) - settings) == [], (
        "excluded settings that don't exist"
    )
    assert sorted(set(UI_EXCLUDED) & set(EXPOSED)) == []
    assert all(reason.strip() for reason in UI_EXCLUDED.values())


def test_form_fields_agree_with_the_settings(spec: Json):
    """Type, enum, default, bounds, title and description come from the schema."""
    real = config_schema()["properties"]
    for name, field in spec["connectionSpecification"]["properties"].items():
        setting = form_schema(real[name])
        assert field["type"] == setting["type"], name
        assert isinstance(field["type"], str), f"{name}: a form field has one type"
        for keyword in ("title", "description", "default", "enum", "minimum"):
            assert field.get(keyword) == setting.get(keyword), f"{name}: {keyword}"


def test_form_schema_drops_the_environment_variable_string():
    """A string-or-integer setting is an integer field; null is dropped."""
    prop = {
        "oneOf": [
            {"type": "string", "description": "as text"},
            {"type": "integer", "minimum": 0},
            {"type": "null"},
        ],
        "title": "Level",
        "default": 0,
    }

    assert form_schema(prop) == {
        "type": "integer",
        "minimum": 0,
        "title": "Level",
        "default": 0,
    }
    assert form_schema({"type": ["string", "null"]}) == {"type": "string"}
    with pytest.raises(ValueError, match="one non-null type"):
        form_schema({"type": ["string", "integer"]})


def test_a_config_the_form_accepts_is_one_the_target_accepts(spec: Json):
    """Required fields filled and every default taken: the target starts."""
    connection = spec["connectionSpecification"]
    config = {
        name: field["default"]
        for name, field in connection["properties"].items()
        if "default" in field
    }
    config.update({"catalog_url": "lake.sqlite", "data_path": "/tmp/lake"})  # noqa: S108
    config["catalog_type"] = "sqlite"

    jsonschema.Draft7Validator(connection).validate(config)
    target = Targetducklake(config=config)
    assert target.config["data_path"] == "/tmp/lake"  # noqa: S108
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft7Validator(connection).validate({"catalog_url": "x"})


# meltano.yml: the loader's Meltano settings.


def test_meltano_declares_every_setting_and_no_other(
    schema_settings: Settings, meltano_settings: Settings
):
    """meltano.yml names each setting, nested ones dotted, and nothing else."""
    assert sorted(set(schema_settings) - set(meltano_settings)) == [], "missing"
    assert sorted(set(meltano_settings) - set(schema_settings)) == [], "unknown"


def test_meltano_kinds_options_and_defaults_agree(
    schema_settings: Settings, meltano_settings: Settings
):
    """Each setting's kind (or options) and default are its schema's."""
    for name, setting in meltano_settings.items():
        prop = form_schema(schema_settings[name])
        if "enum" in prop:
            assert setting.get("kind") == "options", name
            assert [option["value"] for option in setting["options"]] == [
                str(value) for value in prop["enum"]
            ], name
        else:
            assert setting.get("kind", "string") == KINDS[prop["type"]], name
        default = prop.get("default")
        if prop.get("type") == "number" and default is not None:
            default = str(default)
        assert setting.get("value") == default, f"{name}: default"


def test_secrets_agree_everywhere(
    schema_settings: Settings, meltano_settings: Settings, spec: Json
):
    """secret, sensitive and airbyte_secret mark the same settings."""
    secrets = {name for name, prop in schema_settings.items() if prop.get("secret")}
    sensitive = {
        name for name, setting in meltano_settings.items() if setting.get("sensitive")
    }
    form = spec["connectionSpecification"]["properties"]
    masked = {name for name, field in form.items() if field.get("airbyte_secret")}

    assert sensitive == secrets
    assert masked == secrets & set(form)


def test_every_credential_is_secret(schema_settings: Settings):
    """Catalog URL (it can hold the password), storage keys, NATS password and token."""
    credentials = {name for name in schema_settings if CREDENTIAL.search(name)}

    assert {
        "catalog_url",
        "public_key",
        "secret_key",
        "nats.password",
        "nats.token",
    } <= credentials
    assert (
        sorted(name for name in credentials if not schema_settings[name].get("secret"))
        == []
    )


def test_meltano_capabilities_are_the_targets(meltano_loader: Json):
    """Every capability the target has that Meltano's loader vocabulary names."""
    vocabulary = json.loads(MELTANO_SCHEMA.read_text())["$defs"]["plugins"]["loaders"][
        "allOf"
    ][1]["properties"]["capabilities"]["items"]["enum"]

    assert meltano_loader["capabilities"] == [
        str(capability)
        for capability in Targetducklake.capabilities
        if str(capability) in vocabulary
    ]


def test_meltano_group_validation_is_the_forms_required(meltano_loader: Json):
    """Meltano and the form require the same settings."""
    assert meltano_loader["settings_group_validation"] == [list(REQUIRED)]


def test_meltano_yml_is_valid_for_meltano():
    """Against Meltano's own schema: kinds, keys and capabilities it knows."""
    project = yaml.safe_load((ROOT / "meltano.yml").read_text())

    jsonschema.validate(project, json.loads(MELTANO_SCHEMA.read_text()))


# plugin-declaration.yml: what a Meltano project copies.


def test_declaration_is_meltanos_loader_shape(declaration: Json, meltano_loader: Json):
    """A loader Meltano accepts, with meltano.yml's settings and capabilities."""
    jsonschema.validate(
        {"version": 1, "plugins": {"loaders": [declaration]}},
        json.loads(MELTANO_SCHEMA.read_text()),
    )
    assert declaration["settings"] == meltano_loader["settings"]
    assert declaration["capabilities"] == meltano_loader["capabilities"]


def test_declaration_installs_a_tagged_release_with_the_nats_extra(declaration: Json):
    """pip_url installs a tag of the public repo, not the checkout (-e .)."""
    pip_url = declaration["pip_url"]

    assert pip_url.startswith("target-ducklake[nats]@git+https://github.com/")
    assert "paulcaron16k/target-ducklake-definite.git@<tag>" in pip_url
    assert " " not in pip_url, "Meltano splits pip_url on spaces"


def test_declaration_keeps_nested_settings_dotted(declaration: Json):
    """nats.servers, not nats_servers: Meltano only nests dotted names."""
    names = {setting["name"] for setting in declaration["settings"]}

    assert {"nats.servers", "nats.password", "stream_defaults"} <= names
    assert not [name for name in names if name.startswith("nats_")]
