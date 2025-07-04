import dask.config

from dask_cloudprovider.utils.config_helper import (
    prune_defaults,
    serialize_custom_config,
)


def test_prune_defaults_simple():
    # Keys matching defaults get dropped; new keys stay
    cfg = {"a": 1, "b": 2, "c": 3}
    defaults = {"a": 1, "b": 0}
    pruned = prune_defaults(cfg, defaults)
    assert pruned == {"b": 2, "c": 3}


def test_prune_defaults_nested():
    # Nested dicts: only subkeys that differ survive
    cfg = {
        "outer": {"keep": 41, "drop": 0},
        "solo": 99,
    }
    defaults = {
        "outer": {"keep": 42, "drop": 0},
        "solo": 0,
    }
    pruned = prune_defaults(cfg, defaults)
    # 'outer.drop' matches default, 'outer.keep' differs; 'solo' differs
    assert pruned == {"outer": {"keep": 41}, "solo": 99}


def test_serialize_custom_config(monkeypatch):
    # Arrange a fake global_config and defaults
    fake_global = {"x": 10, "y": {"a": 1, "b": 0}}
    fake_defaults = {"x": 0, "y": {"a": 1, "b": 0}}

    # Monkey-patch dask.config
    monkeypatch.setattr(dask.config, "global_config", fake_global)
    # defaults should be a sequence of dict(s)
    monkeypatch.setattr(dask.config, "defaults", (fake_defaults,))

    # Serialize the custom config
    serialized = serialize_custom_config()
    assert isinstance(serialized, str)

    # Assert it's valid JSON and only contains overrides (x and nothing under y)
    pruned = dask.config.deserialize(serialized)
    assert pruned == {"x": 10}
