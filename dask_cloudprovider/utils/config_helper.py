import copy
import dask.config


def prune_defaults(cfg: dict, defaults: dict) -> dict:
    """
    Recursively remove any key in cfg whose value exactly equals
    the corresponding built-in default.
    """
    pruned = {}
    for key, val in cfg.items():
        if key not in defaults:
            pruned[key] = val
        else:
            default_val = defaults[key]
            if isinstance(val, dict) and isinstance(default_val, dict):
                nested = prune_defaults(val, default_val)
                if nested:
                    pruned[key] = nested
            elif val != default_val:
                pruned[key] = val
    return pruned


def serialize_custom_config() -> str:
    """
    Pull out only the user-overrides from global_config and serialize them.
    """
    user_cfg = copy.deepcopy(dask.config.global_config)
    defaults = dask.config.merge(*dask.config.defaults)
    pruned = prune_defaults(user_cfg, defaults)
    return dask.config.serialize(pruned)
