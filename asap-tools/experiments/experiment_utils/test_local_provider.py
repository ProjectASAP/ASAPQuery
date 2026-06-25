"""Self-check for local-vs-CloudLab provider dispatch (issue #424).

Runnable standalone (`python3 test_local_provider.py`) or via pytest.
"""

import os
import sys

_EXPERIMENTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _EXPERIMENTS_DIR)

from omegaconf import OmegaConf  # noqa: E402
import omegaconf.errors as oerr  # noqa: E402

import config  # noqa: E402
from providers.factory import create_provider, detect_provider_type  # noqa: E402
from providers.local import LocalProvider  # noqa: E402
from providers.cloudlab import CloudLabProvider  # noqa: E402


def _base_cfg(**overrides):
    base = OmegaConf.load(os.path.join(_EXPERIMENTS_DIR, "config", "config.yaml"))
    del base["defaults"]  # not relevant to provider dispatch
    return OmegaConf.merge(base, {"experiment": {"name": "smoke"}, **overrides})


def test_local_mode_never_touches_cloudlab_mandatory_fields():
    cfg = _base_cfg(local={"home_dir": "/tmp/sketchdb_home"})
    provider = create_provider(cfg)
    assert isinstance(provider, LocalProvider)
    assert provider.is_remote() is False
    assert provider.get_node_ip(0) == "127.0.0.1"
    assert provider.get_home_dir() == "/tmp/sketchdb_home"
    assert detect_provider_type(cfg) == "local"
    assert config.get_node_params(cfg) == (0, 0)
    assert config.required_cloudlab_params(cfg) == []

    try:
        cfg.cloudlab.username
        raise AssertionError("cfg.cloudlab.username should still be mandatory-missing")
    except oerr.MissingMandatoryValue:
        pass


def test_local_args_resolve_remote_write_ip_through_provider():
    cfg = _base_cfg(local={"home_dir": "/tmp/sketchdb_home"})
    provider = create_provider(cfg)
    OmegaConf.register_new_resolver(
        "remote_write_ip", provider.get_node_ip, replace=True
    )
    args = config.Args(cfg)
    assert args.num_nodes == 0
    assert args.node_offset == 0
    assert args.cloudlab_username is None
    assert args.hostname_suffix is None
    assert args.remote_write_ip == "127.0.0.1"
    assert args.get_node_range(include_coordinator=True) == [0]


def test_cloudlab_mode_unchanged():
    cfg = _base_cfg(
        cloudlab={
            "num_nodes": 2,
            "username": "milindsr",
            "hostname_suffix": "exp.cloudlab.us",
        }
    )
    provider = create_provider(cfg)
    assert isinstance(provider, CloudLabProvider)
    assert provider.is_remote() is True
    assert provider.get_home_dir() == "/scratch/sketch_db_for_prometheus"
    assert detect_provider_type(cfg) == "cloudlab"
    assert config.get_node_params(cfg) == (2, 0)
    assert len(config.required_cloudlab_params(cfg)) == 3

    OmegaConf.register_new_resolver(
        "remote_write_ip", provider.get_node_ip, replace=True
    )
    args = config.Args(cfg)
    assert args.cloudlab_username == "milindsr"
    assert args.remote_write_ip == "10.10.1.1"


if __name__ == "__main__":
    test_local_mode_never_touches_cloudlab_mandatory_fields()
    test_local_args_resolve_remote_write_ip_through_provider()
    test_cloudlab_mode_unchanged()
    print("OK")
