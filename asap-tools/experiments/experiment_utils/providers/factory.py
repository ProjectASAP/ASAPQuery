"""
Provider factory for creating appropriate infrastructure providers.

This module contains the factory logic for instantiating the correct
infrastructure provider based on configuration parameters.
"""

from omegaconf import DictConfig

from .base import InfrastructureProvider
from .cloudlab import CloudLabProvider
from .local import LocalProvider


def create_provider(cfg: DictConfig) -> InfrastructureProvider:
    """
    Create the appropriate infrastructure provider based on configuration.

    This function analyzes the configuration to determine which infrastructure
    provider should be used and returns an instance of that provider.

    Args:
        cfg: Hydra configuration object containing infrastructure settings

    Returns:
        Configured infrastructure provider instance

    Raises:
        ValueError: If the configuration doesn't contain required parameters
                   or specifies an unsupported provider type
    """
    if hasattr(cfg.providers, "local"):
        return LocalProvider(home_dir=cfg.providers.local.home_dir)

    if not cfg.providers.cloudlab.username:
        raise ValueError(
            "Missing 'providers.cloudlab.username' configuration parameter"
        )

    if not cfg.providers.cloudlab.hostname_suffix:
        raise ValueError(
            "Missing 'providers.cloudlab.hostname_suffix' configuration parameter"
        )

    return CloudLabProvider(
        username=cfg.providers.cloudlab.username,
        hostname_suffix=cfg.providers.cloudlab.hostname_suffix,
    )


def detect_provider_type(cfg: DictConfig) -> str:
    """
    Detect the provider type from configuration.

    This function analyzes the configuration to determine which type of
    infrastructure provider should be used.

    Args:
        cfg: Hydra configuration object

    Returns:
        String identifier for the provider type ('cloudlab', 'aws', 'local', etc.)
    """
    if hasattr(cfg.providers, "local"):
        return "local"

    if cfg.providers.cloudlab.username and cfg.providers.cloudlab.hostname_suffix:
        return "cloudlab"

    raise ValueError(
        "Unable to detect infrastructure provider type from configuration. "
        "Currently supported: CloudLab (requires providers.cloudlab.username and providers.cloudlab.hostname_suffix) "
        "or Local (requires providers.local section in config/config.yaml)"
    )
