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
    # Local mode (e.g. `+local.home_dir=...`) takes priority and must be checked
    # before any `cfg.cloudlab.*` access — those fields are Hydra `???` mandatory-
    # missing markers, so merely reading them raises if not provided via CLI.
    if hasattr(cfg, "local"):
        if not hasattr(cfg.local, "home_dir") or not cfg.local.home_dir:
            raise ValueError("Missing 'local.home_dir' configuration parameter")
        return LocalProvider(home_dir=cfg.local.home_dir)

    # Validate that we have the required CloudLab parameters
    if not hasattr(cfg, "cloudlab"):
        raise ValueError(
            "Missing 'cloudlab' configuration section. "
            "CloudLab provider requires 'cloudlab.username' and 'cloudlab.hostname_suffix'"
        )

    if not hasattr(cfg.cloudlab, "username") or not cfg.cloudlab.username:
        raise ValueError("Missing 'cloudlab.username' configuration parameter")

    if not hasattr(cfg.cloudlab, "hostname_suffix") or not cfg.cloudlab.hostname_suffix:
        raise ValueError("Missing 'cloudlab.hostname_suffix' configuration parameter")

    return CloudLabProvider(
        username=cfg.cloudlab.username, hostname_suffix=cfg.cloudlab.hostname_suffix
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
    if hasattr(cfg, "local"):
        return "local"

    # Future phases will add logic to detect other provider types
    # based on configuration parameters like:
    # - cfg.infrastructure.provider
    # - presence of aws/kubernetes configuration sections
    # - environment variables

    if (
        hasattr(cfg, "cloudlab")
        and cfg.cloudlab.username
        and cfg.cloudlab.hostname_suffix
    ):
        return "cloudlab"

    # For future phases, add detection logic like:
    # if hasattr(cfg, "infrastructure") and cfg.infrastructure.provider:
    #     return cfg.infrastructure.provider
    # if hasattr(cfg, "aws"):
    #     return "aws"
    # if hasattr(cfg, "kubernetes"):
    #     return "kubernetes"

    raise ValueError(
        "Unable to detect infrastructure provider type from configuration. "
        "Currently supported: CloudLab (requires cloudlab.username and cloudlab.hostname_suffix) "
        "or Local (requires local.home_dir)"
    )
