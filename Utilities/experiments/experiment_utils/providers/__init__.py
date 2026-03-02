"""
Infrastructure provider package for experiment management.

This package contains the infrastructure provider abstraction layer that allows
experiments to run on different infrastructure types (CloudLab, AWS, Kubernetes, local).

The provider pattern abstracts away the underlying infrastructure details and
provides a consistent interface for:
- Command execution on nodes
- Node addressing and networking
- Path management
- Resource management

Usage:
    from experiment_utils.providers.factory import create_provider

    provider = create_provider(cfg)
    result = provider.execute_command(0, "ls -la", "/home/user")
"""

from .base import InfrastructureProvider
from .cloudlab import CloudLabProvider
from .factory import create_provider, detect_provider_type

__all__ = [
    "InfrastructureProvider",
    "CloudLabProvider",
    "create_provider",
    "detect_provider_type",
]
