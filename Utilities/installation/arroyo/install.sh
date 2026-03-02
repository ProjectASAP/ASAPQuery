#!/bin/bash

set -e

ARROYO_IMAGE="ghcr.io/projectasap/asap-arroyo:v0.1.0"

echo "Pulling Arroyo Docker image..."
# sudo su - required because usermod -aG docker only takes effect in a new shell
sudo su - "$USER" -c "docker pull $ARROYO_IMAGE"
echo "Arroyo image ready: $ARROYO_IMAGE"
