#!/bin/bash

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

sudo apt-get update
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt-get update
sudo apt-get install -y python3-pip ipython3 golang-go python3.11 htop
pip3 install --user pandas 'scipy==1.15' numpy matplotlib psutil loguru pyarrow hydra-core omegaconf
pip3 install --user plotnine

# Install Grafana Foundation SDK for dashboard configuration service
python3.11 -m pip install --user pandas 'scipy==1.15' numpy matplotlib psutil loguru pyarrow hydra-core omegaconf grafana-foundation-sdk requests 'promql-parser==0.5.0'

# For Rust fake exporter, cargo must be installed
# Install Rust if not already installed
echo "Installing Rust..."
if ! command -v rustc &>/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  source "$HOME/.cargo/env"
else
  echo "Rust already installed"
  source "$HOME/.cargo/env"
fi
# (cd "$THIS_DIR/../../experiments/fake_exporter_rust/fake_exporter"; cargo build --release)
(cd "$THIS_DIR/../../../PrometheusExporters/fake_exporter/fake_exporter_rust/fake_exporter" && cargo build --release)

DOCKER_DATA_DIR=/scratch/var_lib_docker

# Add Docker's official GPG key:
sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
#export VERSION_STRING="5:24.0.7-1~ubuntu.20.04~focal"
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" |
  sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update
#sudo apt-get install -y docker-ce=$VERSION_STRING docker-ce-cli=$VERSION_STRING containerd.io docker-buildx-plugin docker-compose-plugin libssl-dev make luarocks luajit
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin libssl-dev make luarocks luajit
sudo usermod -aG docker "$USER"
sudo mkdir -p /etc/docker && mkdir -p $DOCKER_DATA_DIR
echo '{ "data-root": "'$DOCKER_DATA_DIR'" }' | sudo tee /etc/docker/daemon.json
sudo service docker restart
