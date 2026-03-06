#!/bin/bash

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
source "$THIS_DIR/../install_components.sh"

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <component1> [<component2> ...]"
    exit 1
fi

setup_internal_components "$@"
