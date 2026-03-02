#!/bin/bash

source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/shared_utils.sh"

install_components() {
    local setup_dependencies="$1"
    shift
    local components=("$@")

    local this_dir=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
    local root_code_dir=$(dirname "$this_dir")

    for component in "${components[@]}"; do
        local component_dir="$root_code_dir/$component/installation"
        if [ ! -d "$component_dir" ]; then
            echo "Error: Component directory $component_dir does not exist."
            continue
        fi

        if [ "$setup_dependencies" = true ] && [ -f "$component_dir/setup_dependencies.sh" ]; then
            (source "$component_dir/setup_dependencies.sh")
        fi

        if [ -f "$component_dir/install.sh" ]; then
            (source "$component_dir/install.sh")
        fi
    done
}

install_internal_components() {
    local components=("$@")
    local predefined_components
    local this_dir=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
    local components_conf_file="$this_dir/components.conf"
    readarray -t predefined_components < <(load_components_config "$components_conf_file")

    if [ "${components[0]}" == "all" ]; then
        components=("${predefined_components[@]}")
    fi

    install_components false "${components[@]}"
}

setup_internal_components() {
    local components=("$@")
    local predefined_components
    local this_dir=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
    local components_conf_file="$this_dir/components.conf"
    readarray -t predefined_components < <(load_components_config "$components_conf_file")

    if [ "${components[0]}" == "all" ]; then
        components=("${predefined_components[@]}")
    fi

    install_components true "${components[@]}"
}
