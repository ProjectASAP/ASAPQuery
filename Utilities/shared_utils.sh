#!/bin/bash

load_components_config() {
    local components_conf_file="$1"
    if [ ! -f "$components_conf_file" ]; then
        echo "Error: Components configuration file not found at $components_conf_file" >&2
        exit 1
    fi
    grep -v '^#' "$components_conf_file" | grep -v '^$'
}

build_rsync_paths() {
    local this_dir="$1"
    local components=("${@:2}")
    local dirs_to_rsync=()

    for component in "${components[@]}"; do
        dirs_to_rsync+=("$this_dir/../../../$component")
    done

    printf '%s\n' "${dirs_to_rsync[@]}"
}

build_rsync_exclude_options() {
    local dirs=("$@")
    local rsync_exclude_options=()

    for dir in "${dirs[@]}"; do
        local rsyncignore_file="$dir/.rsyncignore"
        if [ -f "$rsyncignore_file" ]; then
            rsync_exclude_options+=("--exclude-from=$rsyncignore_file")
        fi
    done

    printf '%s\n' "${rsync_exclude_options[@]}"
}

parse_rsync_output() {
    local rsync_output="$1"
    shift
    local num_dirs=$(($# / 2))
    local dirs=("${@:1:$num_dirs}")
    local components=("${@:$((num_dirs+1)):$num_dirs}")
    local synced_components=()

    for ((i=0; i<${#dirs[@]}; i++)); do
        local dir="${dirs[$i]}"
        local component="${components[$i]}"
        local dir_basename=$(basename "$dir")

        if echo "$rsync_output" | grep -v ".git/" | grep -q "$dir_basename/"; then
            synced_components+=("$component")
        fi
    done

    printf '%s\n' "${synced_components[@]}"
}

perform_rsync() {
    local username="$1"
    local hostname="$2"
    local destination_dir="$3"
    local options="$4"
    shift 4
    local dirs_to_rsync=("$@")

    ssh $options $username@$hostname "mkdir -p $destination_dir"

    local rsync_exclude_options
    readarray -t rsync_exclude_options < <(build_rsync_exclude_options "${dirs_to_rsync[@]}")

    rsync -e "ssh $options" --omit-dir-times --itemize-changes -rltDzvh "${rsync_exclude_options[@]}" "${dirs_to_rsync[@]}" $username@$hostname:$destination_dir 1>&1
}
