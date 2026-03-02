#!/bin/bash

# function to untar a file
untar() {
    rm -rf $2
    mkdir $2
    tar -xvf $1 -C $2 --strip-components=1
}

set_property() {
    local config_file="$1"
    local property="$2"
    local value="$3"

    if grep -q "^$property=" "$config_file"; then
        # Property exists, update it
        sed -i "s|^$property=.*|$property=$value|g" "$config_file"
    else
        # Property doesn't exist, add it
        echo "$property=$value" >> "$config_file"
    fi
}
