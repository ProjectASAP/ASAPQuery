#!/bin/bash

#OPTIONS="-o StrictHostKeyChecking=no"
OPTIONS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
REMOTE_ROOT_VOLUME="/scratch"
REMOTE_ROOT_DIR=$REMOTE_ROOT_VOLUME"/sketch_db_for_prometheus/"
