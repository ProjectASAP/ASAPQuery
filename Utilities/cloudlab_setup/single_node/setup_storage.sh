#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <username> <hostname>"
    exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "$0")")
source $THIS_DIR"/constants.sh"

USERNAME=$1
HOSTNAME=$2
LOCAL_FILE_NAME="local_storage_helper.sh"

scp $OPTIONS $THIS_DIR"/"$LOCAL_FILE_NAME $USERNAME@$HOSTNAME:~/
ssh $OPTIONS $USERNAME@$HOSTNAME "cd ~; ./$LOCAL_FILE_NAME $REMOTE_ROOT_VOLUME"
