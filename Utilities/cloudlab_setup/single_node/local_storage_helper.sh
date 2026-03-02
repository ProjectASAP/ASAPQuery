#!/bin/bash

# check number of command line arguments
if [ $# -ne 1 ]; then
    echo "Usage: $0 <path_to_install>"
    exit 1
fi

ROOT=$1
USER=$(whoami)
GROUP=$(groups | awk '{print $1}')

sudo mkdir -p $ROOT
sudo /usr/local/etc/emulab/mkextrafs.pl -f $ROOT
sudo chown -R $USER:$GROUP $ROOT
rm -rf $ROOT/lo*
