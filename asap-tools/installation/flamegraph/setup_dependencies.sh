#!/bin/bash

sudo apt-get install -y linux-tools-common "linux-tools-$(uname -r)"
sudo sh -c 'echo -1 > /proc/sys/kernel/perf_event_paranoid'
