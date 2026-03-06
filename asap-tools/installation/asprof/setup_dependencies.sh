#!/bin/bash

sudo sysctl kernel.perf_event_paranoid=1
sudo sysctl kernel.kptr_restrict=0
