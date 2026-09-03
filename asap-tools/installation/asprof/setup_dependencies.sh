#!/bin/bash

sudo sysctl -w kernel.perf_event_paranoid=0
sudo sysctl kernel.kptr_restrict=0
