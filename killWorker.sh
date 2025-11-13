#!/bin/bash

pgrep -fl python | awk '$1{print $1}' | xargs kill -9

echo "killed $1"

sleep 5

osascript ./closeWorkerWindow.txt