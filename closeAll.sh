#!/bin/bash


ps -ef | grep 'workers' | grep -v grep | awk '{print $2}' | xargs -r kill -SIGINT
ps -ef | grep 'Google\|google\|chrome' | grep -v grep | awk '{print $2}' | xargs -r kill -15

echo "killed all processes, waiting for windows to close..."

sleep 5	

pgrep -fl python | awk '!/localManager\.py/{print $1}' | xargs kill -15

osascript ./closeWindows.txt