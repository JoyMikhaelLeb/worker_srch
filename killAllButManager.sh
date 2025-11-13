#!/bin/bash

ps -ef | grep 'workers' | grep -v grep | awk '{print $2}' | xargs -r kill -SIGINT
ps -ef | grep 'Google\|google\|chrome' | grep -v grep | awk '{print $2}' | xargs -r kill -15