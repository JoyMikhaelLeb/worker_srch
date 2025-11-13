#!/bin/bash

git pull $1 -X theirs
python localManager.py