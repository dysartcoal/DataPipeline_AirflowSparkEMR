#!/bin/bash
mkdir -p $HOME/python_apps
aws s3 cp s3://dysartcoal-dend-uswest2/capstone_etl/python_apps  $HOME/python_apps --recursive --exclude "*" --include "*.py" 
