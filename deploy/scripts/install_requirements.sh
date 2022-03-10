#!/bin/bash
cd /home/tradis/last_revision
pwd
virtualenv .env --python=python3.9
source .env/bin/activate
pip install -U -r src/requirements.txt
