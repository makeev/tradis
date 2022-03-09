#!/bin/bash
virtualenv .env --python=python3.9
source .env/bin/activate
pip install -r src/requirements.txt
