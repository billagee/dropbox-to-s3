#!/bin/bash -e

# Install the local copy of the package from the files in ./
python3 -m venv env
source env/bin/activate
pip install --editable .