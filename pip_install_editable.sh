#!/bin/bash -e

# Install the local copy of the package from the files in ./
virtualenv env
. env/bin/activate
pip install --editable .
