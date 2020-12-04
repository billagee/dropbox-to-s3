# dropbox-to-s3

Python env setup:

* Install Anaconda Individual Edition
* Create a Python 3.8 env
* Activate it with `. /opt/anaconda3/bin/activate && conda activate /opt/anaconda3`

Create a virtualenv in which the `drop2s3` command is installed:

    ./pip_install_editable.sh

Typical usage:

    drop2s3 --bucket-name YOUR_BUCKET --device YOUR_DEVICE_ID --year 2020 --month 09 workflow
