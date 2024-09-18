# drop2s3

A CLI backup tool to copy photos and videos from your `Dropbox/Camera Uploads/` dir to an S3 bucket.

## Installation

Run this script to create a virtualenv and install `drop2s3` within it: 

    ./pip_install_editable.sh

Or to handle creation of the virtualenv and installation yourself:

    python3 -m venv env
    source env/bin/activate
    pip install --editable .

## Usage

    # Assumptions:
    # 
    # - You've authenticated to AWS with env vars, the AWS CLI, or ~/aws/credentials
    # - Your Dropbox folder is found at ~/Dropbox

    source env/bin/activate

    drop2s3 --bucket-name YOUR_BUCKET --device YOUR_DEVICE_NAME --year 2024 --month 02 workflow

## Details

* The utility is meant to be installed in a Python virtualenv and invoked with the `drop2s3` command.
* Backups are one-way (deleting files from Dropbox does not affect your backup)
* A local staging directory is created to build the directory structure before copying it to S3, rather than transferring the files directly from Dropbox to S3.

### Directory structure

Files from `Camera Uploads/` are sorted into directories following a `year/month/device/` pattern, while preserving the dropbox filename format:

```
~/Pictures/s3/YOURBUCKET/photos/
└── 2024
    └── 02
        └── iPhone14
            └── 2024 2024-02-06 20.51.38.heic
            └── 2024 2024-02-08 16.10.55.heic
            └── video
                └── 2024-02-01 17.34.02.mov
```
### 