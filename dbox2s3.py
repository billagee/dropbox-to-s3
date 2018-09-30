#!/usr/bin/env python

"""
TL;DR: This script moves image/video files out of
your "~/Dropbox/Camera Uploads/" dir and into an s3 bucket.

This is done by means of subcommands for a few different steps in the process:

  * Creating a local directory layout to hold your files
  * Moving files from Dropbox to the local dir layout
  * Syncing the local dir layout to s3

Subcommand details:

* dbox2s3 mkdir

  Creates a local directory layout to store your photos, with the intent
  that this directory can later be copied to s3. The default path looks like:

    ~/Pictures/s3/${BUCKET_NAME}/photos/${YEAR}/${MONTH}/${DEVICE}

  e.g.

    ~/Pictures/s3/mybucket/photos/2016/08/iPhone6s/

* dbox2s3 mv

  Moves files with a given extension from

    ~/Dropbox/Camera Uploads/

  into your local directory layout.

  Note that extensions recognized as video files will be moved into
  a "video" subdir of the local directory layout, e.g.,

    ~/Pictures/s3/${BUCKET_NAME}/photos/${YEAR}/${MONTH}/${DEVICE}/video

* dbox2s3 sync

  Syncs the local directory layout to an s3 bucket, e.g.

    ~/Pictures/s3/mybucket/photos/2016/08/iPhone6s/

  is copied to

    s3://mybucket/photos/2016/08/iPhone6s/

* dbox2s3 check

  Performs a consistency check to ensure the local and s3 layouts are identical

* TODO: For any files in s3 that are missing locally, download the bucket contents
  into the local directory layout

* NOTE: If you want to double-check consistency outside of this script,
  consider syncing the bucket to another dir (or machine), then diff that
  dir against the original local dir layout
"""

import boto3
import botocore
import click
import datetime
import glob
import os
from os.path import expanduser
from shutil import copy2

HOMEDIR = expanduser("~")
DROPBOX_CAMERA_UPLOADS_DIR = "{0}/Dropbox/Camera Uploads".format(HOMEDIR)

def validate_file_extension(ctx, param, value):
    if value not in ("jpg", "mov"):
        raise click.BadParameter("must be 'jpg' or 'mov'")
    else:
        return value

@click.command()
@click.option('--bucket', prompt='Bucket name',
              help='The s3 bucket to upload files to.')
@click.option('--year', prompt='Photo year',
              help='The year dir to operate on.')
# TODO - validate month with a callback
@click.option('--month', prompt='Photo month',
              help='The month dir to operate on.')
@click.option('--device', prompt='Device name', default="iPhone6s",
              help='The device name to include in the final dir layout path and bucket URL.')
@click.option('--ext', prompt='File extension', default='jpg',
              callback=validate_file_extension, expose_value=True, is_eager=True,
              help='File extension to operate on (jpg or mov).')
@click.option('--dryrun', prompt='Dry run?', type=click.BOOL, default=True,
              help='The s3 bucket to upload files to.')
def main(bucket, year, month, device, ext, dryrun):
    """Uploads files to BUCKET."""
    click.echo("Bucket: {0}, Year: {1}, Month: {2}, Device: {3}, File ext: {4}, Dryrun: {5}".format(
        bucket, year, month, device, ext, dryrun))
    s3 = boto3.resource('s3')

    # Walk ~/Dropbox/Camera Uploads and find all jpg and mov files matching
    # the year/month given. Print the list and ask if the user wants to move
    # them to the local non-Dropbox staging dir.
    dropbox_file_list = glob.glob(DROPBOX_CAMERA_UPLOADS_DIR + "/{0}-{1}*.{2}".format(year, month, ext))
    click.echo("Searching for files matching pattern in '{}'".format(
        DROPBOX_CAMERA_UPLOADS_DIR))
    if len(dropbox_file_list) is 0:
        click.echo("No files found; exiting.")
        exit(0)
    else:
        click.echo("Files in Dropbox that match your pattern:")
        for filename in dropbox_file_list:
            click.echo(click.style(filename, bg="blue"))

    LOCAL_S3_LAYOUT_BASE_DIR = "{0}/Pictures/s3/{1}/photos/{2}/{3}".format(HOMEDIR, bucket, year, month)
    DEVICE = "iPhone6s"
    LOCAL_S3_LAYOUT_DEVICE_DIR = LOCAL_S3_LAYOUT_BASE_DIR + "/" + DEVICE

    click.echo("Next we'll copy those files from Dropbox to your local s3 dir layout.")
    click.echo("The files will be placed in:")
    click.echo(click.style(LOCAL_S3_LAYOUT_DEVICE_DIR, bg="green"))
    click.confirm("Do you want to continue?", abort=True)

    # Copy the files
    dest = LOCAL_S3_LAYOUT_DEVICE_DIR
    if ext is "mov":
        dest = dest + "/video"
    if dryrun:
        click.echo("Would copy files to " + dest)
    else:
        if not os.path.isdir(dest):
            os.makedirs(dest)
        for filename in dropbox_file_list:
            copy2(filename, dest)

    # Prompt the user to ask if they want to delete the originals.
    # Double check that all the files to be deleted were successfully copied first!

    """
    try:
        s3.Bucket(BUCKET_NAME).upload_file('my_local_image.jpg', KEY)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            click.echo("The object does not exist.")
        else:
            raise
    """

if __name__ == '__main__':
    main()

"""
    # Define staging dir outside of dropbox for the month's photos and videos
    dst_dir_photos = "~/Pictures/s3/{0}/{1}/{2}/{3}".format(bucket, year, month, device)
    dst_dir_videos = dst_dir_photos + "/video"
    print("Will create and populate dir structure at '{0}'").format(dst_dir_videos)

    # Copy files to staging dir (w/ rsync?)

    # * If no rsync, checksum/diff the two sets of files
    #
    # * Ask user if they want to remove the files in the Cam Ups dir.
    #   (cmdline option set to false?)
    #
    # * Ask if they want to sync files to s3.
    #   (cmdline option set to false?)
    #
    # Perform additional checksum of files once they're in s3?

    """
"""
    echo "Local staging dir will be '$WORKING_DIR'"
    read -p "Does that look OK? [y/n]: " -r STAGING_DIR_CONFIRMATION
    if [[ $STAGING_DIR_CONFIRMATION =~ ^[Nn]$ ]]
    then
        box_out "Aborting as requested."
        exit 1
    fi

    # Create local working dir
    box_out "Creating '$WORKING_DIR'"
    mkdir -p $WORKING_DIR

    # Copy current month's files from Dropbox to local dir
    box_out "About to cd into '$WORKING_DIR'"
    cd $WORKING_DIR
    # Using 'find -print' is another way to list files in the src dir:
    #find ~/Dropbox/Camera\ Uploads -name "$TARGET_YEAR-$TARGET_MONTH-*.$FILE_EXTENSION" -print
    box_out "Checking whether files matching '$TARGET_YEAR-$TARGET_MONTH-*.$FILE_EXTENSION' exist in dropbox..."
    if [[ ! `ls -la ~/Dropbox/Camera\ Uploads/$TARGET_YEAR-$TARGET_MONTH-*.$FILE_EXTENSION` ]]
    then
        box_out "No files in dropbox dir to move! Exiting."
        exit 1
    fi

    ls -la ~/Dropbox/Camera\ Uploads/$TARGET_YEAR-$TARGET_MONTH-*.$FILE_EXTENSION
    box_out "About to move the above files to current dir:" "'$(pwd)'"

    read -p "OK to proceed? [y/n]: " -r MOVE_CONFIRMATION
    if [[ $MOVE_CONFIRMATION =~ ^[Yy]$ ]]
    then
        mv ~/Dropbox/Camera\ Uploads/$TARGET_YEAR-$TARGET_MONTH-*.$FILE_EXTENSION .
        box_out "mv completed."
    else
        box_out "Aborting as requested."
        exit 1
    fi

    # Print the dry run output of syncing local dir to s3
    box_out "Performing dry-run of s3cmd sync..."
    s3cmd sync --dry-run --exclude '.DS_Store' $LOCAL_BASE_DIR s3://$BUCKET_NAME/photos/$TARGET_YEAR/
    box_out "About to sync the files above to s3."

    read -p "OK? [y/n]: " -r SYNC_CONFIRMATION
    if [[ $SYNC_CONFIRMATION =~ ^[Yy]$ ]]
    then
        s3cmd sync --exclude '.DS_Store' $LOCAL_BASE_DIR s3://$BUCKET_NAME/photos/$TARGET_YEAR/
    else
        box_out "Aborting."
        exit 1
    fi

    # To sync the entire current year's files:
    #s3cmd sync --dry-run --exclude '.DS_Store' 2016 s3://$BUCKET_NAME/photos/
    #s3cmd sync --exclude '.DS_Store' 2016 s3://$BUCKET_NAME/photos/
    """
