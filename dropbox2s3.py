# -*- coding: utf-8 -*-
 
import boto3
import botocore
import click
from datetime import datetime
import filecmp
import glob
import itertools
import os
import pandas as pd
import pathlib
import sqlite3
import sys
from os.path import expanduser
from os.path import splitext
from pathlib import Path
from shutil import copy2

"""
* TODO: For any files in s3 that are missing locally, download the bucket contents
into the local directory layout

* NOTE: If you want to double-check consistency outside of this script,
consider syncing the bucket to another dir (or machine), then diff that
dir against the original local working dir
"""

class BackupContext(object):
    def __init__(self, bucket_name, year, month, device):
        self.db = sqlite3.connect(':memory:')
        self.dbcursor = self.db.cursor()
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket_name)
        self.homedir = expanduser("~")
        self.bucket_name = bucket_name
        self.year = year
        self.month = month
        self.device = device
        self.dir_prefix = "photos/{year}/{month}/{device}".format(
            year=self.year,
            month=self.month,
            device=self.device)
        self.dropbox_camera_uploads_dir = pathlib.Path(
            "{0}/Dropbox/Camera Uploads/".format(self.homedir))
        # e.g., ~/Pictures/s3/mybucket/photos/2016/08/iPhone6s/
        self.local_working_dir = pathlib.Path(
            "{homedir}/Pictures/s3/{bucket}/{dir_prefix}/".format(
                homedir=self.homedir,
                bucket=self.bucket_name,
                dir_prefix=self.dir_prefix))
        # The file extensions that we'll operate on
        self.supported_file_extensions = ["jpg", "mov"]
        self.video_file_extensions = [".mov"]

        # Walk Dropbox and working dir paths and find all files matching
        # the given year/month.
        self.dropbox_file_paths = []
        self.working_dir_file_paths = []
        for file_ext in self.supported_file_extensions:
            pattern = "**/{}-{}-*.{}".format(self.year, self.month, file_ext)
            self.dropbox_file_paths.extend(
                sorted(self.dropbox_camera_uploads_dir.glob(pattern)))
            self.working_dir_file_paths.extend(
                sorted(self.local_working_dir.glob(pattern)))
        self.dropbox_filenames = [x.name for x in self.dropbox_file_paths]
        self.working_dir_filenames = [x.name for x in self.working_dir_file_paths]

        # Get bucket contents for year/month/device
        self.bucket_file_paths = [
            obj.key
            for obj
            in self.bucket.objects.filter(Prefix=self.dir_prefix)]
        self.bucket_filenames = [
            x.split("/")[-1] for x in self.bucket_file_paths]
        # Populate DB
        self.init_db()

    def init_db(self):
        self.dbcursor.execute('''CREATE TABLE files (
            Filename TEXT PRIMARY KEY,
            InDropbox INTEGER DEFAULT 0,
            InWorkingDir INTEGER DEFAULT 0,
            InS3 INTEGER DEFAULT 0)''')
        for file_name in self.dropbox_filenames:
            self.do_upsert_true_value_for_column(
                file_name=file_name, column="InDropbox")
        for file_name in self.working_dir_filenames:
            self.do_upsert_true_value_for_column(
                file_name=file_name, column="InWorkingDir")
        for file_name in self.bucket_filenames:
            self.do_upsert_true_value_for_column(
                file_name=file_name, column="InS3")

    def do_upsert_true_value_for_column(self, file_name, column):
        self.dbcursor.execute("""
            INSERT INTO files (Filename, {column})
            VALUES ('{file_name}', 1)
            ON CONFLICT (Filename)
            DO UPDATE SET {column} = 1 WHERE Filename = '{file_name}'""".format(
                file_name=file_name, column=column))
        self.db.commit()

    def __repr__(self):
        return '<BackupContext %r>' % self.local_working_dir

pass_backup_context = click.make_pass_decorator(BackupContext)

@click.group()
@click.option('--bucket-name', prompt='Bucket name',
              help='The s3 bucket name to upload files to.')
@click.option('--year', prompt='Photo year', default="{:02}".format(datetime.today().year),
              help='The year dir to use in the working dir path (e.g. 2017).')
@click.option('--month', prompt='Photo month', default="{:02}".format(datetime.today().month),
              help='The month dir to use in the working dir path (e.g. 09).')
@click.option('--device', prompt='Device name', default="iPhone6s",
              help='The device name to use in the working dir path.')
@click.pass_context
def cli(ctx, bucket_name, year, month, device):
    """
    This utility copies image/video files from ~/Dropbox/Camera Uploads/
    into a local working dir, then syncs the working dir to an s3 bucket.

    Example workflow:\n
      dropbox2s3 mkdir\n
      dropbox2s3 cp\n
      dropbox2s3 sync\n
      dropbox2s3 rm-dropbox-files
    """
    # Create a BackupContext object and remember it as as the context object.
    # From this point onwards other commands can refer to it by using the
    # @pass_backup_context decorator.
    ctx.obj = BackupContext(bucket_name, year, month, device)

@cli.command()
@pass_backup_context
def mkdir(backup_context):
    """Creates local working dir to copy Dropbox files to.
    will be copied from your Dropbox/Camera Uploads/
    dir. The working dir can then be synced to an s3 bucket.
    """
    if not os.path.exists(backup_context.local_working_dir):
        click.echo("About to create working dir at {}".format(backup_context.local_working_dir))
        click.confirm("Do you want to continue?", abort=True)
        # Append /video to working dir path since videos are stored separately
        os.makedirs(backup_context.local_working_dir + "/video")
    else:
        click.echo("Doing nothing; working dir already exists at {}".format(
            backup_context.local_working_dir))

@cli.command()
@pass_backup_context
@click.option('--dryrun', prompt='Dry run?', type=click.BOOL, default=True,
              help='Do not actually copy files.')
def cp(backup_context, dryrun):
    """Copy files from Dropbox to working dir.

    Note that files with video extensions will be copied into
    a "video" subdir of the working dir.
    """
    # Handle images
    """
    if len(backup_context.dropbox_file_paths_jpg) is 0:
        click.echo("No image files found; not copying any images.")
        #sys.exit(0)
    else:
        click.echo("Image files in Dropbox dir that match your pattern:")
        for filename in backup_context.dropbox_file_paths_jpg:
            click.echo("'{}'".format(click.style(filename, bg="blue")))
    # Handle videos
    click.echo("Searching for video files matching pattern in '{}'".format(
        backup_context.dropbox_camera_uploads_dir))
    if len(backup_context.dropbox_file_paths_mov) is 0:
        click.echo("No video files found; not copying any videos.")
        #sys.exit(0)
    else:
        click.echo("Video files in Dropbox dir that match your pattern:")
        for filename in backup_context.dropbox_file_paths_mov:
            click.echo("'{}'".format(click.style(filename, bg="blue")))
    """

    click.echo("About to copy files from: {}".format(
        backup_context.dropbox_camera_uploads_dir))
    click.echo("To local working dir: {}".format(
        backup_context.local_working_dir))
    dest_images = backup_context.local_working_dir
    dest_videos = backup_context.local_working_dir + "video"
    #if not os.path.isdir(dest_videos):
    #    click.echo("Exiting - Local working dir doesn't exist at {}".format(
    #        backup_context.local_working_dir))
    #    sys.exit(1)

    # Save list of filenames in working dir
    #workdir_filenames = [x.split("/")[-1] for x in backup_context.working_dir_file_paths]
    # Walk list of dropbox abs paths
    for dropbox_file_abspath in backup_context.dropbox_file_paths:
        dropbox_filename = dropbox_file_abspath.split("/")[-1]
        if dropbox_filename in self.working_dir_filenames:
            click.echo("Skipping file '{}'; it already exists in workdir".format(dropbox_filename))
            continue
        # Set destination dir
        if os.path.splitext(dropbox_filename)[1] in backup_context.video_file_extensions:
            dest_root = dest_videos
        else:
            dest_root = dest_images
        if dryrun:
            click.echo("Dry run; would have copied '{}' to {}".format(
                dropbox_filename, dest_root))
        else:
            click.echo("Copying '{}' to {}".format(dropbox_filename, dest_root))
            copy2(dropbox_file_abspath, dest_root)

@cli.command()
@pass_backup_context
@click.option('--dryrun', prompt='Dry run?', type=click.BOOL, default=True,
              help='Do not actually delete files.')
def rm_dropbox_files(backup_context, dryrun):
    """Delete backed-up files in your Camera Uploads dir.
    """
    # Double check that all the files to be deleted in Dropbox have been
    # successfully copied to the working dir:
    click.echo("Checking files in working dir against Dropbox dir...")
    for dropbox_filename_jpg in backup_context.dropbox_filenames_jpg:
        if dropbox_filename_jpg not in backup_context.working_dir_filenames_jpg:
            # TODO - also diff the two files
            click.echo("ERROR: Dropbox file {} not found in local working dir!".format(dropbox_filename))
            click.echo("Aborting clean; please run the cp command before continuing.")
            sys.exit(1)
        else:
            click.echo("Found file {}{}".format(backup_context.local_working_dir, dropbox_filename_jpg))
            if dryrun:
                click.echo("Exiting due to dry run; would have deleted Dropbox file '{}'".format(
                    dropbox_filename_jpg))
                sys.exit(0)
            else:
                click.echo("deleting {}/{}".format(
                    backup_context.dropbox_camera_uploads_dir, dropbox_filename_jpg))
                #os.remove(dropbox_file_path)

@cli.command()
@pass_backup_context
@click.option('--dryrun', prompt='Dry run?', type=click.BOOL, default=True,
              help='The s3 bucket to upload files to.')
def sync(backup_context):
    """Syncs local working dir to an s3 bucket. For example:

    ~/Pictures/s3/mybucket/photos/2016/08/iPhone6s/

    will be copied to

    s3://mybucket/photos/2016/08/iPhone6s/
    """
    """
    try:
        s3.Bucket(BUCKET_NAME).upload_file('my_local_image.jpg', KEY)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            click.echo("The object does not exist.")
        else:
    """

@cli.command()
@pass_backup_context
def difflocal(backup_context):
    """Diff Dropbox and working dir contents.
    """
    fmt = '{:<4}{:<8}{:<40}{}'
    # Print header row
    print(fmt.format(
        '',
        'Diff',
        backup_context.dropbox_camera_uploads_dir,
        backup_context.local_working_dir))
    # Create data structure that looks like
    abspath_tuples = itertools.zip_longest(
        backup_context.dropbox_file_paths,
        backup_context.working_dir_file_paths,
        fillvalue="-")
    #import pdb ; pdb.set_trace()
    # Print both dirs' contents side by side
    for i, (dropbox_file_abspath, working_dir_file_abspath) in enumerate(abspath_tuples):
        dropbox_filename = dropbox_file_abspath.split("/")[-1]
        workdir_filename = working_dir_file_abspath.split("/")[-1]
        # Compare filenames and their contents
        if dropbox_filename == workdir_filename:
            if filecmp.cmp(dropbox_file_abspath, working_dir_file_abspath, shallow=False):
                print(fmt.format(i, "👍", dropbox_filename, workdir_filename))
            else:
                print(fmt.format(i, "❌", dropbox_filename, workdir_filename))
                print("ERROR! Files have the same name, but contents differ.")
        else:
            # Filenames differ
            #click.echo(fmt.format(
            #    i, "❌", click.style(dropbox_filename, bg='red'), click.style(workdir_filename, bg='red')))
            if dropbox_filename in backup_context.intersection_of_dropbox_and_workdir_filenames:
                click.echo(fmt.format(
                    i, "❌", click.style("", bg='red'), click.style(workdir_filename, bg='red')))
            else:
                click.echo(fmt.format(
                    i, "❌", click.style(dropbox_filename, bg='red'), click.style("", bg='red')))

@cli.command()
@pass_backup_context
def diffbucket(backup_context):
    """Diff working dir and bucket contents.
    """
    fmt = '{:<4}{:<8}{:<40}{}'
    # Print header row
    print(fmt.format(
        '',
        'Diff',
        backup_context.bucket_name,
        backup_context.local_working_dir))
    abspath_tuples = zip(
        backup_context.bucket_file_paths,
        backup_context.working_dir_file_paths)
    # Print both dirs' contents side by side
    for i, (bucket_file_abspath, working_dir_file_abspath) in enumerate(abspath_tuples):
        bucket_filename = bucket_file_abspath.split("/")[-1]
        workdir_filename = working_dir_file_abspath.split("/")[-1]
        # Compare filenames and their contents
        #if dropbox_filename == workdir_filename and filecmp.cmp(dropbox_file_abspath, working_dir_file_abspath, shallow=False):
        if bucket_filename == workdir_filename:
            print(fmt.format(i, "👍", bucket_filename, workdir_filename))
        else:
            # Filenames differ
            click.echo(fmt.format(
                i, "❌", click.style(bucket_filename, bg='red'), click.style(workdir_filename, bg='red')))
 
@cli.command()
@pass_backup_context
def lsbucket(backup_context):
    """Print s3 bucket contents for given year/month/device.
    """
    for key in backup_context.bucket_file_paths:
        print(key)
 
@cli.command()
@pass_backup_context
def lsdb(backup_context):
    """Populate and print db contents for given year/month/device.
    """
    #backup_context.dbcursor.execute('''
    #    SELECT * FROM files''')
    #from pprint import pprint
    #pprint(backup_context.dbcursor.fetchall())
    pd.set_option('display.max_rows', 500)
    print(pd.read_sql_query("SELECT * FROM files", backup_context.db))

@cli.command()
@pass_backup_context
def lsdropbox(backup_context):
    """Print Dropbox contents for given year/month/device.
    """
    for path in backup_context.dropbox_file_paths:
        print(path)
    for filename in backup_context.dropbox_filenames:
        print(filename)

@cli.command()
@pass_backup_context
def lsworkdir(backup_context):
    """Print working dir contents for given year/month/device.
    """
    for path in backup_context.working_dir_file_paths:
        print(path)
    for filename in backup_context.working_dir_filenames:
        print(filename)
