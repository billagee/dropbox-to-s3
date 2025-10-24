# -*- coding: utf-8 -*-

"""
drop2s3 - Backup utility for copying images/videos from Dropbox to S3.

This utility copies image/video files from ~/Dropbox/Camera Uploads/
into a local working directory, then uploads the files to an S3 bucket.

TODO: For any files in S3 that are missing in local workdir,
download the bucket contents into the local directory layout

NOTE: If you want to double-check consistency, consider syncing
your bucket to another dir (or machine), then diff that dir against
the original local working dir
"""

import os
import re
import sys
import filecmp
import sqlite3
from datetime import datetime
from pathlib import Path
from shutil import copy2
from typing import List, Optional, Dict, Set

import boto3
import click
import pandas as pd

# Configuration constants
SUPPORTED_FILE_EXTENSIONS = ["jpg", "png", "mov", "3gp", "heic", "mp4"]
VIDEO_FILE_EXTENSIONS = [".mov", ".3gp", ".mp4"]
DROPBOX_CAMERA_DIR = "Dropbox/Camera Uploads/"
LOCAL_PICTURES_DIR = "Pictures/s3/"
S3_PREFIX_TEMPLATE = "photos/{year}/{month}/{device}/"


def detect_year_month_combinations(dropbox_dir: Path) -> List[Dict[str, str]]:
    """
    Scan Dropbox Camera Uploads directory and detect all year/month combinations.

    Args:
        dropbox_dir: Path to Dropbox Camera Uploads directory

    Returns:
        List of dictionaries with 'year' and 'month' keys, sorted by year and month
    """
    year_month_set: Set[tuple] = set()

    # Pattern to match Dropbox filename format: YYYY-MM-DD-*
    # Example: 2024-10-15-IMG_001.jpg
    date_pattern = re.compile(r'^(\d{4})-(\d{2})-\d{2}')

    if not dropbox_dir.exists():
        click.echo(f"Warning: Dropbox directory not found at {dropbox_dir}")
        return []

    # Scan all files in the directory
    for file_path in dropbox_dir.glob('**/*'):
        if file_path.is_file():
            match = date_pattern.match(file_path.name)
            if match:
                year, month = match.groups()
                year_month_set.add((year, month))

    # Convert to sorted list of dictionaries
    sorted_combinations = sorted(year_month_set)
    return [{'year': year, 'month': month} for year, month in sorted_combinations]


def prompt_user_for_year_month(combinations: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """
    Present detected year/month combinations to user and get their selection.

    Args:
        combinations: List of year/month combination dictionaries

    Returns:
        Selected year/month dictionary, or None if cancelled
    """
    if not combinations:
        click.echo("No files with year/month format found in Dropbox Camera Uploads.")
        return None

    click.echo("\nDetected the following year/month combinations in Dropbox Camera Uploads:\n")

    for idx, combo in enumerate(combinations, 1):
        click.echo(f"  {idx}. {combo['year']}-{combo['month']}")

    click.echo()

    # Prompt user for selection
    while True:
        choice = click.prompt(
            "Enter the number of the year/month to process (or 'q' to quit)",
            type=str
        )

        if choice.lower() == 'q':
            return None

        try:
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(combinations):
                selected = combinations[choice_idx]
                click.echo(f"\nSelected: {selected['year']}-{selected['month']}")

                # Ask for confirmation
                if click.confirm("\nProceed with copying these files to S3?"):
                    return selected
                else:
                    click.echo("Operation cancelled.")
                    return None
            else:
                click.echo(f"Please enter a number between 1 and {len(combinations)}")
        except ValueError:
            click.echo("Invalid input. Please enter a number or 'q' to quit.")


class DatabaseManager:
    """Manages the SQLite database for tracking file locations."""

    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()
        self._init_schema()

    def _init_schema(self):
        """Initialize the database schema."""
        self.cursor.execute("DROP TABLE IF EXISTS files")
        self.cursor.execute(
            """CREATE TABLE files (
            Filename TEXT PRIMARY KEY,
            InDropbox INTEGER DEFAULT 0,
            InWorkingDir INTEGER DEFAULT 0,
            InS3 INTEGER DEFAULT 0)"""
        )
        self.db.commit()

    def upsert_file_location(self, file_name: str, column: str):
        """
        Insert or update a file's location status.

        Args:
            file_name: Name of the file
            column: Column to update (InDropbox, InWorkingDir, or InS3)
        """
        # Use parameterized query to prevent SQL injection
        query = f"""
            INSERT INTO files (Filename, {column})
            VALUES (?, 1)
            ON CONFLICT (Filename)
            DO UPDATE SET {column} = 1 WHERE Filename = ?
        """
        self.cursor.execute(query, (file_name, file_name))
        self.db.commit()

    def get_file_row(self, file_name: str) -> Optional[sqlite3.Row]:
        """
        Get database row for a specific file.

        Args:
            file_name: Name of the file to query

        Returns:
            Database row or None if not found
        """
        query = "SELECT * FROM files WHERE Filename = ?"
        return self.cursor.execute(query, (file_name,)).fetchone()

    def execute_query(self, query: str):
        """Execute a SQL query and return the cursor."""
        return self.cursor.execute(query)


class BackupContext:
    """Context object for managing backup operations between Dropbox and S3."""

    def __init__(self, bucket_name: str, year: str, month: str, device: str):
        # Core configuration
        self.bucket_name = bucket_name
        self.year = year
        self.month = month
        self.device = device
        self.homedir = Path.home()

        # S3 setup
        self.s3 = boto3.resource("s3")
        self.bucket = self.s3.Bucket(bucket_name)
        self.dir_prefix = S3_PREFIX_TEMPLATE.format(
            year=self.year, month=self.month, device=self.device
        )

        # Directory paths
        self.dropbox_camera_uploads_dir = self.homedir / DROPBOX_CAMERA_DIR
        self.local_bucket_dir = self.homedir / LOCAL_PICTURES_DIR / self.bucket_name
        self.local_working_dir = self.local_bucket_dir / self.dir_prefix

        # File extension configuration
        self.supported_file_extensions = SUPPORTED_FILE_EXTENSIONS
        self.video_file_extensions = VIDEO_FILE_EXTENSIONS

        # Initialize database
        self.db_manager = DatabaseManager()
        self.db = self.db_manager.db  # For backward compatibility
        self.dbcursor = self.db_manager.cursor  # For backward compatibility

        self.init_db()

    def get_glob_pattern(self, file_ext: str) -> str:
        """
        Get the glob pattern for finding files based on device and file extension.

        Args:
            file_ext: File extension to search for

        Returns:
            Glob pattern string
        """
        if self.device == "NikonCoolpix":
            return f"**/*DSCN*.{file_ext.upper()}"
        return f"**/{self.year}-{self.month}-*.{file_ext}"

    def get_file_destination_path(self, filename: str, base_dir: Path) -> Path:
        """
        Get the destination path for a file (handles video subdirectory).

        Args:
            filename: Name of the file
            base_dir: Base directory path

        Returns:
            Full destination path for the file
        """
        file_ext = os.path.splitext(filename)[1]
        if file_ext in self.video_file_extensions:
            return base_dir / "video" / filename
        return base_dir / filename

    def init_db(self):
        """
        Initialize the database by scanning Dropbox, working directory, and S3.

        Populates the database with file locations from all three sources.
        """
        # Scan local directories for files matching year/month pattern
        self.dropbox_filenames = self._scan_directory(self.dropbox_camera_uploads_dir)
        self.working_dir_filenames = self._scan_directory(self.local_working_dir)

        # Get S3 bucket contents for year/month/device
        self.bucket_file_paths = [
            obj.key
            for obj in self.bucket.objects.filter(Prefix=self.dir_prefix)
            if Path(obj.key).suffix != ""
        ]
        self.bucket_filenames = [Path(x).name for x in self.bucket_file_paths]

        # Populate database with file locations
        for file_name in self.dropbox_filenames:
            self.db_manager.upsert_file_location(file_name, "InDropbox")
        for file_name in self.working_dir_filenames:
            self.db_manager.upsert_file_location(file_name, "InWorkingDir")
        for file_name in self.bucket_filenames:
            self.db_manager.upsert_file_location(file_name, "InS3")

    def _scan_directory(self, directory: Path) -> List[str]:
        """
        Scan a directory for files matching supported extensions.

        Args:
            directory: Directory path to scan

        Returns:
            List of filenames found
        """
        filenames = []
        for file_ext in self.supported_file_extensions:
            pattern = self.get_glob_pattern(file_ext)
            file_glob = directory.glob(pattern)
            filenames.extend([x.name for x in sorted(file_glob)])
        return filenames

    def get_file_db_row(self, file_name: str) -> Optional[sqlite3.Row]:
        """
        Get database row for a specific file.

        Args:
            file_name: Name of the file to query

        Returns:
            Database row or None if not found
        """
        return self.db_manager.get_file_row(file_name)

    def mkdir(self):
        """Create the local working directory if it doesn't exist."""
        if not self.local_working_dir.exists():
            click.echo(f"About to create working dir at {self.local_working_dir}")
            click.confirm("Do you want to continue?", abort=True)
            # Create working dir and video subdirectory
            (self.local_working_dir / "video").mkdir(parents=True, exist_ok=True)
        else:
            click.echo(f"Working dir already exists at {self.local_working_dir}")

    def __repr__(self):
        return "<BackupContext %r>" % self.local_working_dir


pass_backup_context = click.make_pass_decorator(BackupContext)


@click.group()
@click.option(
    "--bucket-name", prompt="Bucket name", help="The s3 bucket name to upload files to."
)
@click.option(
    "--year",
    default=None,
    help="The year dir to use in the working dir path (e.g. 2017). If not provided, will auto-detect from Dropbox files.",
)
@click.option(
    "--month",
    default=None,
    help="The month dir to use in the working dir path (e.g. 09). If not provided, will auto-detect from Dropbox files.",
)
@click.option(
    "--device",
    prompt="Device name",
    default="default",
    help="The device name to use in the working dir path (e.g. 'iPhone15').",
)
@click.pass_context
def cli(ctx, bucket_name, year, month, device):
    """
    This utility copies image/video files from ~/Dropbox/Camera Uploads/
    into a local working dir, then uploads the files to an s3 bucket.

    If --year and --month are not provided, the tool will automatically scan
    your Dropbox Camera Uploads directory, detect available year/month combinations,
    and present them for you to choose.

    Example usage:\n
      drop2s3 workflow  # Auto-detects year/month\n
      drop2s3 --year 2024 --month 10 workflow  # Explicitly specify year/month\n
      drop2s3 diffbucket\n
      drop2s3 rm-dropbox-files
    """
    # Auto-detect year/month if not provided
    if year is None or month is None:
        dropbox_dir = Path.home() / DROPBOX_CAMERA_DIR
        combinations = detect_year_month_combinations(dropbox_dir)

        if not combinations:
            click.echo("No files with year/month format found in Dropbox Camera Uploads.")
            click.echo("Please specify --year and --month parameters explicitly.")
            ctx.abort()

        selected = prompt_user_for_year_month(combinations)

        if selected is None:
            click.echo("No selection made. Exiting.")
            ctx.abort()

        year = selected['year']
        month = selected['month']

    # Create a BackupContext object and remember it as as the context object.
    # From this point onwards other commands can refer to it by using the
    # @pass_backup_context decorator.
    ctx.obj = BackupContext(bucket_name, year, month, device)


@cli.command()
@pass_backup_context
def mkdir(backup_context):
    """Creates local working dir to copy Dropbox files to.
    will be copied from your Dropbox/Camera Uploads/ dir.
    The working dir files can then be uploaded to an s3 bucket.
    """
    backup_context.mkdir()


@cli.command()
@pass_backup_context
@click.option(
    "--dryrun",
    prompt="Dry run?",
    type=click.BOOL,
    default=True,
    help="Do not actually copy files.",
)
def cp(backup_context, dryrun):
    """Copy files from Dropbox to working dir.

    Note that files with video extensions will be copied into
    a "video" subdir of the working dir.
    """
    # Check for working dir and run mkdir() if it doesn't exist
    backup_context.mkdir()
    click.echo(f"About to copy files from: {backup_context.dropbox_camera_uploads_dir}")
    click.echo(f"To local working dir: {backup_context.local_working_dir}")

    for dropbox_file_name in backup_context.dropbox_filenames:
        file_row = backup_context.get_file_db_row(dropbox_file_name)
        if file_row["InWorkingDir"]:
            click.echo(f"Skipping file '{dropbox_file_name}'; it already exists in workdir")
            continue

        # Get destination path using helper method
        dest_path = backup_context.get_file_destination_path(
            dropbox_file_name, backup_context.local_working_dir
        )

        if dryrun:
            click.echo(f"Dry run; would have copied '{dropbox_file_name}' to workdir")
        else:
            click.echo(f"Copying '{dropbox_file_name}' to {dest_path.parent}")
            copy2(backup_context.dropbox_camera_uploads_dir / dropbox_file_name, dest_path)


@cli.command()
@pass_backup_context
@click.option(
    "--dryrun",
    prompt="Dry run?",
    type=click.BOOL,
    default=True,
    help="Do not actually delete files.",
)
def rm_dropbox_files(backup_context, dryrun):
    """Delete backed-up files in your Camera Uploads dir."""
    # Verify files exist in both working dir and S3 before deletion
    click.echo("Checking for Dropbox files in workdir and s3...")
    for dropbox_file_name in backup_context.dropbox_filenames:
        file_row = backup_context.get_file_db_row(dropbox_file_name)
        in_workdir = file_row["InWorkingDir"]
        in_s3 = file_row["InS3"]

        dropbox_file_path = backup_context.dropbox_camera_uploads_dir / dropbox_file_name
        workdir_file_path = backup_context.get_file_destination_path(
            dropbox_file_name, backup_context.local_working_dir
        )

        if in_workdir and in_s3:
            # Verify file integrity before deletion
            if filecmp.cmp(dropbox_file_path, workdir_file_path, shallow=False):
                if dryrun:
                    click.echo(f"[dry run] would have deleted Dropbox file '{dropbox_file_name}'")
                else:
                    click.echo(f"Deleting {dropbox_file_path}...")
                    os.remove(dropbox_file_path)
            else:
                click.echo(
                    f"Error: Files differ! Dropbox: '{dropbox_file_name}', "
                    f"Workdir: '{workdir_file_path}'"
                )
                click.echo("Aborting clean; please investigate before continuing.")
                sys.exit(1)
        else:
            click.echo(
                f"Skipping rm of Dropbox file '{dropbox_file_name}'; "
                "it's not present in both workdir and s3..."
            )
            click.echo("Please run the upload command to back the file up in s3 first.")


@cli.command()
@pass_backup_context
def difflocal(backup_context):
    """Diff Dropbox and working dir contents."""
    fmt = "{:<40}{:<20}"
    print(fmt.format("File name", "Status"))
    query = "SELECT * FROM files ORDER BY Filename"
    for row in backup_context.db_manager.execute_query(query):
        filename = row["Filename"]
        in_dropbox = row["InDropbox"]
        in_workdir = row["InWorkingDir"]
        in_s3 = row["InS3"]

        dropbox_file_path = backup_context.dropbox_camera_uploads_dir / filename
        workdir_file_path = backup_context.get_file_destination_path(
            filename, backup_context.local_working_dir
        )

        if in_dropbox == 1 and in_workdir == 1:
            if filecmp.cmp(dropbox_file_path, workdir_file_path, shallow=False):
                print(fmt.format(filename, "diff OK"))
            else:
                print(fmt.format(filename, "diff NOT OK - files differ!"))
        elif in_dropbox == 1 and in_workdir == 0:
            click.secho(fmt.format(filename, "dropbox only"), bg="red", fg="white")
        elif in_dropbox == 0 and in_workdir == 0 and in_s3 == 1:
            click.secho(fmt.format(filename, "s3 only"), bg="blue", fg="white")


@cli.command()
@pass_backup_context
def diffbucket(backup_context):
    """Diff working dir and s3 bucket contents."""
    fmt = "{:<40}{:<20}"
    print(fmt.format("File name", "Status"))
    query = "SELECT * FROM files ORDER BY Filename"
    for row in backup_context.db_manager.execute_query(query):
        filename = row["Filename"]
        in_dropbox = row["InDropbox"]
        in_workdir = row["InWorkingDir"]
        in_s3 = row["InS3"]

        if in_workdir == 1 and in_s3 == 1:
            print(fmt.format(filename, "found in s3 & workdir"))
        elif in_workdir == 1 and in_s3 == 0:
            click.secho(fmt.format(filename, "workdir only"), bg="red", fg="white")
        elif in_workdir == 0 and in_s3 == 1:
            click.secho(fmt.format(filename, "s3 only"))
        elif in_workdir == 0 and in_s3 == 0 and in_dropbox == 1:
            click.secho(fmt.format(filename, "dropbox only"), bg="red", fg="white")


@cli.command()
@pass_backup_context
@click.option(
    "--dryrun",
    prompt="Dry run?",
    type=click.BOOL,
    default=True,
    help="Do not actually upload files.",
)
def upload(backup_context, dryrun):
    """Uploads local working dir files to an s3 bucket."""
    for workdir_filename in backup_context.working_dir_filenames:
        file_row = backup_context.get_file_db_row(workdir_filename)
        if file_row["InS3"]:
            continue  # File already exists in S3

        # Determine S3 key path based on file type
        file_ext = os.path.splitext(workdir_filename)[1]
        if file_ext in backup_context.video_file_extensions:
            bucket_key = backup_context.dir_prefix + "video/" + workdir_filename
        else:
            bucket_key = backup_context.dir_prefix + workdir_filename

        workdir_file_path = backup_context.get_file_destination_path(
            workdir_filename, backup_context.local_working_dir
        )

        if dryrun:
            click.echo(f"Dry run; would have uploaded '{workdir_filename}' to s3 key '{bucket_key}'")
        else:
            click.echo(f"Uploading '{workdir_file_path}' to s3 key '{bucket_key}'")
            backup_context.bucket.upload_file(str(workdir_file_path), bucket_key)


@cli.command()
@pass_backup_context
@click.option(
    "--dryrun",
    prompt="Dry run?",
    type=click.BOOL,
    default=True,
    help="Do not actually download files.",
)
def download(backup_context, dryrun):
    """Downloads files from s3 to local working dir."""
    if not dryrun:
        backup_context.mkdir()

    for key in backup_context.bucket_file_paths:
        dest_path = backup_context.local_bucket_dir / key

        if dryrun:
            click.echo(f"Dry run; would have downloaded s3 key '{key}' to '{dest_path}'")
        else:
            click.echo(f"Downloading s3 key '{key}' to '{dest_path}'")
            # Ensure parent directory exists
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            backup_context.bucket.download_file(key, str(dest_path))


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
    """Populate and print DB rows for given year/month/device.
    """
    pd.set_option("display.max_rows", 1000)
    print(pd.read_sql_query("SELECT * FROM files", backup_context.db))


@cli.command()
@pass_backup_context
def lsdropbox(backup_context):
    """Print Dropbox contents for given year/month/device.
    """
    for name in backup_context.dropbox_filenames:
        print(name)


@cli.command()
@pass_backup_context
def lsworkdir(backup_context):
    """Print working dir contents for given year/month/device.
    """
    for filename in backup_context.working_dir_filenames:
        print(filename)


@cli.command()
@pass_backup_context
def sync_workdir(backup_context):
    """s3 sync working dir contents for given year/month/device.
    """
    # TODO - use snippet from
    # https://github.com/boto/boto3/issues/358#issuecomment-372086466
    for filename in backup_context.working_dir_filenames:
        print(filename)


@cli.command()
@click.pass_context
@click.option(
    "--dryrun",
    prompt="Dry run?",
    type=click.BOOL,
    default=True,
    help="Print steps rather than execute them.",
)
def workflow(backup_context, dryrun):
    """Runs all commands for a typical backup workflow."""
    backup_context.invoke(mkdir)
    backup_context.invoke(difflocal)

    click.confirm("About to copy files to workdir - do you want to continue?", abort=True)
    backup_context.forward(cp)

    if dryrun:
        print("Skipping s3 preview step since dryrun mode is on...")
    else:
        click.confirm("About to upload files to s3 - do you want to continue?", abort=True)
        # Reinitialize DB with updated paths after copying files
        backup_context.obj.init_db()
        backup_context.forward(upload)
        click.echo("All done - to delete your files from Dropbox, run the rm-dropbox-files command.")
