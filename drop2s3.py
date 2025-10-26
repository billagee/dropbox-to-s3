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
import sys
import filecmp
import sqlite3
from datetime import datetime
from pathlib import Path
from shutil import copy2
from typing import List, Optional

import boto3
import typer
import pandas as pd

# Configuration constants
SUPPORTED_FILE_EXTENSIONS = ["jpg", "png", "mov", "3gp", "heic", "mp4"]
VIDEO_FILE_EXTENSIONS = [".mov", ".3gp", ".mp4"]
DROPBOX_CAMERA_DIR = "Dropbox/Camera Uploads/"
LOCAL_PICTURES_DIR = "Pictures/s3/"
S3_PREFIX_TEMPLATE = "photos/{year}/{month}/{device}/"

# Global context - initialized by callback
_backup_context: Optional["BackupContext"] = None


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
            typer.echo(f"About to create working dir at {self.local_working_dir}")
            typer.confirm("Do you want to continue?", abort=True)
            # Create working dir and video subdirectory
            (self.local_working_dir / "video").mkdir(parents=True, exist_ok=True)
        else:
            typer.echo(f"Working dir already exists at {self.local_working_dir}")

    def __repr__(self):
        return "<BackupContext %r>" % self.local_working_dir


def get_context() -> BackupContext:
    """Get the current backup context. Raises error if not initialized."""
    if _backup_context is None:
        typer.echo("Error: Context not initialized. This shouldn't happen.", err=True)
        raise typer.Exit(1)
    return _backup_context


# Create the Typer app
app = typer.Typer(
    help="""
    This utility copies image/video files from ~/Dropbox/Camera Uploads/
    into a local working dir, then uploads the files to an s3 bucket.

    Example usage:
      drop2s3 workflow
      drop2s3 diffbucket
      drop2s3 rm-dropbox-files
    """
)


@app.callback()
def main(
    bucket_name: str = typer.Option(
        ..., prompt="Bucket name", help="The s3 bucket name to upload files to."
    ),
    year: str = typer.Option(
        "{:02}".format(datetime.today().year),
        prompt="Photo year",
        help="The year dir to use in the working dir path (e.g. 2017).",
    ),
    month: str = typer.Option(
        "{:02d}".format(datetime.today().month),
        prompt="Photo month",
        help="The month dir to use in the working dir path (e.g. 09).",
    ),
    device: str = typer.Option(
        "default",
        prompt="Device name",
        help="The device name to use in the working dir path (e.g. 'iPhone15').",
    ),
):
    """
    Initialize the backup context with bucket, year, month, and device information.
    """
    global _backup_context
    _backup_context = BackupContext(bucket_name, year, month, device)


@app.command()
def mkdir():
    """
    Creates local working dir to copy Dropbox files to.
    will be copied from your Dropbox/Camera Uploads/ dir.
    The working dir files can then be uploaded to an s3 bucket.
    """
    ctx = get_context()
    ctx.mkdir()


@app.command()
def cp(
    dryrun: bool = typer.Option(
        True, prompt="Dry run?", help="Do not actually copy files."
    )
):
    """
    Copy files from Dropbox to working dir.

    Note that files with video extensions will be copied into
    a "video" subdir of the working dir.
    """
    ctx = get_context()
    # Check for working dir and run mkdir() if it doesn't exist
    ctx.mkdir()
    typer.echo(f"About to copy files from: {ctx.dropbox_camera_uploads_dir}")
    typer.echo(f"To local working dir: {ctx.local_working_dir}")

    for dropbox_file_name in ctx.dropbox_filenames:
        file_row = ctx.get_file_db_row(dropbox_file_name)
        if file_row["InWorkingDir"]:
            typer.echo(f"Skipping file '{dropbox_file_name}'; it already exists in workdir")
            continue

        # Get destination path using helper method
        dest_path = ctx.get_file_destination_path(dropbox_file_name, ctx.local_working_dir)

        if dryrun:
            typer.echo(f"Dry run; would have copied '{dropbox_file_name}' to workdir")
        else:
            typer.echo(f"Copying '{dropbox_file_name}' to {dest_path.parent}")
            copy2(ctx.dropbox_camera_uploads_dir / dropbox_file_name, dest_path)


@app.command()
def rm_dropbox_files(
    dryrun: bool = typer.Option(
        True, prompt="Dry run?", help="Do not actually delete files."
    )
):
    """Delete backed-up files in your Camera Uploads dir."""
    ctx = get_context()
    # Verify files exist in both working dir and S3 before deletion
    typer.echo("Checking for Dropbox files in workdir and s3...")
    for dropbox_file_name in ctx.dropbox_filenames:
        file_row = ctx.get_file_db_row(dropbox_file_name)
        in_workdir = file_row["InWorkingDir"]
        in_s3 = file_row["InS3"]

        dropbox_file_path = ctx.dropbox_camera_uploads_dir / dropbox_file_name
        workdir_file_path = ctx.get_file_destination_path(
            dropbox_file_name, ctx.local_working_dir
        )

        if in_workdir and in_s3:
            # Verify file integrity before deletion
            if filecmp.cmp(dropbox_file_path, workdir_file_path, shallow=False):
                if dryrun:
                    typer.echo(f"[dry run] would have deleted Dropbox file '{dropbox_file_name}'")
                else:
                    typer.echo(f"Deleting {dropbox_file_path}...")
                    os.remove(dropbox_file_path)
            else:
                typer.echo(
                    f"Error: Files differ! Dropbox: '{dropbox_file_name}', "
                    f"Workdir: '{workdir_file_path}'"
                )
                typer.echo("Aborting clean; please investigate before continuing.")
                raise typer.Exit(1)
        else:
            typer.echo(
                f"Skipping rm of Dropbox file '{dropbox_file_name}'; "
                "it's not present in both workdir and s3..."
            )
            typer.echo("Please run the upload command to back the file up in s3 first.")


@app.command()
def difflocal():
    """Diff Dropbox and working dir contents."""
    ctx = get_context()
    fmt = "{:<40}{:<20}"
    print(fmt.format("File name", "Status"))
    query = "SELECT * FROM files ORDER BY Filename"
    for row in ctx.db_manager.execute_query(query):
        filename = row["Filename"]
        in_dropbox = row["InDropbox"]
        in_workdir = row["InWorkingDir"]
        in_s3 = row["InS3"]

        dropbox_file_path = ctx.dropbox_camera_uploads_dir / filename
        workdir_file_path = ctx.get_file_destination_path(filename, ctx.local_working_dir)

        if in_dropbox == 1 and in_workdir == 1:
            if filecmp.cmp(dropbox_file_path, workdir_file_path, shallow=False):
                print(fmt.format(filename, "diff OK"))
            else:
                print(fmt.format(filename, "diff NOT OK - files differ!"))
        elif in_dropbox == 1 and in_workdir == 0:
            typer.secho(fmt.format(filename, "dropbox only"), bg=typer.colors.RED, fg=typer.colors.WHITE)
        elif in_dropbox == 0 and in_workdir == 0 and in_s3 == 1:
            typer.secho(fmt.format(filename, "s3 only"), bg=typer.colors.BLUE, fg=typer.colors.WHITE)


@app.command()
def diffbucket():
    """Diff working dir and s3 bucket contents."""
    ctx = get_context()
    fmt = "{:<40}{:<20}"
    print(fmt.format("File name", "Status"))
    query = "SELECT * FROM files ORDER BY Filename"
    for row in ctx.db_manager.execute_query(query):
        filename = row["Filename"]
        in_dropbox = row["InDropbox"]
        in_workdir = row["InWorkingDir"]
        in_s3 = row["InS3"]

        if in_workdir == 1 and in_s3 == 1:
            print(fmt.format(filename, "found in s3 & workdir"))
        elif in_workdir == 1 and in_s3 == 0:
            typer.secho(fmt.format(filename, "workdir only"), bg=typer.colors.RED, fg=typer.colors.WHITE)
        elif in_workdir == 0 and in_s3 == 1:
            typer.secho(fmt.format(filename, "s3 only"))
        elif in_workdir == 0 and in_s3 == 0 and in_dropbox == 1:
            typer.secho(fmt.format(filename, "dropbox only"), bg=typer.colors.RED, fg=typer.colors.WHITE)


@app.command()
def upload(
    dryrun: bool = typer.Option(
        True, prompt="Dry run?", help="Do not actually upload files."
    )
):
    """Uploads local working dir files to an s3 bucket."""
    ctx = get_context()
    for workdir_filename in ctx.working_dir_filenames:
        file_row = ctx.get_file_db_row(workdir_filename)
        if file_row["InS3"]:
            continue  # File already exists in S3

        # Determine S3 key path based on file type
        file_ext = os.path.splitext(workdir_filename)[1]
        if file_ext in ctx.video_file_extensions:
            bucket_key = ctx.dir_prefix + "video/" + workdir_filename
        else:
            bucket_key = ctx.dir_prefix + workdir_filename

        workdir_file_path = ctx.get_file_destination_path(workdir_filename, ctx.local_working_dir)

        if dryrun:
            typer.echo(f"Dry run; would have uploaded '{workdir_filename}' to s3 key '{bucket_key}'")
        else:
            typer.echo(f"Uploading '{workdir_file_path}' to s3 key '{bucket_key}'")
            ctx.bucket.upload_file(str(workdir_file_path), bucket_key)


@app.command()
def download(
    dryrun: bool = typer.Option(
        True, prompt="Dry run?", help="Do not actually download files."
    )
):
    """Downloads files from s3 to local working dir."""
    ctx = get_context()
    if not dryrun:
        ctx.mkdir()

    for key in ctx.bucket_file_paths:
        dest_path = ctx.local_bucket_dir / key

        if dryrun:
            typer.echo(f"Dry run; would have downloaded s3 key '{key}' to '{dest_path}'")
        else:
            typer.echo(f"Downloading s3 key '{key}' to '{dest_path}'")
            # Ensure parent directory exists
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            ctx.bucket.download_file(key, str(dest_path))


@app.command()
def lsbucket():
    """Print s3 bucket contents for given year/month/device."""
    ctx = get_context()
    for key in ctx.bucket_file_paths:
        print(key)


@app.command()
def lsdb():
    """Populate and print DB rows for given year/month/device."""
    ctx = get_context()
    pd.set_option("display.max_rows", 1000)
    print(pd.read_sql_query("SELECT * FROM files", ctx.db))


@app.command()
def lsdropbox():
    """Print Dropbox contents for given year/month/device."""
    ctx = get_context()
    for name in ctx.dropbox_filenames:
        print(name)


@app.command()
def lsworkdir():
    """Print working dir contents for given year/month/device."""
    ctx = get_context()
    for filename in ctx.working_dir_filenames:
        print(filename)


@app.command()
def sync_workdir():
    """s3 sync working dir contents for given year/month/device."""
    ctx = get_context()
    # TODO - use snippet from
    # https://github.com/boto/boto3/issues/358#issuecomment-372086466
    for filename in ctx.working_dir_filenames:
        print(filename)


@app.command()
def workflow(
    dryrun: bool = typer.Option(
        True, prompt="Dry run?", help="Print steps rather than execute them."
    )
):
    """Runs all commands for a typical backup workflow."""
    ctx = get_context()

    # Run mkdir
    ctx.mkdir()

    # Run difflocal
    typer.echo("\n=== Running difflocal ===")
    fmt = "{:<40}{:<20}"
    print(fmt.format("File name", "Status"))
    query = "SELECT * FROM files ORDER BY Filename"
    for row in ctx.db_manager.execute_query(query):
        filename = row["Filename"]
        in_dropbox = row["InDropbox"]
        in_workdir = row["InWorkingDir"]
        in_s3 = row["InS3"]

        dropbox_file_path = ctx.dropbox_camera_uploads_dir / filename
        workdir_file_path = ctx.get_file_destination_path(filename, ctx.local_working_dir)

        if in_dropbox == 1 and in_workdir == 1:
            if filecmp.cmp(dropbox_file_path, workdir_file_path, shallow=False):
                print(fmt.format(filename, "diff OK"))
            else:
                print(fmt.format(filename, "diff NOT OK - files differ!"))
        elif in_dropbox == 1 and in_workdir == 0:
            typer.secho(fmt.format(filename, "dropbox only"), bg=typer.colors.RED, fg=typer.colors.WHITE)
        elif in_dropbox == 0 and in_workdir == 0 and in_s3 == 1:
            typer.secho(fmt.format(filename, "s3 only"), bg=typer.colors.BLUE, fg=typer.colors.WHITE)

    typer.confirm("About to copy files to workdir - do you want to continue?", abort=True)

    # Copy files
    typer.echo(f"\n=== Copying files (dryrun={dryrun}) ===")
    typer.echo(f"About to copy files from: {ctx.dropbox_camera_uploads_dir}")
    typer.echo(f"To local working dir: {ctx.local_working_dir}")

    for dropbox_file_name in ctx.dropbox_filenames:
        file_row = ctx.get_file_db_row(dropbox_file_name)
        if file_row["InWorkingDir"]:
            typer.echo(f"Skipping file '{dropbox_file_name}'; it already exists in workdir")
            continue

        dest_path = ctx.get_file_destination_path(dropbox_file_name, ctx.local_working_dir)

        if dryrun:
            typer.echo(f"Dry run; would have copied '{dropbox_file_name}' to workdir")
        else:
            typer.echo(f"Copying '{dropbox_file_name}' to {dest_path.parent}")
            copy2(ctx.dropbox_camera_uploads_dir / dropbox_file_name, dest_path)

    if dryrun:
        print("\nSkipping s3 preview step since dryrun mode is on...")
    else:
        typer.confirm("About to upload files to s3 - do you want to continue?", abort=True)
        # Reinitialize DB with updated paths after copying files
        ctx.init_db()

        # Upload files
        typer.echo("\n=== Uploading files to S3 ===")
        for workdir_filename in ctx.working_dir_filenames:
            file_row = ctx.get_file_db_row(workdir_filename)
            if file_row["InS3"]:
                continue

            file_ext = os.path.splitext(workdir_filename)[1]
            if file_ext in ctx.video_file_extensions:
                bucket_key = ctx.dir_prefix + "video/" + workdir_filename
            else:
                bucket_key = ctx.dir_prefix + workdir_filename

            workdir_file_path = ctx.get_file_destination_path(workdir_filename, ctx.local_working_dir)
            typer.echo(f"Uploading '{workdir_file_path}' to s3 key '{bucket_key}'")
            ctx.bucket.upload_file(str(workdir_file_path), bucket_key)

        typer.echo("\nAll done - to delete your files from Dropbox, run the rm-dropbox-files command.")


def cli():
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    cli()
