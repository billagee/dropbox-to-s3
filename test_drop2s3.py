# test_drop2s3.py

import pytest
from pathlib import Path
from click.testing import CliRunner
from drop2s3 import cli, detect_year_month_combinations
from moto import mock_aws
import boto3

def test_help_command():
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert 'Usage: cli [OPTIONS] COMMAND [ARGS]' in result.output
    assert 'Options' in result.output
    assert 'Commands' in result.output

@mock_aws
def test_lsdb_command_when_staging_dir_is_empty():
    # Set up mock S3
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='my-test-bucket')

    # Add any necessary mock objects to the bucket
    #s3.put_object(Bucket='my-test-bucket', Key='test.txt', Body='This is a test file.')

    runner = CliRunner()
    result = runner.invoke(
        cli, [
            '--bucket-name', 'my-test-bucket',
            '--year', '2024',
            '--month', '02',
            '--device', 'default',
            'lsdb'
        ]
    )
    assert result.exit_code == 0
    assert 'Empty DataFrame' in result.output


def test_detect_year_month_combinations(tmp_path):
    """Test auto-detection of year/month combinations from filenames."""
    # Create a temporary Dropbox directory with test files
    dropbox_dir = tmp_path / "Dropbox" / "Camera Uploads"
    dropbox_dir.mkdir(parents=True)

    # Create test files with different year/month combinations
    test_files = [
        "2024-01-15-IMG_001.jpg",
        "2024-01-20-IMG_002.jpg",
        "2024-02-10-IMG_003.jpg",
        "2024-10-05-IMG_004.jpg",
        "2023-12-25-IMG_005.jpg",
        "invalid-filename.jpg",  # Should be ignored
    ]

    for filename in test_files:
        (dropbox_dir / filename).touch()

    # Test the detection function
    combinations = detect_year_month_combinations(dropbox_dir)

    # Should detect 4 unique year/month combinations, sorted
    assert len(combinations) == 4
    assert combinations[0] == {'year': '2023', 'month': '12'}
    assert combinations[1] == {'year': '2024', 'month': '01'}
    assert combinations[2] == {'year': '2024', 'month': '02'}
    assert combinations[3] == {'year': '2024', 'month': '10'}


def test_detect_year_month_combinations_empty_directory(tmp_path):
    """Test detection with no matching files."""
    dropbox_dir = tmp_path / "Dropbox" / "Camera Uploads"
    dropbox_dir.mkdir(parents=True)

    # No files created
    combinations = detect_year_month_combinations(dropbox_dir)

    assert len(combinations) == 0


def test_detect_year_month_combinations_nonexistent_directory(tmp_path):
    """Test detection with non-existent directory."""
    dropbox_dir = tmp_path / "nonexistent"

    combinations = detect_year_month_combinations(dropbox_dir)

    assert len(combinations) == 0