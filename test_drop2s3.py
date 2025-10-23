# test_drop2s3.py

import pytest
from click.testing import CliRunner
from drop2s3 import cli # Assumes the main CLI function is named 'cli'
from moto import mock_aws
import boto3

def test_help_command():
    """Test that --help flag displays expected help information."""
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])

    # Verify command succeeds
    assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}"

    # Verify help structure
    assert 'Usage:' in result.output, "Help should contain usage information"
    assert 'Options:' in result.output, "Help should contain options section"
    assert 'Commands:' in result.output, "Help should contain commands section"

    # Verify description is present
    assert 'Dropbox/Camera Uploads' in result.output, "Help should contain utility description"

    # Verify key commands are listed
    expected_commands = ['mkdir', 'cp', 'upload', 'download', 'lsdb', 'workflow']
    for command in expected_commands:
        assert command in result.output, f"Command '{command}' should be listed in help output"

    # Verify required options are documented
    assert '--bucket-name' in result.output, "Help should document --bucket-name option"
    assert '--year' in result.output, "Help should document --year option"
    assert '--month' in result.output, "Help should document --month option"
    assert '--device' in result.output, "Help should document --device option"

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