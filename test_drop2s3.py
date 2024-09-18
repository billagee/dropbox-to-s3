# test_drop2s3.py

import pytest
from click.testing import CliRunner
from drop2s3 import cli # Assumes the main CLI function is named 'cli'
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