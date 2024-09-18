# test_drop2s3.py

import pytest
from click.testing import CliRunner
from drop2s3 import cli # Assumes the main CLI function is named 'cli'

def test_help_command():
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert 'Usage: cli [OPTIONS] COMMAND [ARGS]' in result.output
    assert 'Options' in result.output
    assert 'Commands' in result.output