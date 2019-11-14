import pytest
from click.testing import CliRunner
from unittest.mock import patch, call, MagicMock

from dtran.main import cli


# https://click.palletsprojects.com/en/7.x/testing/
@pytest.mark.parametrize(
    "func_name, attr_name, arg_value",
    [
        ("", "", ""),
        ("substitute", "attr", "some value"),
    ]
)
@patch('dtran.main.ConfigParser', autospec=True)
def test_cli_valid(parser_mock, func_name, attr_name, arg_value):
    """
    This function tests 3 valid scenarios:
    1) User does not specify any inputs
    2) User updates existing inputs
    3) User inserts new valid inputs
    """
    pipeline_mock = MagicMock()
    mock_parsed_inputs = {
        "keep_attr": "keep this value",
        "substitute_attr": "substitute this value"
    }

    parser_mock.return_value.parse.return_value = (
        pipeline_mock,
        mock_parsed_inputs
    )

    mock_user_inputs = {}

    runner = CliRunner()
    if not func_name or not attr_name:
        result = runner.invoke(cli, [
            'create_pipeline', '--config', 'config/path'
        ])
    else:
        mock_user_inputs = {
            (func_name, attr_name): arg_value
        }
        result = runner.invoke(cli, [
            'create_pipeline', '--config', 'config/path',
            f'--{func_name}.{attr_name}={arg_value}',
        ])

    assert result.exit_code == 0
    assert parser_mock.mock_calls == [call(mock_user_inputs), call().parse('config/path')]
    assert pipeline_mock.exec.mock_calls[-1] == call(mock_parsed_inputs)


@pytest.mark.parametrize(
    "func_name, attr_name, arg_value",
    [
        ("substitute", "attr", "some value"),
    ]
)
def test_cli_invalid(func_name, attr_name, arg_value):
    """
    This function tests invalid cli input scenario.
    """
    runner = CliRunner()
    arg = f'--{func_name}//{attr_name}={arg_value}'
    result = runner.invoke(cli, [
        'create_pipeline', '--config', 'config/path', arg
    ])

    assert result.exit_code == 0
    assert f"user input: '{arg}' should have format '--FuncName.Attr=value'" in result.output


@pytest.mark.parametrize(
    "func_name, attr_name, arg_value",
    [
        ("substitute", "attr", "some value"),
    ]
)
@patch('dtran.main.ConfigParser', autospec=True)
def test_cli_dryrun(parser_mock, func_name, attr_name, arg_value):
    pipeline_mock = MagicMock()
    mock_parsed_inputs = {
        "keep_attr": "keep this value",
        "substitute_attr": "substitute this value"
    }

    parser_mock.return_value.parse.return_value = (
        pipeline_mock,
        mock_parsed_inputs
    )

    runner = CliRunner()
    result = runner.invoke(cli, [
        'create_pipeline', '--config', 'config/path',
        f'--{func_name}.{attr_name}={arg_value}', '--dryrun'
    ])

    for parsed_key, parsed_value in mock_parsed_inputs.items():
        assert parsed_key in result.output
        assert parsed_value in result.output
