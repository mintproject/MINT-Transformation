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
    This function tests cli given 4 scenarios:
    1) User does not specify any inputs
    2) User updates existing inputs
    3) User inserts new valid inputs
    4) User trys to insert invalid inputs
    """
    # TODO: how to combine mock context and pytest fixture?
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
        ("", "", ""),
        ("substitute", "attr", "some value"),
    ]
)
@patch('dtran.main.ConfigParser', autospec=True)
def test_cli_invalid(parser_mock, func_name, attr_name, arg_value):
    pass
