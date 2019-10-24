import pytest
from click.testing import CliRunner
from unittest.mock import patch, call

from dtran.main import cli


def _mock_pipeline_parser(pipeline_mock, parse_mock):
    pipeline_mock.exec.side_effect = lambda _: True
    mock_parsed_inputs = {
        "keep_attr": "keep this value",
        "substitute_attr": "substitute this value"
    }

    mock_mappings = {
        "my_custom_func1": "keep_",
        "my_custom_func2": "substitute_"
    }

    parse_mock.return_value = (
        pipeline_mock,
        mock_parsed_inputs,
        mock_mappings
    )

    return pipeline_mock, parse_mock


# https://click.palletsprojects.com/en/7.x/testing/
@pytest.mark.parametrize(
    "func_name, attr_name, arg_value, upserted_inputs",
    [
        ("", "", "", {
            "keep_attr": "keep this value",
            "substitute_attr": "substitute this value",
        }), ("my_custom_func2", "attr", "some value", {
            "keep_attr": "keep this value",
            "substitute_attr": "some value"
        }), ("my_custom_func2", "other_attr", "some value", {
            "keep_attr": "keep this value",
            "substitute_attr": "substitute this value",
            "substitute_other_attr": "some value"
        }), ("my_custom_func4", "attr", "some value", {
            "keep_attr": "keep this value",
            "substitute_attr": "substitute this value",
        }),
    ]
)
@patch('dtran.config_parser.ConfigParser.parse')
@patch('dtran.pipeline.Pipeline')
def test_cli(pipeline_mock, parse_mock, func_name, attr_name, arg_value, upserted_inputs):
    """
    This function tests cli given 4 scenarios:
    1) User does not specify any inputs
    2) User updates existing inputs
    3) User inserts new valid inputs
    4) User trys to insert invalid inputs
    """
    # TODO: how to combine mock context and pytest fixture?
    pipeline_mock, parse_mock = _mock_pipeline_parser(pipeline_mock, parse_mock)
    runner = CliRunner()
    if not func_name or not attr_name:
        result = runner.invoke(cli, [
            'create_pipeline', '--config', 'config/path/config.yml'
        ])
    else:
        result = runner.invoke(cli, [
            'create_pipeline', '--config', 'config/path/config.yml',
            f'--{func_name}.{attr_name}={arg_value}',
        ])

    assert result.exit_code == 0
    assert pipeline_mock.exec.mock_calls[-1] == call(upserted_inputs)
